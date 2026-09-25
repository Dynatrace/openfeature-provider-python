import socket
import threading
import time
from random import Random

import pytest
import requests
import responses as rsps_lib

from dynatrace.openfeature import _cdn as cdn
from dynatrace.openfeature._cdn import (
    MAX_FETCH_DURATION_S,
    CdnFetcher,
    _parse_retry_after_ms,
)
from dynatrace.openfeature._retry import RetryStrategy
from dynatrace.openfeature.options import DynatraceProviderOptions

_KEY = "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
_CDN_URL = "https://cdn.us.fm.dynatrace.com/server/" + _KEY + ".json"
_BODY = '{"flags":{}}'
_ETAG = '"abc123"'
_LM_OLD = "Thu, 01 May 2025 00:00:00 GMT"
_LM_NEW = "Fri, 02 May 2025 00:00:00 GMT"
_SHORTEST_POLL_INTERVAL_S = 10.0  # providers.md section 2.1 (SSE disconnected)


def _fast_retry() -> RetryStrategy:
    return RetryStrategy(rng=Random(0))  # noqa: S311


def _fetcher(**kw: object) -> CdnFetcher:
    opts = DynatraceProviderOptions.from_sdk_key(_KEY)
    return CdnFetcher(opts, _fast_retry(), **kw)  # type: ignore[arg-type]


class TestParseRetryAfterMs:
    def test_seconds(self) -> None:
        assert _parse_retry_after_ms("30", 0.0) == 30_000.0

    def test_http_date(self) -> None:
        future = "Thu, 13 Jan 2000 03:09:00 GMT"  # epoch=947732940
        result = _parse_retry_after_ms(future, 947_732_840.0)  # 100s before
        assert 99_000 <= result <= 101_000

    def test_none_returns_default(self) -> None:
        assert _parse_retry_after_ms(None, 0.0) == 60_000.0

    def test_blank_returns_default(self) -> None:
        assert _parse_retry_after_ms("  ", 0.0) == 60_000.0

    def test_invalid_returns_default(self) -> None:
        assert _parse_retry_after_ms("not-a-date", 0.0) == 60_000.0


class TestCdnFetcherResponses:
    @rsps_lib.activate
    def test_200_stores_etag_and_last_modified(self) -> None:
        rsps_lib.add(
            rsps_lib.GET,
            _CDN_URL,
            body=_BODY,
            status=200,
            headers={"ETag": _ETAG, "Last-Modified": _LM_NEW},
        )
        f = _fetcher()
        result = f.fetch()
        assert result.is_success
        assert result.body == _BODY
        assert result.etag == _ETAG
        assert result.last_modified == _LM_NEW

    @rsps_lib.activate
    def test_etag_sent_on_second_request(self) -> None:
        rsps_lib.add(
            rsps_lib.GET, _CDN_URL, body=_BODY, status=200, headers={"ETag": _ETAG}
        )
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        f = _fetcher()
        f.fetch()
        result = f.fetch()
        assert result.is_not_modified
        assert rsps_lib.calls[1].request.headers.get("If-None-Match") == _ETAG

    @rsps_lib.activate
    def test_304_returns_cached_headers(self) -> None:
        rsps_lib.add(
            rsps_lib.GET,
            _CDN_URL,
            body=_BODY,
            status=200,
            headers={"ETag": _ETAG, "Last-Modified": _LM_NEW},
        )
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        f = _fetcher()
        f.fetch()
        result = f.fetch()
        assert result.is_not_modified
        assert result.etag == _ETAG

    @rsps_lib.activate
    def test_429_with_seconds_sets_rate_limit(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=429, headers={"Retry-After": "30"})
        fixed_now = 1_000.0
        f = _fetcher(clock=lambda: fixed_now)
        result = f.fetch()
        assert result.is_rate_limited
        assert f.is_rate_limited(now=fixed_now + 29)
        assert not f.is_rate_limited(now=fixed_now + 31)

    @rsps_lib.activate
    def test_429_no_header_uses_60s_default(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=429)
        fixed_now = 1_000.0
        f = _fetcher(clock=lambda: fixed_now)
        f.fetch()
        assert f.is_rate_limited(now=fixed_now + 59)
        assert not f.is_rate_limited(now=fixed_now + 61)

    @rsps_lib.activate
    def test_redirects_are_not_followed(self) -> None:
        # Each hop would get its own full timeout, so a chain of them would run
        # past MAX_FETCH_DURATION_S and break the single-flight bound.
        rsps_lib.add(
            rsps_lib.GET,
            _CDN_URL,
            status=302,
            headers={"Location": "https://elsewhere.example/config.json"},
        )
        result = _fetcher().fetch()
        assert result.status == 302
        assert not result.is_success
        assert len(rsps_lib.calls) == 1

    def test_is_rate_limited_skips_network(self) -> None:
        fixed_now = 1_000.0
        f = _fetcher(clock=lambda: fixed_now)
        f._rate_limited_until = fixed_now + 60.0
        result = f.fetch()
        # No request leaves the process inside the window, so the caller learned
        # nothing: a skip, not a 429 the CDN actually sent.
        assert result.skipped
        assert not result.is_rate_limited

    @rsps_lib.activate
    def test_stale_node_defense_same_last_modified(self) -> None:
        rsps_lib.add(
            rsps_lib.GET,
            _CDN_URL,
            body=_BODY,
            status=200,
            headers={"Last-Modified": _LM_OLD},
        )
        rsps_lib.add(
            rsps_lib.GET,
            _CDN_URL,
            body="new",
            status=200,
            headers={"Last-Modified": _LM_OLD},
        )
        f = _fetcher()
        f.fetch()
        result = f.fetch()
        assert result.is_not_modified

    @rsps_lib.activate
    def test_stale_node_defense_older_last_modified(self) -> None:
        rsps_lib.add(
            rsps_lib.GET,
            _CDN_URL,
            body=_BODY,
            status=200,
            headers={"Last-Modified": _LM_NEW},
        )
        rsps_lib.add(
            rsps_lib.GET,
            _CDN_URL,
            body="new",
            status=200,
            headers={"Last-Modified": _LM_OLD},
        )
        f = _fetcher()
        f.fetch()
        result = f.fetch()
        assert result.is_not_modified

    @rsps_lib.activate
    def test_200_without_last_modified_accepted(self) -> None:
        rsps_lib.add(
            rsps_lib.GET,
            _CDN_URL,
            body=_BODY,
            status=200,
            headers={"Last-Modified": _LM_OLD},
        )
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body="new", status=200)
        f = _fetcher()
        f.fetch()
        result = f.fetch()
        assert result.is_success
        assert result.body == "new"

    @rsps_lib.activate
    def test_retries_on_500(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_BODY, status=200)
        f = _fetcher()
        result = f.fetch()
        assert result.is_success
        assert len(rsps_lib.calls) == 2

    @rsps_lib.activate
    def test_gives_up_after_two_attempts(self) -> None:
        # providers.md section 2.4: a fetch is one attempt plus one retry, so it
        # cannot outlive the shortest poll interval.
        for _ in range(3):
            rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        f = _fetcher()
        result = f.fetch()
        assert result.status == 500
        assert len(rsps_lib.calls) == 2

    def test_max_fetch_duration_stays_under_shortest_poll_interval(self) -> None:
        # The invariant the two-attempt cap exists to preserve.
        assert MAX_FETCH_DURATION_S < _SHORTEST_POLL_INTERVAL_S


class TestFetchDeadline:
    """The body read must respect the fetch budget.

    No timeout requests or urllib3 offers bounds it: they all measure
    inactivity between socket reads, so a server that trickles bytes can stretch
    a fetch indefinitely and MAX_FETCH_DURATION_S stops being a real bound.
    """

    @staticmethod
    def _drip_server(chunk_delay_s: float, chunks: int) -> tuple[str, socket.socket]:
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(8)

        def serve() -> None:
            while True:
                try:
                    conn, _ = srv.accept()
                except OSError:
                    return
                conn.recv(4096)
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n" % chunks)
                for _ in range(chunks):
                    time.sleep(chunk_delay_s)
                    try:
                        conn.sendall(b"x")
                    except OSError:
                        break
                conn.close()

        threading.Thread(target=serve, daemon=True).start()
        return f"http://127.0.0.1:{srv.getsockname()[1]}/", srv

    def test_slow_body_is_cut_off_at_the_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        budget = 0.4
        monkeypatch.setattr(cdn, "REQUEST_TIMEOUT_S", budget)
        # Each chunk lands inside the read timeout but the body as a whole runs
        # far past the budget, which is exactly the case the socket timeout
        # cannot catch.
        url, srv = self._drip_server(chunk_delay_s=budget / 2, chunks=20)
        f = _fetcher()
        f._url = url
        try:
            started = time.monotonic()
            with pytest.raises(requests.Timeout):
                f._do_fetch()
            elapsed = time.monotonic() - started
        finally:
            srv.close()
        assert elapsed < budget * 3, (
            f"body read ran {elapsed:.2f}s past a {budget}s budget"
        )

    def test_prompt_body_is_returned(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(cdn, "REQUEST_TIMEOUT_S", 4.0)
        url, srv = self._drip_server(chunk_delay_s=0.0, chunks=5)
        f = _fetcher()
        f._url = url
        try:
            result = f._do_fetch()
        finally:
            srv.close()
        assert result.status == 200
        assert result.body == "xxxxx"
