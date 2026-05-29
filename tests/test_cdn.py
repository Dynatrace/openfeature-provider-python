from random import Random

import responses as rsps_lib

from dynatrace.openfeature._cdn import CdnFetcher, _parse_retry_after_ms
from dynatrace.openfeature._retry import RetryStrategy
from dynatrace.openfeature.options import DynatraceProviderOptions

_KEY = "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
_CDN_URL = "https://cdn.us.fm.dynatrace.com/server/" + _KEY + ".json"
_BODY = '{"flags":{}}'
_ETAG = '"abc123"'
_LM_OLD = "Thu, 01 May 2025 00:00:00 GMT"
_LM_NEW = "Fri, 02 May 2025 00:00:00 GMT"


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

    def test_is_rate_limited_skips_network(self) -> None:
        fixed_now = 1_000.0
        f = _fetcher(clock=lambda: fixed_now)
        f._rate_limited_until = fixed_now + 60.0
        result = f.fetch()
        assert result.is_rate_limited

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
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_BODY, status=200)
        f = _fetcher()
        result = f.fetch()
        assert result.is_success
        assert len(rsps_lib.calls) == 3
