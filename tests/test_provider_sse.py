import json
import threading
from pathlib import Path

import pytest
import responses as rsps_lib
from openfeature.exception import GeneralError
from openfeature.provider import ProviderStatus
from sse_server import SseServer, wait_until

from dynatrace.openfeature import DynatraceProvider
from dynatrace.openfeature._cdn import CdnFetcher, FetchResult
from dynatrace.openfeature._sse_message import SseMessage
from dynatrace.openfeature.options import DynatraceProviderOptions

_KEY = "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
_CDN_URL = "https://cdn.us.fm.dynatrace.com/server/" + _KEY + ".json"
_FIXTURE = Path(__file__).parent / "fixtures" / "flag-config.json"

_CONNECTED_S = 600.0
_DISCONNECTED_S = 300.0


def _config(sse_url: str | None = None) -> str:
    config = json.loads(_FIXTURE.read_text())
    if sse_url is not None:
        config["eventStreams"] = [{"type": "sse", "url": sse_url}]
    return json.dumps(config)


def _make_provider(
    *, poll_s: float = _DISCONNECTED_S, grace_s: float = 60.0, disable_sse: bool = False
) -> DynatraceProvider:
    return DynatraceProvider(
        _KEY,
        fetcher=CdnFetcher(DynatraceProviderOptions.from_sdk_key(_KEY)),
        poll_interval_s=poll_s,
        sse_connected_poll_interval_s=_CONNECTED_S,
        disable_sse=disable_sse,
        grace_period_s=grace_s,
    )


def _record_events(p: DynatraceProvider) -> list[str]:
    events: list[str] = []
    for name in ("ready", "stale", "error"):
        original = getattr(p, f"emit_provider_{name}")

        def emit(details: object, _name: str = name, _original=original) -> None:
            _original(details)
            events.append(_name)

        setattr(p, f"emit_provider_{name}", emit)
    return events


def _cdn_calls() -> int:
    return len(rsps_lib.calls)


class _FakeSse:
    """Stands in for an SseManager when a test drives the callbacks directly."""

    def __init__(self, log: list[str] | None = None) -> None:
        self.stopped = False
        self._log = log

    def stop(self) -> None:
        self.stopped = True
        if self._log is not None:
            self._log.append("sse")


def _ready_provider_with_fake_sse(
    **kwargs: object,
) -> tuple[DynatraceProvider, _FakeSse]:
    """An initialized, READY provider whose SSE manager is a fake the test drives."""
    rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_config(), status=200)
    p = _make_provider(**kwargs)  # type: ignore[arg-type]
    p.initialize()
    fake = _FakeSse()
    with p._lock:
        p._sse_manager = fake  # type: ignore[assignment]
    return p, fake


class TestSseStartup:
    @rsps_lib.activate
    def test_advertised_stream_connects_and_pulls_a_catch_up_fetch(
        self, sse_server: SseServer
    ) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_config(sse_server.url), status=200)
        p = _make_provider()
        p.initialize()
        try:
            assert wait_until(lambda: sse_server.client_count == 1)
            assert wait_until(lambda: _cdn_calls() == 2), "no catch-up fetch on connect"
            assert p._sse_connected
            assert p._poller is not None
            assert p._poller.interval_s == _CONNECTED_S
        finally:
            p.shutdown()

    @rsps_lib.activate
    def test_structured_endpoint_falls_back_to_the_cdn_origin(self) -> None:
        config = json.loads(_FIXTURE.read_text())
        config["eventStreams"] = [{"type": "sse", "endpoint": {"requestUri": "/sse"}}]
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=json.dumps(config), status=200)
        p = _make_provider()
        p.initialize()
        try:
            assert p._sse_manager is not None
            assert p._sse_manager._sse_url == "https://cdn.us.fm.dynatrace.com/sse"
        finally:
            p.shutdown()

    @rsps_lib.activate
    def test_no_advertised_stream_runs_polling_only(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_config(), status=200)
        p = _make_provider()
        p.initialize()
        try:
            assert p._sse_manager is None
            assert p._poller is not None
            assert p._poller.interval_s == _DISCONNECTED_S
        finally:
            p.shutdown()

    @rsps_lib.activate
    def test_disable_sse_ignores_an_advertised_stream(
        self, sse_server: SseServer
    ) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_config(sse_server.url), status=200)
        p = _make_provider(disable_sse=True)
        p.initialize()
        try:
            assert p._sse_manager is None
            assert not wait_until(lambda: sse_server.client_count > 0, timeout=0.3)
        finally:
            p.shutdown()

    @rsps_lib.activate
    def test_unreachable_stream_stays_ready_and_polls_fast(self) -> None:
        rsps_lib.add(
            rsps_lib.GET, _CDN_URL, body=_config("http://127.0.0.1:1/sse"), status=200
        )
        p = _make_provider()
        p.initialize()
        try:
            assert p._sse_manager is not None
            # faults before a first connect are not disconnects
            assert not wait_until(lambda: p._state != ProviderStatus.READY, timeout=0.5)
            assert not p._sse_connected
            assert p._poller is not None
            assert p._poller.interval_s == _DISCONNECTED_S
        finally:
            p.shutdown()

    @rsps_lib.activate
    def test_failed_init_starts_sse_from_the_first_stored_config(
        self, sse_server: SseServer
    ) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_config(sse_server.url), status=200)
        p = _make_provider(poll_s=0.05)
        with pytest.raises(GeneralError):
            p.initialize()
        try:
            assert p._sse_manager is None
            assert wait_until(lambda: sse_server.client_count == 1)
            assert wait_until(lambda: p._state == ProviderStatus.READY)
        finally:
            p.shutdown()

    def test_superseded_init_opens_no_stream(self, sse_server: SseServer) -> None:
        fetcher = _BlockingFetcher(_config(sse_server.url))
        p = DynatraceProvider(_KEY, fetcher=fetcher, poll_interval_s=600.0)
        init_thread = threading.Thread(target=p.initialize)
        init_thread.start()
        assert fetcher.fetch_started.wait(timeout=5.0)
        p.shutdown()
        fetcher.release.set()
        init_thread.join(timeout=5.0)
        assert p._sse_manager is None
        assert not wait_until(lambda: sse_server.client_count > 0, timeout=0.3)


class _BlockingFetcher:
    def __init__(self, body: str) -> None:
        self._body = body
        self.fetch_started = threading.Event()
        self.release = threading.Event()

    def fetch(self, cancel: threading.Event | None = None) -> FetchResult:
        self.fetch_started.set()
        self.release.wait(timeout=5.0)
        return FetchResult(200, self._body, None, None)

    def invalidate_cache(self) -> None:
        pass


class TestSseConnectionLifecycle:
    @rsps_lib.activate
    def test_connect_switches_cadence_and_pulls_catch_up(self) -> None:
        p, fake = _ready_provider_with_fake_sse()
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        try:
            p._on_sse_connected(fake)  # type: ignore[arg-type]
            assert p._poller is not None
            assert p._poller.interval_s == _CONNECTED_S
            assert wait_until(lambda: _cdn_calls() == 2)
        finally:
            p.shutdown()

    @rsps_lib.activate
    def test_disconnect_falls_back_to_polling_without_a_state_change(self) -> None:
        p, fake = _ready_provider_with_fake_sse()
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        events = _record_events(p)
        try:
            p._on_sse_connected(fake)  # type: ignore[arg-type]
            assert wait_until(lambda: _cdn_calls() == 2)
            p._on_sse_disconnected(fake)  # type: ignore[arg-type]
            p._on_sse_disconnected(fake)  # type: ignore[arg-type]  # repeat fault while retrying
            assert not p._sse_connected
            assert p._poller is not None
            assert p._poller.interval_s == _DISCONNECTED_S
            # SSE only prompts fetches: losing it changes the cadence, nothing else
            assert p._state == ProviderStatus.READY
            assert p._grace_timer is None
            assert not wait_until(lambda: _cdn_calls() > 2, timeout=0.2)
            assert events == []
        finally:
            p.shutdown()

    @rsps_lib.activate
    def test_reconnect_returns_to_sse_mode_and_pulls_catch_up(self) -> None:
        p, fake = _ready_provider_with_fake_sse()
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        events = _record_events(p)
        try:
            p._on_sse_connected(fake)  # type: ignore[arg-type]
            assert wait_until(lambda: _cdn_calls() == 2)
            p._on_sse_disconnected(fake)  # type: ignore[arg-type]
            p._on_sse_connected(fake)  # type: ignore[arg-type]
            assert wait_until(lambda: _cdn_calls() == 3), (
                "no catch-up after a reconnect"
            )
            assert p._sse_connected
            assert p._poller is not None
            assert p._poller.interval_s == _CONNECTED_S
            assert events == []
        finally:
            p.shutdown()

    @rsps_lib.activate
    def test_stale_provider_recovers_only_through_a_successful_fetch(self) -> None:
        p, fake = _ready_provider_with_fake_sse()
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        events = _record_events(p)
        try:
            with p._lock:
                p._sse_connected = True
            p._on_fetch_failure("HTTP 500")
            # a disconnect neither worsens nor resolves staleness
            p._on_sse_disconnected(fake)  # type: ignore[arg-type]
            assert p._state == ProviderStatus.STALE
            # the reconnect catch-up is an ordinary fetch; its success is what recovers READY
            p._on_sse_connected(fake)  # type: ignore[arg-type]
            assert wait_until(lambda: p._state == ProviderStatus.READY)
            assert events == ["stale", "ready"]
        finally:
            p.shutdown()

    @rsps_lib.activate
    def test_callbacks_from_a_replaced_stream_are_ignored(self) -> None:
        p, _ = _ready_provider_with_fake_sse()
        stale_stream = _FakeSse()
        try:
            p._on_sse_connected(stale_stream)  # type: ignore[arg-type]
            assert not p._sse_connected
            p._on_sse_message(stale_stream, SseMessage(type="refetchConfig"))  # type: ignore[arg-type]
            assert not wait_until(lambda: _cdn_calls() > 1, timeout=0.2)
        finally:
            p.shutdown()


class TestPollFailureWithSse:
    @pytest.mark.parametrize("sse_connected", [True, False], ids=["sse-up", "sse-down"])
    @rsps_lib.activate
    def test_poll_failure_goes_stale_whatever_the_sse_state(
        self, sse_connected: bool
    ) -> None:
        p, _ = _ready_provider_with_fake_sse()
        try:
            with p._lock:
                p._sse_connected = sse_connected
            p._on_fetch_failure("HTTP 500")
            assert p._state == ProviderStatus.STALE
        finally:
            p.shutdown()


class TestSseMessages:
    @rsps_lib.activate
    def test_refetch_config_triggers_a_conditional_refetch(self) -> None:
        rsps_lib.add(
            rsps_lib.GET, _CDN_URL, body=_config(), status=200, headers={"ETag": '"v1"'}
        )
        p = _make_provider()
        p.initialize()
        fake = _FakeSse()
        with p._lock:
            p._sse_manager = fake  # type: ignore[assignment]
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        try:
            p._on_sse_message(fake, SseMessage(type="refetchConfig", last_modified=1))  # type: ignore[arg-type]
            assert wait_until(lambda: _cdn_calls() == 2)
            assert rsps_lib.calls[1].request.headers["If-None-Match"] == '"v1"'
        finally:
            p.shutdown()

    @rsps_lib.activate
    def test_other_types_are_ignored(self) -> None:
        p, fake = _ready_provider_with_fake_sse()
        try:
            p._on_sse_message(fake, SseMessage(type="unknownType"))  # type: ignore[arg-type]
            assert not wait_until(lambda: _cdn_calls() > 1, timeout=0.2)
        finally:
            p.shutdown()


class TestSseShutdown:
    @rsps_lib.activate
    def test_shutdown_closes_sse_before_polling(self) -> None:
        order: list[str] = []
        p, _ = _ready_provider_with_fake_sse()
        fake = _FakeSse(order)
        with p._lock:
            p._sse_manager = fake  # type: ignore[assignment]
        assert p._poller is not None
        stop_poller = p._poller.stop

        def recording_stop() -> None:
            order.append("poller")
            stop_poller()

        p._poller.stop = recording_stop  # type: ignore[method-assign]
        p.shutdown()
        assert order == ["sse", "poller"]
        assert p._sse_manager is None

    @rsps_lib.activate
    def test_shutdown_closes_the_live_stream(self, sse_server: SseServer) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_config(sse_server.url), status=200)
        p = _make_provider()
        p.initialize()
        assert wait_until(lambda: sse_server.client_count == 1)
        assert wait_until(lambda: _cdn_calls() == 2)
        p.shutdown()
        sse_server.emit('{"type":"refetchConfig"}')
        assert wait_until(lambda: sse_server.client_count == 0)
        assert not wait_until(lambda: _cdn_calls() > 2, timeout=0.3)
