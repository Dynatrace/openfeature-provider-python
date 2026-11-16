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


def _flag_config(sse_url: str | None = None) -> str:
    config = json.loads(_FIXTURE.read_text())
    if sse_url is not None:
        config["eventStreams"] = [{"type": "sse", "url": sse_url}]
    return json.dumps(config)


def _make_provider(
    *, poll_s: float = _DISCONNECTED_S, grace_s: float = 60.0, disable_sse: bool = False
) -> DynatraceProvider:
    provider = DynatraceProvider(
        _KEY,
        fetcher=CdnFetcher(DynatraceProviderOptions.from_sdk_key(_KEY)),
        poll_interval_s=poll_s,
        disable_sse=disable_sse,
        grace_period_s=grace_s,
    )
    provider._sse_connected_poll_interval_s = _CONNECTED_S
    return provider


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
    rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_flag_config(), status=200)
    provider = _make_provider(**kwargs)  # type: ignore[arg-type]
    provider.initialize()
    fake = _FakeSse()
    with provider._lock:
        provider._sse_manager = fake  # type: ignore[assignment]
    return provider, fake


class TestSseStartup:
    @rsps_lib.activate
    def test_advertised_stream_connects_and_pulls_a_catch_up_fetch(
        self, sse_server: SseServer
    ) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_flag_config(sse_server.url), status=200)
        provider = _make_provider()
        provider.initialize()
        try:
            assert wait_until(lambda: sse_server.client_count == 1)
            # first on init, second on sse connection
            assert wait_until(lambda: _cdn_calls() == 2), "no catch-up fetch on connect"
            assert provider._sse_connected
            assert provider._poller is not None
            assert wait_until(lambda: provider._poller.interval_s == _CONNECTED_S)
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_structured_endpoint_falls_back_to_the_cdn_origin(self) -> None:
        config = json.loads(_FIXTURE.read_text())
        config["eventStreams"] = [{"type": "sse", "endpoint": {"requestUri": "/sse"}}]
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=json.dumps(config), status=200)
        provider = _make_provider()
        provider.initialize()
        try:
            assert provider._sse_manager is not None
            assert provider._sse_manager._sse_url == "https://cdn.us.fm.dynatrace.com/sse"
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_no_advertised_stream_runs_polling_only(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_flag_config(), status=200)
        provider = _make_provider()
        provider.initialize()
        try:
            assert provider._sse_manager is None
            assert provider._poller is not None
            assert provider._poller.interval_s == _DISCONNECTED_S
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_disable_sse_ignores_an_advertised_stream(
        self, sse_server: SseServer
    ) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_flag_config(sse_server.url), status=200)
        provider = _make_provider(disable_sse=True)
        provider.initialize()
        try:
            assert provider._sse_manager is None
            assert not wait_until(lambda: sse_server.client_count > 0, timeout=0.5)
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_unreachable_stream_stays_ready_and_polls_fast(self) -> None:
        rsps_lib.add(
            rsps_lib.GET, _CDN_URL, body=_flag_config("http://127.0.0.1:1/sse"), status=200
        )
        provider = _make_provider()
        provider.initialize()
        try:
            assert provider._sse_manager is not None
            # faults before a first connect are not disconnects
            assert not wait_until(lambda: provider._state != ProviderStatus.READY, timeout=0.5)
            assert not provider._sse_connected
            assert provider._poller is not None
            assert provider._poller.interval_s == _DISCONNECTED_S
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_failed_init_starts_sse_from_the_first_stored_config(
        self, sse_server: SseServer
    ) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_flag_config(sse_server.url), status=200)
        provider = _make_provider(poll_s=0.05)
        with pytest.raises(GeneralError):
            provider.initialize()
        try:
            assert provider._sse_manager is None
            assert wait_until(lambda: sse_server.client_count == 1)
            assert wait_until(lambda: provider._state == ProviderStatus.READY)
        finally:
            provider.shutdown()

    def test_superseded_init_opens_no_stream(self, sse_server: SseServer) -> None:
        fetcher = _BlockingFetcher(_flag_config(sse_server.url))
        provider = DynatraceProvider(_KEY, fetcher=fetcher, poll_interval_s=600.0)
        init_thread = threading.Thread(target=provider.initialize)
        init_thread.start()
        assert fetcher.fetch_started.wait(timeout=5.0)
        provider.shutdown()
        fetcher.release.set()
        init_thread.join(timeout=5.0)
        assert provider._sse_manager is None
        assert not wait_until(lambda: sse_server.client_count > 0, timeout=0.5)


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
    def test_connect_alone_keeps_the_normal_cadence(self) -> None:
        provider, fake = _ready_provider_with_fake_sse()
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        try:
            # holding the lock keeps the catch-up's success callback from running yet
            with provider._lock:
                provider._on_sse_connected(fake)  # type: ignore[arg-type]
                assert provider._sse_connected
                assert provider._poller is not None
                assert provider._poller.interval_s == _DISCONNECTED_S
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_catch_up_success_relaxes_the_cadence(self) -> None:
        provider, fake = _ready_provider_with_fake_sse()
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        try:
            provider._on_sse_connected(fake)  # type: ignore[arg-type]
            assert wait_until(lambda: _cdn_calls() == 2)
            assert provider._poller is not None
            assert wait_until(lambda: provider._poller.interval_s == _CONNECTED_S)
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_failed_catch_up_keeps_the_short_cadence(self) -> None:
        provider, fake = _ready_provider_with_fake_sse()
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        try:
            provider._on_sse_connected(fake)  # type: ignore[arg-type]
            assert wait_until(lambda: provider._state == ProviderStatus.STALE)
            assert provider._sse_connected
            assert provider._poller is not None
            assert provider._poller.interval_s == _DISCONNECTED_S
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_failure_while_connected_restores_the_short_cadence(self) -> None:
        provider, fake = _ready_provider_with_fake_sse()
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)  # catch-up
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)  # refetch, both attempts
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)  # recovery
        try:
            provider._on_sse_connected(fake)  # type: ignore[arg-type]
            assert provider._poller is not None
            assert wait_until(lambda: provider._poller.interval_s == _CONNECTED_S)

            provider._on_sse_message(fake, SseMessage(type="refetchConfig"))  # type: ignore[arg-type]
            assert wait_until(lambda: provider._state == ProviderStatus.STALE)
            assert provider._sse_connected, "the stream itself is still up"
            assert provider._poller.interval_s == _DISCONNECTED_S

            # the next success with SSE still connected relaxes it again
            provider._on_sse_message(fake, SseMessage(type="refetchConfig"))  # type: ignore[arg-type]
            assert wait_until(lambda: provider._state == ProviderStatus.READY)
            assert wait_until(lambda: provider._poller.interval_s == _CONNECTED_S)
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_success_without_sse_keeps_the_short_cadence(self) -> None:
        provider, _ = _ready_provider_with_fake_sse()
        try:
            provider._on_fetch_success()
            assert provider._poller is not None
            assert provider._poller.interval_s == _DISCONNECTED_S
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_disconnect_falls_back_to_polling_without_a_state_change(self) -> None:
        provider, fake = _ready_provider_with_fake_sse()
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        events = _record_events(provider)
        try:
            provider._on_sse_connected(fake)  # type: ignore[arg-type]
            assert wait_until(lambda: _cdn_calls() == 2)
            assert provider._poller is not None
            assert wait_until(lambda: provider._poller.interval_s == _CONNECTED_S)
            provider._on_sse_disconnected(fake)  # type: ignore[arg-type]
            provider._on_sse_disconnected(fake)  # type: ignore[arg-type]  # repeat fault while retrying
            assert not provider._sse_connected
            assert provider._poller is not None
            assert provider._poller.interval_s == _DISCONNECTED_S
            # SSE only prompts fetches: losing it changes the cadence, nothing else
            assert provider._state == ProviderStatus.READY
            assert provider._grace_timer is None
            assert not wait_until(lambda: _cdn_calls() > 2, timeout=0.5)
            assert events == []
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_reconnect_returns_to_sse_mode_and_pulls_catch_up(self) -> None:
        provider, fake = _ready_provider_with_fake_sse()
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        events = _record_events(provider)
        try:
            provider._on_sse_connected(fake)  # type: ignore[arg-type]
            assert wait_until(lambda: _cdn_calls() == 2)
            provider._on_sse_disconnected(fake)  # type: ignore[arg-type]
            provider._on_sse_connected(fake)  # type: ignore[arg-type]
            assert wait_until(lambda: _cdn_calls() == 3), (
                "no catch-up after a reconnect"
            )
            assert provider._sse_connected
            assert provider._poller is not None
            assert wait_until(lambda: provider._poller.interval_s == _CONNECTED_S)
            assert events == []
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_stale_provider_recovers_only_through_a_successful_fetch(self) -> None:
        provider, fake = _ready_provider_with_fake_sse()
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        events = _record_events(provider)
        try:
            with provider._lock:
                provider._sse_connected = True
            provider._on_fetch_failure("HTTP 500")
            # a disconnect neither worsens nor resolves staleness
            provider._on_sse_disconnected(fake)  # type: ignore[arg-type]
            assert provider._state == ProviderStatus.STALE
            # the reconnect catch-up is an ordinary fetch; its success is what recovers READY
            provider._on_sse_connected(fake)  # type: ignore[arg-type]
            assert wait_until(lambda: provider._state == ProviderStatus.READY)
            assert events == ["stale", "ready"]
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_callbacks_from_a_replaced_stream_are_ignored(self) -> None:
        provider, _ = _ready_provider_with_fake_sse()
        stale_stream = _FakeSse()
        try:
            provider._on_sse_connected(stale_stream)  # type: ignore[arg-type]
            assert not provider._sse_connected
            provider._on_sse_message(stale_stream, SseMessage(type="refetchConfig"))  # type: ignore[arg-type]
            assert not wait_until(lambda: _cdn_calls() > 1, timeout=0.2)
        finally:
            provider.shutdown()


class TestSseUrlChanges:
    # the catch-up fetch on connect stores the second CDN response, which is how the config changes here

    @rsps_lib.activate
    def test_rotated_url_reconnects_to_the_new_stream(self, sse_server: SseServer) -> None:
        old_url, new_url = sse_server.url + "?v=1", sse_server.url + "?v=2"
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_flag_config(old_url), status=200)
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_flag_config(new_url), status=200)
        provider = _make_provider()
        provider.initialize()
        old_manager = provider._sse_manager
        try:
            assert old_manager is not None
            assert old_manager.url == old_url
            assert wait_until(lambda: sse_server.paths == ["/sse?v=1", "/sse?v=2"])
            assert provider._sse_manager is not old_manager
            assert provider._sse_manager is not None
            assert provider._sse_manager.url == new_url
            # the old stream is closed, so only the new one remains
            assert wait_until(lambda: sse_server.client_count == 1)
            assert wait_until(lambda: provider._sse_connected)
            # the new stream's catch-up fetch sees the same URL, so it settles instead of reconnecting again
            assert not wait_until(lambda: _cdn_calls() > 3, timeout=0.5)
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_withdrawn_stream_falls_back_to_polling(self, sse_server: SseServer) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_flag_config(sse_server.url), status=200)
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_flag_config(), status=200)
        provider = _make_provider()
        provider.initialize()
        try:
            assert wait_until(lambda: sse_server.paths == ["/sse"])
            assert wait_until(lambda: _cdn_calls() == 2)
            assert wait_until(lambda: provider._sse_manager is None)
            assert not provider._sse_connected
            assert provider._poller is not None
            assert provider._poller.interval_s == _DISCONNECTED_S
            assert wait_until(lambda: sse_server.client_count == 0)
            assert provider._state == ProviderStatus.READY
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_unchanged_url_keeps_the_stream(self, sse_server: SseServer) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_flag_config(sse_server.url), status=200)
        provider = _make_provider()
        provider.initialize()
        manager = provider._sse_manager
        try:
            assert wait_until(lambda: _cdn_calls() == 2)
            assert wait_until(lambda: provider._sse_connected)
            assert provider._sse_manager is manager
            assert len(sse_server.paths) == 1
        finally:
            provider.shutdown()


class TestPollFailureWithSse:
    @pytest.mark.parametrize("sse_connected", [True, False], ids=["sse-up", "sse-down"])
    @rsps_lib.activate
    def test_poll_failure_goes_stale_regardless_of_sse_state(
        self, sse_connected: bool
    ) -> None:
        provider, _ = _ready_provider_with_fake_sse()
        try:
            with provider._lock:
                provider._sse_connected = sse_connected
            provider._on_fetch_failure("HTTP 500")
            assert provider._state == ProviderStatus.STALE
        finally:
            provider.shutdown()


class TestSseMessages:
    @rsps_lib.activate
    def test_refetch_config_triggers_a_conditional_refetch(self) -> None:
        rsps_lib.add(
            rsps_lib.GET, _CDN_URL, body=_flag_config(), status=200, headers={"ETag": '"v1"'}
        )
        provider = _make_provider()
        provider.initialize()
        fake = _FakeSse()
        with provider._lock:
            provider._sse_manager = fake  # type: ignore[assignment]
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        try:
            provider._on_sse_message(fake, SseMessage(type="refetchConfig", last_modified=1))  # type: ignore[arg-type]
            assert wait_until(lambda: _cdn_calls() == 2)
            assert rsps_lib.calls[1].request.headers["If-None-Match"] == '"v1"'
        finally:
            provider.shutdown()

    @rsps_lib.activate
    def test_other_types_are_ignored(self) -> None:
        provider, fake = _ready_provider_with_fake_sse()
        try:
            provider._on_sse_message(fake, SseMessage(type="unknownType"))  # type: ignore[arg-type]
            assert not wait_until(lambda: _cdn_calls() > 1, timeout=0.5)
        finally:
            provider.shutdown()


class TestSseShutdown:
    @rsps_lib.activate
    def test_shutdown_closes_sse_before_polling(self) -> None:
        order: list[str] = []
        provider, _ = _ready_provider_with_fake_sse()
        fake = _FakeSse(order)
        with provider._lock:
            provider._sse_manager = fake  # type: ignore[assignment]
        assert provider._poller is not None
        stop_poller = provider._poller.stop

        def recording_stop() -> None:
            order.append("poller")
            stop_poller()

        provider._poller.stop = recording_stop  # type: ignore[method-assign]
        provider.shutdown()
        assert order == ["sse", "poller"]
        assert provider._sse_manager is None

    @rsps_lib.activate
    def test_shutdown_closes_the_live_stream(self, sse_server: SseServer) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_flag_config(sse_server.url), status=200)
        provider = _make_provider()
        provider.initialize()
        assert wait_until(lambda: sse_server.client_count == 1)
        assert wait_until(lambda: _cdn_calls() == 2)
        provider.shutdown()
        sse_server.emit('{"type":"refetchConfig"}')
        assert wait_until(lambda: sse_server.client_count == 0)
        assert not wait_until(lambda: _cdn_calls() > 2, timeout=0.5)
