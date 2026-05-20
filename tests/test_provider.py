import json
import threading
from pathlib import Path
from random import Random

import pytest
import responses as rsps_lib
from openfeature.exception import FlagNotFoundError
from openfeature.provider import ProviderStatus

from dynatrace.openfeature import DynatraceProvider, DynatraceProviderOptions
from dynatrace.openfeature._cdn import CdnFetcher
from dynatrace.openfeature._retry import RetryStrategy

_KEY = 'dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5'
_CDN_URL = 'https://cdn.us.fm.dynatrace.com/server/' + _KEY + '.json'
_FIXTURE = Path(__file__).parent / 'fixtures' / 'flag-config.json'


def _fast_fetcher(opts: DynatraceProviderOptions) -> CdnFetcher:
    retry = RetryStrategy(rng=Random(0), sleeper=lambda _: None)  # noqa: S311
    return CdnFetcher(opts, retry)


def _make_provider(
    grace_s: float = 60.0,
    poll_s: float = 600.0,
) -> DynatraceProvider:
    opts = DynatraceProviderOptions.from_sdk_key(_KEY)
    return DynatraceProvider(
        opts,
        fetcher=_fast_fetcher(opts),
        poll_interval_s=poll_s,
        grace_period_s=grace_s,
    )


class TestInitialize:
    @rsps_lib.activate
    def test_successful_init_sets_ready(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_FIXTURE.read_text(), status=200)
        p = _make_provider()
        p.initialize()
        assert p._state == ProviderStatus.READY
        p.shutdown()

    @rsps_lib.activate
    def test_init_failure_on_404_raises(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=404)
        p = _make_provider()
        with pytest.raises(Exception, match='HTTP 404'):
            p.initialize()

    @rsps_lib.activate
    def test_init_failure_on_empty_body_raises(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body='', status=200)
        p = _make_provider()
        with pytest.raises(Exception, match='empty'):
            p.initialize()


class TestEvaluation:
    def setup_method(self) -> None:
        rsps_lib.start()
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_FIXTURE.read_text(), status=200)
        self._provider = _make_provider()
        self._provider.initialize()

    def teardown_method(self) -> None:
        self._provider.shutdown()
        rsps_lib.stop()
        rsps_lib.reset()

    def test_boolean_evaluation(self) -> None:
        result = self._provider.resolve_boolean_details('my-bool-flag', False)
        assert result.value is True

    def test_string_evaluation(self) -> None:
        result = self._provider.resolve_string_details('my-string-flag', 'default')
        assert result.value == 'world'

    def test_integer_evaluation(self) -> None:
        result = self._provider.resolve_integer_details('my-int-flag', 0)
        assert result.value == 42

    def test_float_evaluation(self) -> None:
        result = self._provider.resolve_float_details('my-float-flag', 0.0)
        assert abs(result.value - 3.14) < 0.001

    def test_object_evaluation(self) -> None:
        result = self._provider.resolve_object_details('my-obj-flag', {})
        assert result.value == {'key': 'value'}

    def test_unknown_flag_raises_not_found(self) -> None:
        with pytest.raises(FlagNotFoundError):
            self._provider.resolve_boolean_details('no-such-flag', False)

    def test_flag_not_found_before_init(self) -> None:
        opts = DynatraceProviderOptions.from_sdk_key(_KEY)
        p = DynatraceProvider(opts, fetcher=_fast_fetcher(opts))
        with pytest.raises(FlagNotFoundError):
            p.resolve_boolean_details('my-bool-flag', False)


class TestStateMachine:
    @rsps_lib.activate
    def test_ready_to_stale_on_poll_failure(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_FIXTURE.read_text(), status=200)
        p = _make_provider(poll_s=0.05, grace_s=60.0)
        p.initialize()
        stale_event = threading.Event()
        # register a stale listener
        original = p.emit_provider_stale
        def on_stale(details: object) -> None:
            original(details)  # type: ignore[arg-type]
            stale_event.set()
        p.emit_provider_stale = on_stale  # type: ignore[method-assign]
        # now fail CDN
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=500)
        assert stale_event.wait(timeout=3.0), 'never transitioned to STALE'
        assert p._state == ProviderStatus.STALE
        p.shutdown()

    @rsps_lib.activate
    def test_stale_to_ready_on_recovery(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_FIXTURE.read_text(), status=200)
        p = _make_provider(poll_s=0.05, grace_s=60.0)
        p.initialize()
        # force to STALE
        ready_event = threading.Event()
        original = p.emit_provider_ready
        def on_ready(details: object) -> None:
            original(details)  # type: ignore[arg-type]
            ready_event.set()
        p.emit_provider_ready = on_ready  # type: ignore[method-assign]
        with p._state_lock:
            p._state = ProviderStatus.STALE
        # next poll succeeds
        rsps_lib.add(rsps_lib.GET, _CDN_URL, status=304)
        assert ready_event.wait(timeout=3.0), 'never recovered to READY'
        assert p._state == ProviderStatus.READY
        p.shutdown()

    @rsps_lib.activate
    def test_stale_to_error_after_grace(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_FIXTURE.read_text(), status=200)
        p = _make_provider(poll_s=600.0, grace_s=0.05)
        p.initialize()
        error_event = threading.Event()
        original = p.emit_provider_error
        def on_error(details: object) -> None:
            original(details)  # type: ignore[arg-type]
            error_event.set()
        p.emit_provider_error = on_error  # type: ignore[method-assign]
        with p._state_lock:
            p._state = ProviderStatus.STALE
            p._start_grace_timer()
        assert error_event.wait(timeout=2.0), 'never transitioned to ERROR'
        assert p._state == ProviderStatus.ERROR
        p.shutdown()

    @rsps_lib.activate
    def test_config_changed_event_emitted(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_FIXTURE.read_text(), status=200)
        p = _make_provider(poll_s=0.05)
        p.initialize()
        changed_event = threading.Event()
        original = p.emit_provider_configuration_changed
        def on_changed(details: object) -> None:
            original(details)  # type: ignore[arg-type]
            changed_event.set()
        p.emit_provider_configuration_changed = on_changed  # type: ignore[method-assign]
        # return config with a new flag
        new_config = json.loads(_FIXTURE.read_text())
        new_config['flags']['new-flag'] = {
            'state': 'ENABLED', 'defaultVariant': 'v', 'variants': {'v': True}
        }
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=json.dumps(new_config), status=200)
        assert changed_event.wait(timeout=3.0), 'config_changed never fired'
        p.shutdown()


class TestShutdown:
    @rsps_lib.activate
    def test_idempotent_shutdown(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_FIXTURE.read_text(), status=200)
        p = _make_provider()
        p.initialize()
        p.shutdown()
        p.shutdown()  # second call must not raise
        assert p._state == ProviderStatus.NOT_READY
