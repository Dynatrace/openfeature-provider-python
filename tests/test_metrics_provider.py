from pathlib import Path

import responses as rsps_lib
from openfeature import api

from dynatrace.openfeature import DynatraceProvider
from dynatrace.openfeature._cdn import CdnFetcher
from dynatrace.openfeature._metrics_aggregator import DrainedBatch
from dynatrace.openfeature._metrics_hook import DynatraceMetricsHook
from dynatrace.openfeature.options import DynatraceProviderOptions

_KEY = "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
_CDN_URL = "https://cdn.us.fm.dynatrace.com/server/" + _KEY + ".json"
_FIXTURES = Path(__file__).parent / "fixtures"
_V1 = (_FIXTURES / "flag-config-versioned.json").read_text()
_V2 = (_FIXTURES / "flag-config-v2.json").read_text()


class _FakeFlusher:
    def __init__(self) -> None:
        self.started = False
        self.shutdown_called = False
        self.enqueued: list[DrainedBatch] = []
        self.trigger_count = 0
        self.record_count = 0

    def start(self) -> None:
        self.started = True

    def shutdown(self) -> None:
        self.shutdown_called = True

    def on_record(self) -> None:
        self.record_count += 1

    def enqueue_sealed_batch(self, batch: DrainedBatch | None) -> None:
        if batch is not None:
            self.enqueued.append(batch)

    def trigger_flush(self) -> None:
        self.trigger_count += 1


def _fetcher() -> CdnFetcher:
    return CdnFetcher(DynatraceProviderOptions.from_sdk_key(_KEY))


def _provider(flusher: _FakeFlusher | None = None) -> DynatraceProvider:
    return DynatraceProvider(
        _KEY,
        fetcher=_fetcher(),
        poll_interval_s=600.0,
        metrics_flusher=flusher,  # type: ignore[arg-type]
    )


class TestWiring:
    def test_get_provider_hooks_returns_metrics_hook(self) -> None:
        p = _provider(_FakeFlusher())
        hooks = p.get_provider_hooks()
        assert len(hooks) == 1
        assert isinstance(hooks[0], DynatraceMetricsHook)

    @rsps_lib.activate
    def test_version_set_eagerly_on_init(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_V1, status=200)
        flusher = _FakeFlusher()
        p = _provider(flusher)
        p.initialize()
        assert p._metrics_aggregator.get_version() == "1741780800"
        assert flusher.started is True
        p.shutdown()

    @rsps_lib.activate
    def test_config_rotation_seals_old_version_before_bump(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_V1, status=200)
        flusher = _FakeFlusher()
        p = _provider(flusher)
        p.initialize()
        # Accumulate an evaluation under the v1 config.
        p._metrics_aggregator.record(
            "my-bool-flag", "on", "static", None, 10, evaluation_version="1741780800"
        )

        # Simulate the poller applying a new config version.
        p._store_flags(_V2)

        # The old batch was sealed (v1) and a flush triggered, BEFORE the version bumped.
        assert len(flusher.enqueued) == 1
        assert flusher.enqueued[0].version == "1741780800"
        assert flusher.trigger_count == 1
        # Aggregator now carries the new version.
        assert p._metrics_aggregator.get_version() == "1741780900"
        p.shutdown()

    @rsps_lib.activate
    def test_shutdown_invokes_flusher_shutdown(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_V1, status=200)
        flusher = _FakeFlusher()
        p = _provider(flusher)
        p.initialize()
        p.shutdown()
        assert flusher.shutdown_called is True


class TestEndToEnd:
    @rsps_lib.activate
    def test_evaluation_through_client_records_metrics(self) -> None:
        rsps_lib.add(rsps_lib.GET, _CDN_URL, body=_V1, status=200)
        provider = _provider()  # real flusher, but interval is long so it won't auto-fire
        api.set_provider(provider)
        provider.initialize()
        try:
            client = api.get_client()
            client.get_boolean_value("my-bool-flag", False)
            # The provider hook ran via the SDK and recorded into the aggregator.
            batch = provider._metrics_aggregator.drain()
            assert "my-bool-flag" in batch.evaluations
        finally:
            api.clear_providers()


class TestOptions:
    def test_metrics_url_derived_from_geo(self) -> None:
        opts = DynatraceProviderOptions.from_sdk_key(_KEY)
        assert opts.metrics_url == "https://metrics.us.fm.dynatrace.com/v1/metrics"

    def test_metrics_url_override(self) -> None:
        opts = DynatraceProviderOptions.from_sdk_key(
            _KEY, metrics_url="https://custom.example.com/m"
        )
        assert opts.metrics_url == "https://custom.example.com/m"
