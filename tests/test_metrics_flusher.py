import json
import threading

import responses as rsps_lib

from dynatrace.openfeature._metrics_aggregator import (
    DynatraceMetricsAggregator,
    EvaluationMetricKey,
)
from dynatrace.openfeature._metrics_flusher import (
    EvaluationMetricsFlusher,
    build_payload,
)

_URL = "https://metrics.test.local/v1/metrics"


def _agg(version: str = "v1") -> DynatraceMetricsAggregator:
    agg = DynatraceMetricsAggregator()
    agg.set_version(version)
    return agg


def _flusher(
    agg: DynatraceMetricsAggregator,
    *,
    interval: float = 30.0,
    capacity: int = 1000,
    clock=None,
    session=None,
) -> EvaluationMetricsFlusher:
    return EvaluationMetricsFlusher(
        agg,
        sdk_key="my-secret-key",
        metrics_url=_URL,
        flush_interval_s=interval,
        capacity_threshold=capacity,
        sdk_version="1.2.3",
        provider_name="dynatrace",
        session=session,
        clock=clock or (lambda: 0.0),
    )


def _bodies() -> list[dict]:
    return [json.loads(c.request.body) for c in rsps_lib.calls]


class TestPayload:
    def test_documented_json_shape(self) -> None:
        evaluations = {
            "header-redesign": {
                EvaluationMetricKey("control", "targeting_match", None, 1741780820): 1
            },
            "deleted-flag": {
                EvaluationMetricKey(
                    "__default__", "error", "flag_not_found", 1741780820
                ): 1
            },
        }
        payload = json.loads(
            build_payload(evaluations, "1741780800", "1.2.3", "dynatrace")
        )
        assert payload["version"] == "1741780800"
        assert payload["sdkType"] == "server"
        assert payload["sdkVersion"] == "1.2.3"
        assert payload["providerName"] == "dynatrace"
        by_flag = {e["flagKey"]: e for e in payload["evaluations"]}
        assert by_flag["header-redesign"] == {
            "flagKey": "header-redesign",
            "variant": "control",
            "evaluationReason": "targeting_match",
            "timestampSec": 1741780820,
            "count": 1,
        }
        # errorCode present only when non-null.
        assert "errorCode" not in by_flag["header-redesign"]
        assert by_flag["deleted-flag"]["errorCode"] == "flag_not_found"


class TestDelivery:
    @rsps_lib.activate
    def test_flush_posts_live_aggregator(self) -> None:
        rsps_lib.add(rsps_lib.POST, _URL, status=202)
        agg = _agg("v1")
        agg.record("flag-a", "control", "targeting_match", None, 10)
        flusher = _flusher(agg)
        flusher._flush()
        assert len(rsps_lib.calls) == 1
        body = _bodies()[0]
        assert body["version"] == "v1"
        assert body["evaluations"][0]["flagKey"] == "flag-a"
        # Authorization header carries the bearer token.
        assert (
            rsps_lib.calls[0].request.headers["Authorization"] == "Bearer my-secret-key"
        )

    @rsps_lib.activate
    def test_empty_aggregator_does_not_post(self) -> None:
        rsps_lib.add(rsps_lib.POST, _URL, status=202)
        flusher = _flusher(_agg())
        flusher._flush()
        assert len(rsps_lib.calls) == 0

    @rsps_lib.activate
    def test_sealed_batches_flushed_before_live(self) -> None:
        rsps_lib.add(rsps_lib.POST, _URL, status=202)
        agg = _agg("v1")
        agg.record("old-flag", "control", "static", None, 10)
        sealed = agg.drain()  # version v1
        agg.set_version("v2")
        agg.record("new-flag", "control", "static", None, 10)

        flusher = _flusher(agg)
        flusher.enqueue_sealed_batch(sealed)
        flusher._flush()

        bodies = _bodies()
        assert len(bodies) == 2
        # Sealed (v1) delivered first, then live (v2).
        assert bodies[0]["version"] == "v1"
        assert bodies[0]["evaluations"][0]["flagKey"] == "old-flag"
        assert bodies[1]["version"] == "v2"
        assert bodies[1]["evaluations"][0]["flagKey"] == "new-flag"


class TestFailureHandling:
    @rsps_lib.activate
    def test_400_drops_batch_and_continues(self) -> None:
        rsps_lib.add(rsps_lib.POST, _URL, status=400)  # first sealed -> dropped
        rsps_lib.add(rsps_lib.POST, _URL, status=202)  # second sealed -> delivered
        agg = _agg("v1")
        agg.record("a", "v", "static", None, 10)
        first = agg.drain()
        agg.record("b", "v", "static", None, 10)
        second = agg.drain()

        flusher = _flusher(agg)
        flusher.enqueue_sealed_batch(first)
        flusher.enqueue_sealed_batch(second)
        flusher._flush()

        assert len(rsps_lib.calls) == 2
        # Both batches consumed; nothing requeued.
        assert len(flusher._sealed) == 0

    @rsps_lib.activate
    def test_5xx_requeues_then_succeeds(self) -> None:
        rsps_lib.add(rsps_lib.POST, _URL, status=500)
        rsps_lib.add(rsps_lib.POST, _URL, status=202)
        agg = _agg("v1")
        agg.record("a", "v", "static", None, 10)
        sealed = agg.drain()

        flusher = _flusher(agg)
        flusher.enqueue_sealed_batch(sealed)

        flusher._flush()  # 500 -> requeued
        assert len(flusher._sealed) == 1
        flusher._flush()  # 202 -> delivered
        assert len(flusher._sealed) == 0
        assert len(rsps_lib.calls) == 2

    def test_uncaught_exception_requeues_live_batch(self) -> None:
        class BoomSession:
            def post(self, *args: object, **kwargs: object) -> object:
                raise ValueError("boom")

        agg = _agg("v1")
        agg.record("a", "v", "static", None, 10)
        flusher = _flusher(agg, session=BoomSession())
        # _flush_quietly swallows the exception; the live batch must be requeued.
        flusher._flush_quietly()
        assert len(flusher._sealed) == 1

    @rsps_lib.activate
    def test_failed_delivery_sets_cooldown(self) -> None:
        rsps_lib.add(rsps_lib.POST, _URL, status=500)
        now = [100.0]
        agg = _agg("v1")
        agg.record("a", "v", "static", None, 10)
        flusher = _flusher(agg, clock=lambda: now[0])
        flusher.enqueue_sealed_batch(agg.drain())
        flusher._flush()
        assert flusher._next_out_of_band_flush_allowed == 100.0 + 30.0


class TestQueueCap:
    def test_max_sealed_batches_drops_incoming(self) -> None:
        agg = _agg("v1")
        flusher = _flusher(agg)
        for i in range(15):
            agg.record(f"flag-{i}", "v", "static", None, 10)
            flusher.enqueue_sealed_batch(agg.drain())
        assert len(flusher._sealed) == 10

    def test_empty_batch_not_enqueued(self) -> None:
        agg = _agg("v1")
        flusher = _flusher(agg)
        flusher.enqueue_sealed_batch(agg.drain())  # empty
        assert len(flusher._sealed) == 0


class TestCapacityTrigger:
    def test_on_record_wakes_when_over_threshold(self) -> None:
        agg = _agg("v1")
        agg.record("a", "v", "static", None, 10)
        flusher = _flusher(agg, capacity=1)
        flusher._started = True
        flusher.on_record()
        assert flusher._wake.is_set()

    def test_on_record_noop_below_threshold(self) -> None:
        agg = _agg("v1")
        agg.record("a", "v", "static", None, 10)
        flusher = _flusher(agg, capacity=5)
        flusher._started = True
        flusher.on_record()
        assert not flusher._wake.is_set()

    def test_on_record_suppressed_during_cooldown(self) -> None:
        agg = _agg("v1")
        agg.record("a", "v", "static", None, 10)
        now = [0.0]
        flusher = _flusher(agg, capacity=1, clock=lambda: now[0])
        flusher._started = True
        flusher._next_out_of_band_flush_allowed = 50.0
        flusher.on_record()
        assert not flusher._wake.is_set()
        # Once the cooldown elapses, capacity flushes resume.
        now[0] = 60.0
        flusher.on_record()
        assert flusher._wake.is_set()


class TestLifecycle:
    @rsps_lib.activate
    def test_shutdown_performs_final_flush(self) -> None:
        rsps_lib.add(rsps_lib.POST, _URL, status=202)
        agg = _agg("v1")
        flusher = _flusher(agg, clock=lambda: 0.0)
        flusher.start()
        agg.record("a", "v", "static", None, 10)
        flusher.shutdown()
        assert len(rsps_lib.calls) == 1

    @rsps_lib.activate
    def test_periodic_flush_runs(self) -> None:
        delivered = threading.Event()

        def callback(_request: object) -> tuple[int, dict, str]:
            delivered.set()
            return (202, {}, "")

        rsps_lib.add_callback(rsps_lib.POST, _URL, callback=callback)
        agg = _agg("v1")
        flusher = _flusher(agg, interval=0.05)
        flusher.start()
        agg.record("a", "v", "static", None, 10)
        assert delivered.wait(timeout=3.0), "periodic flush never fired"
        flusher.shutdown()

    def test_start_is_idempotent(self) -> None:
        flusher = _flusher(_agg())
        flusher.start()
        first = flusher._thread
        flusher.start()
        assert flusher._thread is first
        flusher.shutdown()
