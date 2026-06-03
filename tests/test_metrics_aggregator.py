import threading

import pytest

from dynatrace.openfeature._metrics_aggregator import (
    DynatraceMetricsAggregator,
    EvaluationMetricKey,
)


def _agg(version: str | None = "v1", max_distinct: int = 2000) -> DynatraceMetricsAggregator:
    agg = DynatraceMetricsAggregator(max_distinct)
    if version is not None:
        agg.set_version(version)
    return agg


class TestRecord:
    def test_records_and_aggregates_counts(self) -> None:
        agg = _agg()
        agg.record("flag-a", "control", "targeting_match", None, 1741780820)
        agg.record("flag-a", "control", "targeting_match", None, 1741780820)
        batch = agg.drain()
        key = EvaluationMetricKey("control", "targeting_match", None, 1741780820)
        assert batch.evaluations == {"flag-a": {key: 2}}

    def test_typed_key_has_no_delimiter_collisions(self) -> None:
        # Two distinct dimension tuples that a "a|b|c" string-concat key could
        # collide on remain distinct as typed tuples.
        agg = _agg()
        agg.record("flag", "a|b", "c", None, 10)
        agg.record("flag", "a", "b|c", None, 10)
        buckets = agg.drain().evaluations["flag"]
        assert len(buckets) == 2

    def test_drops_records_when_no_version_set(self) -> None:
        agg = DynatraceMetricsAggregator()  # no version observed yet
        agg.record("flag-a", "control", "targeting_match", None, 10)
        assert agg.size() == 0
        assert agg.drain().is_empty

    def test_version_mismatch_dropped(self) -> None:
        agg = _agg(version="v1")
        agg.record("flag-a", "control", "static", None, 10, evaluation_version="v2")
        assert agg.size() == 0

    def test_version_mismatch_dropped_even_when_map_empty(self) -> None:
        agg = _agg(version="v1")
        agg.record("flag-a", "control", "static", None, 10, evaluation_version="other")
        assert agg.drain().is_empty

    def test_matching_version_recorded(self) -> None:
        agg = _agg(version="v1")
        agg.record("flag-a", "control", "static", None, 10, evaluation_version="v1")
        assert agg.size() == 1

    def test_null_evaluation_version_always_accepted(self) -> None:
        agg = _agg(version="v1")
        agg.record("flag-a", "control", "static", None, 10, evaluation_version=None)
        assert agg.size() == 1


class TestHardCap:
    def test_drops_new_flags_beyond_cap_but_existing_keep_counting(self) -> None:
        agg = _agg(max_distinct=2)
        agg.record("flag-a", "v", "static", None, 10)
        agg.record("flag-b", "v", "static", None, 10)
        # Third distinct flag is dropped.
        agg.record("flag-c", "v", "static", None, 10)
        assert agg.size() == 2
        # Existing flags continue to accumulate.
        agg.record("flag-a", "v", "static", None, 10)
        batch = agg.drain()
        assert "flag-c" not in batch.evaluations
        key = EvaluationMetricKey("v", "static", None, 10)
        assert batch.evaluations["flag-a"][key] == 2


class TestVersion:
    def test_set_version_ignores_blank(self) -> None:
        agg = DynatraceMetricsAggregator()
        agg.set_version("  ")
        assert agg.get_version() is None
        agg.set_version("v1")
        assert agg.get_version() == "v1"

    def test_record_never_mutates_version(self) -> None:
        agg = _agg(version="v1")
        agg.record("flag", "v", "static", None, 10, evaluation_version="v1")
        assert agg.get_version() == "v1"


class TestDrain:
    def test_drain_swaps_and_captures_version(self) -> None:
        agg = _agg(version="v1")
        agg.record("flag", "v", "static", None, 10)
        batch = agg.drain()
        assert batch.version == "v1"
        assert not batch.is_empty
        # Live map is now empty but the aggregator keeps its version.
        assert agg.size() == 0
        assert agg.get_version() == "v1"

    def test_constructor_rejects_non_positive_cap(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            DynatraceMetricsAggregator(0)


class TestConcurrency:
    def test_concurrent_record_and_drain_loses_nothing(self) -> None:
        agg = _agg(version="v1")
        threads_n = 4
        per_thread = 1000
        drained_total = 0

        def writer() -> None:
            for _ in range(per_thread):
                agg.record("flag", "v", "static", None, 10, evaluation_version="v1")

        def total_of(batch_evals: dict) -> int:
            return sum(c for buckets in batch_evals.values() for c in buckets.values())

        workers = [threading.Thread(target=writer) for _ in range(threads_n)]
        for w in workers:
            w.start()

        # Interleave drains while writers run.
        while any(w.is_alive() for w in workers):
            drained_total += total_of(agg.drain().evaluations)
        for w in workers:
            w.join()
        drained_total += total_of(agg.drain().evaluations)

        assert drained_total == threads_n * per_thread
