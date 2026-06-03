from collections.abc import Mapping

from openfeature.evaluation_context import EvaluationContext
from openfeature.exception import ErrorCode
from openfeature.flag_evaluation import FlagEvaluationDetails, FlagType, Reason
from openfeature.hook import HookContext

from dynatrace.openfeature._metrics_aggregator import (
    DynatraceMetricsAggregator,
    EvaluationMetricKey,
)
from dynatrace.openfeature._metrics_hook import DynatraceMetricsHook

# Fixed clock at epoch 1741780823 -> 10s bucket 1741780820.
_FIXED_NOW = 1741780823.0
_BUCKET = 1741780820


def _aggregator() -> DynatraceMetricsAggregator:
    agg = DynatraceMetricsAggregator()
    agg.set_version("v1")
    return agg


def _details(
    *,
    flag_key: str = "header-redesign",
    variant: str | None = "control",
    reason: str | Reason | None = Reason.TARGETING_MATCH,
    error_code: ErrorCode | None = None,
    metadata: Mapping[str, object] | None = None,
) -> FlagEvaluationDetails:
    return FlagEvaluationDetails(
        flag_key=flag_key,
        value=True,
        variant=variant,
        flag_metadata=metadata or {},
        reason=reason,
        error_code=error_code,
    )


def _ctx(flag_key: str = "header-redesign") -> HookContext:
    return HookContext(
        flag_key=flag_key,
        flag_type=FlagType.BOOLEAN,
        default_value=False,
        evaluation_context=EvaluationContext(),
    )


def _record(
    agg: DynatraceMetricsAggregator,
    details: FlagEvaluationDetails,
    *,
    on_record: object = None,
) -> None:
    hook = DynatraceMetricsHook(
        agg,
        clock=lambda: _FIXED_NOW,
        on_record=on_record,  # type: ignore[arg-type]
    )
    hook.finally_after(_ctx(details.flag_key), details, {})


def _single_key(agg: DynatraceMetricsAggregator) -> EvaluationMetricKey:
    buckets = agg.drain().evaluations
    assert len(buckets) == 1
    flag_buckets = next(iter(buckets.values()))
    assert len(flag_buckets) == 1
    return next(iter(flag_buckets))


class TestNormalization:
    def test_targeting_match(self) -> None:
        agg = _aggregator()
        _record(agg, _details(variant="control", reason=Reason.TARGETING_MATCH))
        assert _single_key(agg) == EvaluationMetricKey(
            "control", "targeting_match", None, _BUCKET
        )

    def test_error_evaluation_uses_default_variant_and_error_reason(self) -> None:
        agg = _aggregator()
        _record(
            agg,
            _details(
                variant=None, reason=Reason.ERROR, error_code=ErrorCode.FLAG_NOT_FOUND
            ),
        )
        assert _single_key(agg) == EvaluationMetricKey(
            "__default__", "error", "flag_not_found", _BUCKET
        )

    def test_default_reason_uses_default_variant(self) -> None:
        agg = _aggregator()
        _record(agg, _details(variant="control", reason=Reason.DEFAULT))
        assert _single_key(agg) == EvaluationMetricKey(
            "__default__", "default", None, _BUCKET
        )

    def test_error_overrides_valid_variant_and_reason(self) -> None:
        agg = _aggregator()
        _record(
            agg,
            _details(
                variant="control",
                reason=Reason.TARGETING_MATCH,
                error_code=ErrorCode.TYPE_MISMATCH,
            ),
        )
        assert _single_key(agg) == EvaluationMetricKey(
            "__default__", "error", "type_mismatch", _BUCKET
        )

    def test_string_reason_is_normalized(self) -> None:
        agg = _aggregator()
        _record(agg, _details(variant="control", reason="STATIC"))
        assert _single_key(agg).reason == "static"

    def test_missing_reason_is_unknown(self) -> None:
        agg = _aggregator()
        _record(agg, _details(variant="control", reason=None))
        assert _single_key(agg).reason == "unknown"

    def test_blank_variant_becomes_default(self) -> None:
        agg = _aggregator()
        _record(agg, _details(variant="  ", reason=Reason.STATIC))
        assert _single_key(agg).variant == "__default__"


class TestVersionAndBucketing:
    def test_version_read_from_flag_metadata(self) -> None:
        agg = DynatraceMetricsAggregator()
        agg.set_version("metadata-version")
        _record(
            agg,
            _details(reason=Reason.STATIC, metadata={"version": "metadata-version"}),
        )
        assert agg.size() == 1

    def test_mismatched_metadata_version_dropped(self) -> None:
        agg = DynatraceMetricsAggregator()
        agg.set_version("v1")
        _record(agg, _details(reason=Reason.STATIC, metadata={"version": "v2"}))
        assert agg.size() == 0

    def test_timestamp_bucketing(self) -> None:
        agg = _aggregator()
        _record(agg, _details(reason=Reason.STATIC))
        assert _single_key(agg).timestamp_sec == _BUCKET


class TestCallback:
    def test_on_record_invoked(self) -> None:
        agg = _aggregator()
        calls: list[int] = []
        _record(agg, _details(reason=Reason.STATIC), on_record=lambda: calls.append(1))
        assert calls == [1]

    def test_on_record_failure_does_not_break_recording(self) -> None:
        agg = _aggregator()

        def boom() -> None:
            raise RuntimeError("flush bookkeeping failed")

        _record(agg, _details(reason=Reason.STATIC), on_record=boom)
        # Recording still happened despite the callback raising.
        assert agg.size() == 1

    def test_blank_flag_key_is_ignored(self) -> None:
        agg = _aggregator()
        _record(agg, _details(flag_key="", reason=Reason.STATIC))
        assert agg.size() == 0
