from __future__ import annotations

import enum
import logging
import time
from collections.abc import Callable

from openfeature.exception import ErrorCode
from openfeature.flag_evaluation import FlagEvaluationDetails, FlagValueType, Reason
from openfeature.hook import Hook, HookContext, HookHints

from ._metrics_aggregator import DynatraceMetricsAggregator

log = logging.getLogger(__name__)

DEFAULT_VARIANT = "__default__"
DEFAULT_REASON = Reason.DEFAULT.value.lower()
UNKNOWN_REASON = Reason.UNKNOWN.value.lower()
ERROR_REASON = Reason.ERROR.value.lower()
_VERSION_METADATA_KEY = "version"

_TIMESTAMP_BUCKET_SEC = 10


def _reason_token(reason: str | Reason | None) -> str | None:
    """Returns the canonical (upper-case enum-name style) token for a reason,
    accepting either a Reason enum or a plain string."""
    if reason is None:
        return None
    if isinstance(reason, enum.Enum):
        return str(reason.value)
    return str(reason)


class DynatraceMetricsHook(Hook):
    """Provider-owned ``finally_after`` hook that captures every flag evaluation
    (success and error) into the Dynatrace evaluation-metrics aggregate map."""

    def __init__(
        self,
        aggregator: DynatraceMetricsAggregator,
        *,
        clock: Callable[[], float] = time.time,
        on_record: Callable[[], None] | None = None,
    ) -> None:
        self._aggregator = aggregator
        self._clock = clock
        self._on_record = on_record or (lambda: None)

    def finally_after(
        self,
        hook_context: HookContext,
        details: FlagEvaluationDetails[FlagValueType],
        hints: HookHints,
    ) -> None:
        flag_key = details.flag_key or hook_context.flag_key
        if not flag_key or not flag_key.strip():
            return

        version = self._read_version(details)
        raw_reason = details.reason
        error_code = self._normalize_error_code(details.error_code)
        self._aggregator.record(
            flag_key,
            self._normalize_variant(details.variant, raw_reason, error_code),
            self._normalize_reason(raw_reason, error_code),
            error_code,
            self._current_timestamp_bucket_sec(),
            version,
        )

        try:
            self._on_record()
        except Exception as exc:
            log.warning("metrics hook on_record callback failed: %r", exc)

    def _normalize_variant(
        self, variant: str | None, reason: str | Reason | None, error_code: str | None
    ) -> str:
        if (
            error_code is not None
            or self._is_default_reason(reason)
            or not variant
            or not variant.strip()
        ):
            return DEFAULT_VARIANT
        return variant

    def _normalize_reason(
        self, reason: str | Reason | None, error_code: str | None
    ) -> str:
        if error_code is not None:
            return ERROR_REASON
        token = _reason_token(reason)
        if token is None or not token.strip():
            return UNKNOWN_REASON
        return token.lower()

    def _is_default_reason(self, reason: str | Reason | None) -> bool:
        token = _reason_token(reason)
        return token is not None and token.upper() == Reason.DEFAULT.value

    def _normalize_error_code(self, error_code: ErrorCode | None) -> str | None:
        if error_code is None:
            return None
        if isinstance(error_code, enum.Enum):
            return error_code.name.lower()
        return str(error_code).lower()

    def _read_version(
        self, details: FlagEvaluationDetails[FlagValueType]
    ) -> str | None:
        metadata = details.flag_metadata
        if not metadata:
            return None
        value = metadata.get(_VERSION_METADATA_KEY)
        return value if isinstance(value, str) else None

    def _current_timestamp_bucket_sec(self) -> int:
        epoch_sec = int(self._clock())
        return (epoch_sec // _TIMESTAMP_BUCKET_SEC) * _TIMESTAMP_BUCKET_SEC
