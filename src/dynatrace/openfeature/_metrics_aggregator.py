from __future__ import annotations

import logging
import threading
import typing

log = logging.getLogger(__name__)

# Soft/hard caps on distinct flag keys. Faithful to the Java provider, these are
# fixed constants and are NOT user-configurable.
SOFT_CAP_DISTINCT_FLAGS = 1_000
HARD_CAP_DISTINCT_FLAGS = 2_000


class EvaluationMetricKey(typing.NamedTuple):
    """Typed dimension key for one aggregation bucket.

    Using a tuple (rather than a delimiter-joined string) gives value-based
    equality/hashing for free and avoids delimiter-collision ambiguity.
    """

    variant: str
    reason: str
    error_code: str | None
    timestamp_sec: int


# Two-level live map: {flag_key: {dimension_key: count}}.
_FlagBuckets = dict[EvaluationMetricKey, int]
_AggregateMap = dict[str, _FlagBuckets]


class DrainedBatch:
    """An immutable snapshot drained from the aggregator, tagged with the config
    version that was active when it was drained."""

    __slots__ = ("_evaluations", "_version")

    def __init__(self, evaluations: _AggregateMap, version: str | None) -> None:
        self._evaluations = evaluations
        self._version = version

    @property
    def is_empty(self) -> bool:
        return not self._evaluations

    @property
    def evaluations(self) -> _AggregateMap:
        return self._evaluations

    @property
    def version(self) -> str | None:
        return self._version


class DynatraceMetricsAggregator:
    """In-memory aggregate map backing the evaluation-metrics hook.

    All public methods are guarded by a single lock so a record can never land in
    a snapshot after that snapshot has been drained.
    """

    def __init__(self, max_distinct_flags: int = HARD_CAP_DISTINCT_FLAGS) -> None:
        if max_distinct_flags <= 0:
            raise ValueError("max_distinct_flags must be positive")
        self._metrics: _AggregateMap = {}
        self._max_distinct_flags = max_distinct_flags
        self._version: str | None = None
        self._lock = threading.RLock()

    def record(
        self,
        flag_key: str,
        variant: str,
        reason: str,
        error_code: str | None,
        timestamp_sec: int,
        evaluation_version: str | None = None,
    ) -> None:
        with self._lock:
            if self._version is None:
                log.debug(
                    "dropping evaluation for %r: no config version observed yet",
                    flag_key,
                )
                return
            if evaluation_version is not None and evaluation_version != self._version:
                # Guards against cross-version contamination within a batch.
                return
            buckets = self._metrics.get(flag_key)
            if buckets is None:
                if len(self._metrics) >= self._max_distinct_flags:
                    log.debug("dropping metrics due to max aggregator size")
                    return
                buckets = self._metrics[flag_key] = {}
            key = EvaluationMetricKey(variant, reason, error_code, timestamp_sec)
            buckets[key] = buckets.get(key, 0) + 1

    def set_version(self, version: str | None) -> None:
        """Records the latest config version. Version transitions are the
        provider's responsibility; record() never mutates the version."""
        with self._lock:
            if version is not None and version.strip():
                self._version = version

    def get_version(self) -> str | None:
        with self._lock:
            return self._version

    def size(self) -> int:
        """Number of distinct flag keys currently in the live map."""
        with self._lock:
            return len(self._metrics)

    def drain(self) -> DrainedBatch:
        """Swaps the live map for a fresh one and returns the previous contents.
        The version is preserved on the batch (it remains valid for the drained
        data); the aggregator keeps its current version."""
        with self._lock:
            previous = self._metrics
            self._metrics = {}
            return DrainedBatch(previous, self._version)
