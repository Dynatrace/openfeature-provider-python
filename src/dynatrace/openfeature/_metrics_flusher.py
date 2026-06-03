from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from collections.abc import Callable

import requests

from ._metrics_aggregator import DrainedBatch, DynatraceMetricsAggregator, _AggregateMap

log = logging.getLogger(__name__)

_SDK_TYPE = "server"
_HTTP_TIMEOUT_S = 10.0
# Cooldown applied to out-of-band (capacity) flushes after a failed delivery, to
# avoid hammering a struggling endpoint between periodic flush ticks.
_FAILED_CAPACITY_RETRY_COOLDOWN_S = 30.0
# Maximum number of sealed batches held in the queue at any time.
_MAX_SEALED_BATCHES = 10

_THREAD_NAME = "dt-provider-metrics-flusher"


def build_payload(
    evaluations: _AggregateMap,
    version: str | None,
    sdk_version: str,
    provider_name: str,
) -> str:
    """Serializes the documented Dynatrace evaluation-metrics payload."""
    evals: list[dict[str, object]] = []
    for flag_key, buckets in evaluations.items():
        for key, count in buckets.items():
            entry: dict[str, object] = {
                "flagKey": flag_key,
                "variant": key.variant,
                "evaluationReason": key.reason,
            }
            if key.error_code is not None:
                entry["errorCode"] = key.error_code
            entry["timestampSec"] = key.timestamp_sec
            entry["count"] = count
            evals.append(entry)
    root = {
        "version": version,
        "sdkType": _SDK_TYPE,
        "sdkVersion": sdk_version,
        "providerName": provider_name,
        "evaluations": evals,
    }
    return json.dumps(root)


class EvaluationMetricsFlusher:
    """Drains the aggregator, serializes the payload, and POSTs it to the metrics
    ingest endpoint.

    All HTTP work happens on a single dedicated daemon thread, never on the
    evaluation path. Flush triggers: periodic (the worker's wait timeout),
    capacity (:meth:`on_record`), version-rotation (:meth:`trigger_flush` after
    :meth:`enqueue_sealed_batch`), and shutdown (final synchronous flush).
    """

    def __init__(  # noqa: PLR0913 - all keyword-only config + injectable test seams
        self,
        aggregator: DynatraceMetricsAggregator,
        *,
        sdk_key: str,
        metrics_url: str,
        flush_interval_s: float,
        capacity_threshold: int,
        sdk_version: str,
        provider_name: str,
        session: requests.Session | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._aggregator = aggregator
        self._metrics_url = metrics_url
        self._authorization_header = f"Bearer {sdk_key}"
        self._flush_interval_s = flush_interval_s
        self._capacity_threshold = capacity_threshold
        self._sdk_version = sdk_version
        self._provider_name = provider_name
        self._session = session or requests.Session()
        # Only close the session we created ourselves; never an injected one.
        self._owns_session = session is None
        self._clock = clock

        self._started = False
        self._shutdown = False
        self._next_out_of_band_flush_allowed = 0.0
        self._state_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None

        self._queue_lock = threading.Lock()
        self._sealed: deque[DrainedBatch] = deque()

    def start(self) -> None:
        """Starts periodic flushing. Idempotent."""
        with self._state_lock:
            if self._started or self._shutdown:
                return
            self._started = True
            self._thread = threading.Thread(
                target=self._run, name=_THREAD_NAME, daemon=True
            )
            self._thread.start()
        log.debug(
            "evaluation metrics flusher started (interval=%ss, url=%s)",
            self._flush_interval_s,
            self._metrics_url,
        )

    def enqueue_sealed_batch(self, batch: DrainedBatch | None) -> None:
        """Accepts a pre-drained batch for delivery ahead of the next periodic
        tick, preserving per-version ordering. No-op for empty batches."""
        if batch is None or batch.is_empty:
            return
        with self._queue_lock:
            if len(self._sealed) >= _MAX_SEALED_BATCHES:
                log.warning(
                    "sealed batch queue full (%d batches); dropping incoming batch "
                    "for version %s to prevent unbounded growth",
                    _MAX_SEALED_BATCHES,
                    batch.version,
                )
                return
            self._sealed.append(batch)

    def trigger_flush(self) -> None:
        """Wakes the worker so a just-enqueued sealed batch is picked up promptly."""
        if self._shutdown:
            return
        self._wake.set()

    def on_record(self) -> None:
        """Capacity callback invoked by the hook after each evaluation. Never
        blocks the calling thread."""
        if not self._started or self._shutdown:
            return
        if self._aggregator.size() < self._capacity_threshold:
            return
        if self._clock() < self._next_out_of_band_flush_allowed:
            return
        self._wake.set()

    def shutdown(self) -> None:
        """Stops the worker and performs a final synchronous flush. Idempotent."""
        with self._state_lock:
            if self._shutdown:
                return
            self._shutdown = True
            thread = self._thread
        self._stop_event.set()
        self._wake.set()
        if thread is not None:
            thread.join(timeout=_HTTP_TIMEOUT_S + 2.0)
        # Final synchronous flush of anything still queued/accumulated.
        self._flush_quietly()
        # Release the connection pool we own once no more requests will be made.
        if self._owns_session:
            try:
                self._session.close()
            except Exception as exc:
                log.debug("metrics flusher session close failed: %r", exc)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._wake.wait(self._flush_interval_s)
            self._wake.clear()
            if self._stop_event.is_set():
                break
            self._flush_quietly()

    def _flush_quietly(self) -> None:
        try:
            self._flush()
        except Exception as exc:
            log.warning("evaluation metrics flush failed: %r", exc)

    def _flush(self) -> None:
        # Flush sealed version-rotation batches first to preserve per-version
        # ordering. A delivery failure requeues at the head and stops the cycle.
        while True:
            with self._queue_lock:
                sealed = self._sealed.popleft() if self._sealed else None
            if sealed is None:
                break
            delivered = False
            try:
                delivered = self._flush_batch(sealed)
            finally:
                if not delivered:
                    with self._queue_lock:
                        self._sealed.appendleft(sealed)
            if not delivered:
                return

        # Then flush whatever has accumulated in the live aggregator.
        live = self._aggregator.drain()
        if live.is_empty:
            return
        delivered = False
        try:
            delivered = self._flush_batch(live)
        finally:
            if not delivered:
                with self._queue_lock:
                    self._sealed.appendleft(live)

    def _flush_batch(self, batch: DrainedBatch) -> bool:
        """POSTs ``batch``. Returns True if delivered (or non-retryable and
        dropped); False if the caller should requeue it."""
        body = build_payload(
            batch.evaluations, batch.version, self._sdk_version, self._provider_name
        )
        try:
            response = self._session.post(
                self._metrics_url,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": self._authorization_header,
                },
                timeout=_HTTP_TIMEOUT_S,
            )
        except requests.RequestException as exc:
            log.warning("evaluation metrics flush HTTP error: %r", exc)
            self._apply_failure_cooldown()
            return False

        status = response.status_code
        if 200 <= status < 300:
            self._next_out_of_band_flush_allowed = 0.0
            log.debug(
                "evaluation metrics flush ok (status=%d, payload bytes=%d)",
                status,
                len(body),
            )
            return True
        if status == 400:
            log.warning(
                "evaluation metrics payload rejected with HTTP 400; dropping "
                "non-retryable batch (payload bytes=%d)",
                len(body),
            )
            return True
        log.warning(
            "evaluation metrics flush returned HTTP %d; payload bytes=%d",
            status,
            len(body),
        )
        self._apply_failure_cooldown()
        return False

    def _apply_failure_cooldown(self) -> None:
        self._next_out_of_band_flush_allowed = (
            self._clock() + _FAILED_CAPACITY_RETRY_COOLDOWN_S
        )
