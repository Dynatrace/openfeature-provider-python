from __future__ import annotations

import logging
import math
import threading
import time
from collections.abc import Callable

from ._cdn import FetchResult

log = logging.getLogger(__name__)

# stands in for an SSE announcement that carried no lastModified: greater than any real one, so never dropped
_LAST_MODIFIED_UNKNOWN = math.inf


class PollingManager:
    """Polls on a fixed-rate schedule anchored to request initiation, not completion.

    At most one fetch runs at a time (providers.md 2.1.1): a contended tick is skipped and emits
    nothing; a contended SSE re-fetch is deferred and coalesced. set_interval switches cadence.
    """

    def __init__(
        self,
        fetch_flags: Callable[[threading.Event | None], FetchResult],
        store_flags: Callable[[str], list[str]],
        emit_change: Callable[[list[str]], None],
        emit_success: Callable[[], None],
        emit_failure: Callable[[str], None],
        interval_s: float,
    ) -> None:
        self._fetch_flags = fetch_flags
        self._store_flags = store_flags
        self._emit_change = emit_change
        self._emit_success = emit_success
        self._emit_failure = emit_failure
        self._stop_event = threading.Event()
        self._ticker_thread: threading.Thread | None = None
        # guards everything below, and wakes the ticker when the interval changes or polling stops
        self._cond = threading.Condition()
        self._interval_s = interval_s
        self._rescheduled = False
        # set from submit until the fetch has settled (not until its thread exits), so no trigger defers behind a settled fetch
        self._in_flight = False
        # last-modified value of a deferred SSE re-fetch, or None when none is pending
        self._pending_sse_fetch_last_modified: float | None = None
        # last-modified value of the current (in-flight) SSE-triggered fetch, or None if poll-triggered
        self._in_flight_sse_fetch_last_modified: float | None = None

    @property
    def interval_s(self) -> float:
        with self._cond:
            return self._interval_s

    def start(self) -> None:
        if self._ticker_thread is not None and self._ticker_thread.is_alive():
            return
        self._stop_event.clear()
        self._ticker_thread = threading.Thread(
            target=self._ticker_loop,
            name="dt-provider-ticker",
            daemon=True,
        )
        self._ticker_thread.start()

    def stop(self) -> None:
        # stop event doubles as the fetch cancel signal, so an in-flight poll abandons its backoff
        self._stop_event.set()
        with self._cond:
            self._pending_sse_fetch_last_modified = None
            self._cond.notify_all()
        if self._ticker_thread is not None:
            self._ticker_thread.join(timeout=2.0)

    def set_interval(self, interval_s: float) -> None:
        """Change the poll cadence and re-anchor the schedule to now. No-op if unchanged."""
        with self._cond:
            if interval_s == self._interval_s:
                return
            self._interval_s = interval_s
            self._rescheduled = True
            self._cond.notify_all()

    def trigger_immediate_fetch(
        self, announced_last_modified: float | None = None
    ) -> None:
        """Fetch now if the slot is free, else defer and coalesce; never disturbs the cadence.
        A deferred re-fetch is dropped only if a completed fetch is at least as new as announced."""
        if self._stop_event.is_set():
            return
        announced = (
            _LAST_MODIFIED_UNKNOWN
            if announced_last_modified is None
            else announced_last_modified
        )
        with self._cond:
            if self._in_flight:
                self._pending_sse_fetch_last_modified = (
                    announced
                    if self._pending_sse_fetch_last_modified is None
                    else max(self._pending_sse_fetch_last_modified, announced)
                )
                log.debug(
                    "fetch in flight; deferring SSE re-fetch (lastModified=%s)",
                    announced_last_modified,
                )
                return
            self._submit_locked(announced)

    def _ticker_loop(self) -> None:
        with self._cond:
            next_tick = time.monotonic() + self._interval_s
            while not self._stop_event.is_set():
                if self._rescheduled:
                    # the polling interval changed
                    self._rescheduled = False
                    next_tick = time.monotonic() + self._interval_s
                wait = next_tick - time.monotonic()
                if wait > 0:
                    # woken early by set_interval or stop; the loop re-evaluates either way
                    self._cond.wait(wait)
                    continue
                next_tick += self._interval_s
                now = time.monotonic()
                if next_tick < now:
                    next_tick = now + self._interval_s
                self._on_tick_locked()

    def _on_tick_locked(self) -> None:
        if self._stop_event.is_set():
            return
        if self._in_flight:
            # Neither failed nor succeeded: the tick learned nothing.
            log.debug("poll tick skipped: a fetch is already in flight")
            return
        self._submit_locked(None)

    def _submit_locked(self, sse_refetch_at: float | None) -> None:
        self._in_flight_sse_fetch_last_modified = sse_refetch_at
        self._in_flight = True
        threading.Thread(
            target=self._run_fetches,
            name="dt-provider-poller",
            daemon=True,
        ).start()

    def _run_fetches(self) -> None:
        # a deferred re-fetch runs on this same thread, so it queues behind the fetch rather than overlapping it
        again = True
        while again:
            result: FetchResult | None = None
            try:
                result = self._poll()
            finally:
                with self._cond:
                    again = self._on_fetch_complete_locked(result)

    def _on_fetch_complete_locked(self, completed: FetchResult | None) -> bool:
        """Settle the SSE re-fetch bookkeeping; return True to issue the pending re-fetch now.
        ``completed`` is the finished fetch's result, or None if it raised."""
        issued_for = self._in_flight_sse_fetch_last_modified
        self._in_flight_sse_fetch_last_modified = None
        # frees the slot before anything below can re-claim it
        self._in_flight = False

        if completed is not None and completed.skipped:
            # no request was made, so a re-fetch we issued is still unsatisfied; put it back rather than re-issue it,
            # which would just be skipped again (the Retry-After window or fetch lock still holds) and spin
            if issued_for is not None:
                self._pending_sse_fetch_last_modified = (
                    issued_for
                    if self._pending_sse_fetch_last_modified is None
                    else max(self._pending_sse_fetch_last_modified, issued_for)
                )
                log.debug("SSE re-fetch made no request; keeping it pending")
            return False

        pending = self._pending_sse_fetch_last_modified
        if pending is None or self._stop_event.is_set():
            return False
        self._pending_sse_fetch_last_modified = None

        observed = completed.last_modified_epoch_s if completed is not None else None
        if (
            pending != _LAST_MODIFIED_UNKNOWN
            and observed is not None
            and observed >= pending
        ):
            log.debug(
                "dropping deferred SSE re-fetch: completed fetch already returned "
                "Last-Modified %s >= announced %s",
                observed,
                pending,
            )
            return False
        log.debug("issuing deferred SSE re-fetch")
        self._in_flight_sse_fetch_last_modified = pending
        self._in_flight = True
        return True

    def _poll(self) -> FetchResult | None:
        """Run one fetch and emit its outcome; return its result, or None if the fetch raised."""
        cancel = self._stop_event
        result: FetchResult | None = None
        try:
            result = self._fetch_flags(cancel)

            if cancel.is_set():
                log.debug("provider stopped during fetch; discarding result")
                return result

            self._handle_result(result)

        except Exception as exc:
            if cancel.is_set():
                log.debug("poll abandoned during shutdown")
                return result
            log.warning("poll failed: %s", type(exc).__name__)
            self._emit_failure(type(exc).__name__)
        return result

    def _handle_result(self, result: FetchResult) -> None:
        if result.skipped:
            log.debug("fetch skipped (single-flight); emitting nothing")
            return

        if result.is_not_modified:
            log.debug("flags not modified")
            self._emit_success()
            return

        if result.is_rate_limited:
            log.debug("rate limited; serving cached values")
            return

        if not result.is_success:
            self._emit_failure(f"HTTP {result.status}")
            return

        body = result.body
        if not body:
            self._emit_failure("empty response body")
            return

        changed_keys = self._store_flags(body)
        self._emit_success()

        if changed_keys:
            log.info("flag configuration changed: %s", changed_keys)
            self._emit_change(changed_keys)
