from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from ._cdn import FetchResult

log = logging.getLogger(__name__)


class PollingManager:
    """
    Polls on a fixed-rate schedule anchored to request initiation, not completion.

    A ticker thread fires every interval_s regardless of how long the previous
    fetch took. At most one fetch is in flight at any time (providers.md section
    2.1.1): a tick that fires while a poll is still running is skipped outright.
    A skipped tick has no observable effect -- it emits neither success nor
    failure, so it does not reset the staleness grace timer. The in-flight fetch
    is left alone; it is already doing what the tick wanted.

    Under the section 2.1 bound (max fetch duration < shortest poll interval) a
    skipped tick should not occur at all. It is handled rather than assumed away
    because the bound is a design invariant, not something the runtime enforces.
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
        self._interval_s = interval_s
        self._stop_event = threading.Event()
        self._in_flight = False
        self._in_flight_lock = threading.Lock()
        self._ticker_thread: threading.Thread | None = None

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
        # The stop event doubles as the cancel signal handed to the fetch, so an
        # in-flight poll abandons its backoff instead of holding shutdown up.
        self._stop_event.set()
        with self._in_flight_lock:
            # A stopped poller is not reused today -- the provider builds a
            # fresh one -- but leaving the slot claimed would silently skip
            # every tick if one ever were.
            self._in_flight = False
        if self._ticker_thread is not None:
            self._ticker_thread.join(timeout=2.0)

    def _ticker_loop(self) -> None:
        next_tick = time.monotonic() + self._interval_s
        while not self._stop_event.is_set():
            wait = max(0.0, next_tick - time.monotonic())
            if self._stop_event.wait(wait):
                break
            next_tick += self._interval_s
            now = time.monotonic()
            if next_tick < now:
                next_tick = now + self._interval_s
            self._dispatch()

    def _dispatch(self) -> None:
        if self._stop_event.is_set():
            return
        with self._in_flight_lock:
            if self._in_flight:
                # Skipped, not failed and not succeeded: the tick learned
                # nothing, so it must not be reported as an outcome.
                log.debug("poll tick skipped: a fetch is already in flight")
                return
            self._in_flight = True
        threading.Thread(
            target=self._run_poll,
            name="dt-provider-poller",
            daemon=True,
        ).start()

    def _run_poll(self) -> None:
        try:
            self._poll()
        finally:
            with self._in_flight_lock:
                self._in_flight = False

    def _poll(self) -> None:
        cancel = self._stop_event
        try:
            result = self._fetch_flags(cancel)

            if cancel.is_set():
                log.debug("provider stopped during fetch; discarding result")
                return

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

        except Exception as exc:
            if cancel.is_set():
                log.debug("poll abandoned during shutdown")
                return
            log.warning("poll failed: %s", type(exc).__name__)
            self._emit_failure(type(exc).__name__)
