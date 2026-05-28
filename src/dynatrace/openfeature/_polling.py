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
    fetch took. When the ticker fires it cancels any in-flight poll and starts a
    fresh one on a worker thread. Cancelled polls are silent: no emitFailure is
    called.
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
        self._current_cancel: threading.Event | None = None
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
        self._stop_event.set()
        if self._ticker_thread is not None:
            self._ticker_thread.join(timeout=2.0)
        cancel = self._current_cancel
        if cancel is not None:
            cancel.set()

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
        prev = self._current_cancel
        if prev is not None:
            prev.set()
        cancel = threading.Event()
        self._current_cancel = cancel
        threading.Thread(
            target=self._poll,
            args=(cancel,),
            name="dt-provider-poller",
            daemon=True,
        ).start()

    def _poll(self, cancel: threading.Event) -> None:
        try:
            result = self._fetch_flags(cancel)

            if cancel.is_set():
                log.debug("poll cancelled after fetch returned; discarding result")
                return

            if result.skipped:
                log.debug("fetch skipped (lock contention); retaining cached state")
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
                log.debug("poll cancelled")
                return
            log.warning("poll failed: %s", type(exc).__name__)
            self._emit_failure(type(exc).__name__)
