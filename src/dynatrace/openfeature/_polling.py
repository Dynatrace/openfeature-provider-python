from __future__ import annotations

import logging
import threading
from collections.abc import Callable

from ._cdn import FetchResult

log = logging.getLogger(__name__)


class PollingManager:
    def __init__(
        self,
        fetch_flags: Callable[[], FetchResult],
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
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._poll_loop,
            name='dt-provider-poller',
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval_s + 2.0)

    def _poll_loop(self) -> None:
        # Fixed-delay: wait first, then poll (mirrors Java scheduleWithFixedDelay initial delay).
        # Event.wait() returns True if stop was requested, False on timeout.
        while not self._stop_event.wait(self._interval_s):
            self._poll()

    def _poll(self) -> None:
        try:
            result = self._fetch_flags()

            if result.is_not_modified:
                log.debug('flags not modified')
                self._emit_success()
                return

            if result.is_rate_limited:
                log.debug('rate limited; serving cached values')
                return

            if not result.is_success:
                self._emit_failure(f'HTTP {result.status}')
                return

            body = result.body
            if not body:
                self._emit_failure('empty response body')
                return

            changed_keys = self._store_flags(body)
            self._emit_success()

            if changed_keys:
                log.info('flag configuration changed: %s', changed_keys)
                self._emit_change(changed_keys)
        except Exception as exc:
            log.warning('poll failed', exc_info=exc)
            self._emit_failure(str(exc))
