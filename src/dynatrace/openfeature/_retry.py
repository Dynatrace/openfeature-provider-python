from __future__ import annotations

import logging
import threading
import time
import typing
from collections.abc import Callable
from random import Random

log = logging.getLogger(__name__)

T = typing.TypeVar("T")

# A fetch is capped at two attempts so it can never outlive the shortest poll
# interval (providers.md section 2.1): 4s + 0.12s + 4s = 8.12s < 10s. Beyond the
# single retry, recovery is the next poll tick's job.
MAX_ATTEMPTS = 2
BASE_DELAY_MS = 100.0
MAX_JITTER_FACTOR = 0.20

#: Worst-case time spent backing off within one fetch, used to derive the
#: fetch-duration bound in :mod:`._cdn`. There is one backoff between each pair
#: of attempts, so this tracks MAX_ATTEMPTS rather than assuming a single retry.
MAX_BACKOFF_S = (
    (MAX_ATTEMPTS - 1) * BASE_DELAY_MS * (1.0 + MAX_JITTER_FACTOR)
) / 1000.0


class RetryStrategy:
    def __init__(self, rng: Random | None = None) -> None:
        self._rng = rng or Random()  # noqa: S311

    def execute(
        self,
        fn: Callable[[], T],
        should_retry: Callable[[T], bool] = lambda _: False,
        retryable_exceptions: tuple[type[BaseException], ...] = (OSError,),
        cancel: threading.Event | None = None,
    ) -> T:
        last_exception: BaseException | None = None
        last_result: T | None = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                result = fn()
                last_result = result
                if not should_retry(result):
                    return result
                log.warning(
                    "attempt %d/%d returned retryable result; retrying",
                    attempt,
                    MAX_ATTEMPTS,
                )
            except retryable_exceptions as exc:
                last_exception = exc
                log.warning(
                    "attempt %d/%d failed: %s; retrying", attempt, MAX_ATTEMPTS, exc
                )
            except Exception as exc:
                raise OSError("unexpected error during fetch") from exc

            if attempt < MAX_ATTEMPTS:
                self._backoff(cancel)
                if cancel is not None and cancel.is_set():
                    break

        if last_result is not None:
            return last_result
        if last_exception is not None:
            raise last_exception
        raise OSError("all retry attempts failed")

    def _backoff(self, cancel: threading.Event | None) -> None:
        jitter = BASE_DELAY_MS * self._rng.random() * MAX_JITTER_FACTOR
        sleep_ms = BASE_DELAY_MS + jitter
        if cancel is not None:
            cancel.wait(sleep_ms / 1000.0)
        else:
            time.sleep(sleep_ms / 1000.0)


def default_should_retry_status(status: int) -> bool:
    return 500 <= status < 600
