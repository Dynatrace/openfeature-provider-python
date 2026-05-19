from __future__ import annotations

import logging
import time
import typing
from collections.abc import Callable
from random import Random

log = logging.getLogger(__name__)

T = typing.TypeVar("T")

_MAX_ATTEMPTS = 5
_BASE_DELAY_MS = 100.0
_MULTIPLIER = 2.0
_MAX_JITTER_FACTOR = 0.20


class RetryStrategy:
    def __init__(
        self,
        rng: Random | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._rng = rng or Random()  # noqa: S311
        self._sleeper = sleeper or (lambda ms: time.sleep(ms / 1000.0))

    def execute(
        self,
        fn: Callable[[], T],
        should_retry: Callable[[T], bool] = lambda _: False,
        retryable_exceptions: tuple[type[BaseException], ...] = (OSError,),
    ) -> T:
        last_exc: BaseException | None = None
        last_result: T | None = None
        delay_ms = _BASE_DELAY_MS

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                result = fn()
                last_result = result
                if not should_retry(result):
                    return result
                log.warning("attempt %d/%d returned retryable result; retrying",
                            attempt, _MAX_ATTEMPTS)
            except retryable_exceptions as exc:
                last_exc = exc
                log.warning("attempt %d/%d failed: %s; retrying",
                            attempt, _MAX_ATTEMPTS, exc)
            except Exception as exc:
                raise OSError("unexpected error during fetch") from exc

            if attempt < _MAX_ATTEMPTS:
                jitter = delay_ms * self._rng.random() * _MAX_JITTER_FACTOR
                self._sleeper(delay_ms + jitter)
                delay_ms *= _MULTIPLIER

        if last_result is not None:
            return last_result
        if last_exc is not None:
            raise last_exc
        raise OSError("all retry attempts failed")


def default_should_retry_status(status: int) -> bool:
    return 500 <= status < 600
