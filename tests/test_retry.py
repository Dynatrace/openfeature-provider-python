from random import Random
from unittest.mock import patch

import pytest

from dynatrace.openfeature._retry import (
    _MAX_ATTEMPTS,
    RetryStrategy,
    default_should_retry_status,
)

_SLEEP = "dynatrace.openfeature._retry.time.sleep"


def _seeded_retry() -> RetryStrategy:
    return RetryStrategy(rng=Random(42))  # noqa: S311


class TestDefaultShouldRetryStatus:
    def test_5xx_retryable(self) -> None:
        for s in (500, 502, 503, 599):
            assert default_should_retry_status(s)

    def test_non_5xx_not_retryable(self) -> None:
        for s in (200, 304, 400, 404, 429):
            assert not default_should_retry_status(s)


class TestRetryStrategyExecute:
    def test_success_on_first_attempt(self) -> None:
        calls = 0

        def fn() -> int:
            nonlocal calls
            calls += 1
            return 200

        result = _seeded_retry().execute(fn, should_retry=default_should_retry_status)
        assert result == 200
        assert calls == 1

    @patch(_SLEEP)
    def test_retries_on_5xx_then_succeeds(self, _mock_sleep: object) -> None:
        responses = [500, 500, 200]
        idx = 0

        def fn() -> int:
            nonlocal idx
            v = responses[idx]
            idx += 1
            return v

        result = _seeded_retry().execute(fn, should_retry=default_should_retry_status)
        assert result == 200
        assert idx == 3

    @patch(_SLEEP)
    def test_exhausts_attempts_returns_last_result(self, _mock_sleep: object) -> None:
        def fn() -> int:
            return 503

        result = _seeded_retry().execute(fn, should_retry=default_should_retry_status)
        assert result == 503

    @patch(_SLEEP)
    def test_retries_on_exception_then_succeeds(self, _mock_sleep: object) -> None:
        calls = 0

        def fn() -> int:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise OSError("network error")
            return 200

        result = _seeded_retry().execute(
            fn,
            should_retry=default_should_retry_status,
            retryable_exceptions=(OSError,),
        )
        assert result == 200
        assert calls == 3

    @patch(_SLEEP)
    def test_exhausts_all_attempts_raises_last_exception(
        self, _mock_sleep: object
    ) -> None:
        def fn() -> int:
            raise OSError("always fails")

        with pytest.raises(OSError, match="always fails"):
            _seeded_retry().execute(
                fn,
                should_retry=default_should_retry_status,
                retryable_exceptions=(OSError,),
            )

    def test_non_retryable_exception_wrapped_as_os_error(self) -> None:
        calls = 0

        def fn() -> int:
            nonlocal calls
            calls += 1
            raise ValueError("not retryable")

        with pytest.raises(OSError, match="unexpected error"):
            _seeded_retry().execute(
                fn,
                retryable_exceptions=(OSError,),
            )
        assert calls == 1

    def test_non_retryable_status_returns_immediately(self) -> None:
        calls = 0

        def fn() -> int:
            nonlocal calls
            calls += 1
            return 429

        result = _seeded_retry().execute(fn, should_retry=default_should_retry_status)
        assert result == 429
        assert calls == 1

    @patch(_SLEEP)
    def test_sleep_called_between_attempts(self, mock_sleep: object) -> None:
        attempts = 0

        def fn() -> int:
            nonlocal attempts
            attempts += 1
            return 500

        RetryStrategy(rng=Random(0)).execute(  # noqa: S311
            fn, should_retry=default_should_retry_status
        )
        assert mock_sleep.call_count == _MAX_ATTEMPTS - 1  # type: ignore[attr-defined]
        sleep_args = [call.args[0] for call in mock_sleep.call_args_list]  # type: ignore[attr-defined]
        # delays are in seconds; first is ~100ms base + jitter
        assert sleep_args[0] >= 0.1
        assert sleep_args[1] > sleep_args[0]
