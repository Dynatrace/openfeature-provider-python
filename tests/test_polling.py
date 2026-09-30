import threading
import time

import pytest

from dynatrace.openfeature._cdn import FetchResult
from dynatrace.openfeature._polling import PollingManager

_200 = FetchResult(200, '{"flags":{}}', None, None)
_304 = FetchResult(304, None, None, None)
_429 = FetchResult(429, None, None, None)
_500 = FetchResult(500, None, None, None)
_SKIPPED = FetchResult(0, None, None, None, skipped=True)

_FAST_INTERVAL = 0.05  # 50ms for tests


def _make_pm(
    fetch_result: FetchResult = _304,
    fetch_fn=None,
    changed_keys: list[str] | None = None,
    interval_s: float = _FAST_INTERVAL,
) -> tuple[PollingManager, dict[str, list[object]]]:
    calls: dict[str, list[object]] = {
        "store": [],
        "change": [],
        "success": [],
        "failure": [],
    }
    if fetch_fn is None:
        fetch_fn = lambda cancel=None: fetch_result  # noqa: E731
    pm = PollingManager(
        fetch_flags=fetch_fn,
        store_flags=lambda body: (calls["store"].append(body), changed_keys or [])[1],
        emit_change=lambda keys: calls["change"].append(keys),
        emit_success=lambda: calls["success"].append(True),
        emit_failure=lambda reason: calls["failure"].append(reason),
        interval_s=interval_s,
    )
    return pm, calls


class TestPollingManagerPoll:
    # a 429 and a single-flight skip emit nothing: neither is an outcome the tick observed
    @pytest.mark.parametrize(
        ("result", "changed_keys", "store", "success", "failure", "change"),
        [
            (_304, [], [], [True], [], []),
            (_200, [], ['{"flags":{}}'], [True], [], []),
            (
                _200,
                ["flag-a", "flag-b"],
                ['{"flags":{}}'],
                [True],
                [],
                [["flag-a", "flag-b"]],
            ),
            (_429, [], [], [], [], []),
            (_SKIPPED, [], [], [], [], []),
            (_500, [], [], [], ["HTTP 500"], []),
            (FetchResult(200, "", None, None), [], [], [], ["empty response body"], []),
        ],
        ids=["304", "200", "200-changed", "429", "skipped", "500", "empty-body"],
    )
    def test_poll_dispatches_by_result(
        self, result, changed_keys, store, success, failure, change
    ) -> None:
        pm, calls = _make_pm(result, changed_keys=changed_keys)
        pm._poll()
        assert calls["store"] == store
        assert calls["success"] == success
        assert calls["failure"] == failure
        assert calls["change"] == change

    def test_exception_emits_failure(self) -> None:
        def boom(cancel=None):
            raise RuntimeError("boom")

        pm, calls = _make_pm(fetch_fn=boom)
        pm._poll()  # should not raise
        assert calls["failure"] == ["RuntimeError"]

    def test_result_discarded_after_stop(self) -> None:
        pm, calls = _make_pm(_200)
        pm._stop_event.set()
        pm._poll()
        assert calls["success"] == []
        assert calls["failure"] == []

    def test_exception_during_shutdown_does_not_emit_failure(self) -> None:
        pm, calls = _make_pm()

        def fetch(cancel=None):
            pm._stop_event.set()
            raise OSError("network error")

        pm._fetch_flags = fetch
        pm._poll()
        assert calls["failure"] == []


class TestPollingManagerSingleFlight:
    def test_tick_during_in_flight_fetch_is_skipped(self) -> None:
        # providers.md 2.1.1: the contended tick is skipped outright; the in-flight fetch is neither cancelled nor joined
        first_started = threading.Event()
        release = threading.Event()
        calls: list[str] = []
        emitted: list[str] = []

        def fetch(cancel=None) -> FetchResult:
            calls.append("fetch")
            if len(calls) == 1:
                first_started.set()
                release.wait(2.0)
            return _304

        pm = PollingManager(
            fetch_flags=fetch,
            store_flags=lambda _: [],
            emit_change=lambda _: None,
            emit_success=lambda: emitted.append("success"),
            emit_failure=lambda r: emitted.append(f"failure:{r}"),
            interval_s=_FAST_INTERVAL,
        )
        pm.start()
        assert first_started.wait(timeout=1.0), "first poll should start"
        # Several ticks elapse while the first fetch is blocked.
        time.sleep(_FAST_INTERVAL * 4)
        assert calls == ["fetch"], f"contended ticks must not fetch, got {calls}"
        assert emitted == [], f"skipped ticks must emit nothing, got {emitted}"
        release.set()
        pm.stop()

    def test_polling_resumes_after_slow_fetch_completes(self) -> None:
        first_started = threading.Event()
        second_started = threading.Event()
        release = threading.Event()
        count = [0]

        def fetch(cancel=None) -> FetchResult:
            count[0] += 1
            if count[0] == 1:
                first_started.set()
                release.wait(2.0)
            else:
                second_started.set()
            return _304

        pm = PollingManager(
            fetch_flags=fetch,
            store_flags=lambda _: [],
            emit_change=lambda _: None,
            emit_success=lambda: None,
            emit_failure=lambda _: None,
            interval_s=_FAST_INTERVAL,
        )
        pm.start()
        assert first_started.wait(timeout=1.0)
        release.set()
        assert second_started.wait(timeout=1.0), "next tick should poll normally"
        pm.stop()


class TestPollingManagerThreading:
    def test_starts_and_stops(self) -> None:
        fired = threading.Event()
        pm = PollingManager(
            fetch_flags=lambda cancel=None: (_304, fired.set())[0],
            store_flags=lambda _: [],
            emit_change=lambda _: None,
            emit_success=lambda: None,
            emit_failure=lambda _: None,
            interval_s=_FAST_INTERVAL,
        )
        pm.start()
        assert fired.wait(timeout=2.0), "poller never fired"
        pm.stop()
        assert pm._ticker_thread is not None
        assert not pm._ticker_thread.is_alive()

    def test_polls_multiple_times(self) -> None:
        counter = [0]
        done = threading.Event()

        def fetch(cancel=None) -> FetchResult:
            counter[0] += 1
            if counter[0] >= 3:
                done.set()
            return _304

        pm = PollingManager(
            fetch_flags=fetch,
            store_flags=lambda _: [],
            emit_change=lambda _: None,
            emit_success=lambda: None,
            emit_failure=lambda _: None,
            interval_s=_FAST_INTERVAL,
        )
        pm.start()
        assert done.wait(timeout=2.0), "did not poll 3 times"
        pm.stop()
        assert counter[0] >= 3

    def test_slow_poll_does_not_emit_failure(self) -> None:
        started = threading.Event()
        failures: list[str] = []

        def fetch(cancel=None) -> FetchResult:
            started.set()
            (cancel or threading.Event()).wait(10)
            return _304

        pm = PollingManager(
            fetch_flags=fetch,
            store_flags=lambda _: [],
            emit_change=lambda _: None,
            emit_success=lambda: None,
            emit_failure=lambda r: failures.append(r),
            interval_s=_FAST_INTERVAL,
        )
        pm.start()
        assert started.wait(timeout=1.0)
        time.sleep(_FAST_INTERVAL * 3)
        pm.stop()
        assert failures == [], f"skipped ticks must not emit failure, got: {failures}"

    def test_stop_returns_promptly(self) -> None:
        started = threading.Event()

        def fetch(cancel=None) -> FetchResult:
            started.set()
            (cancel or threading.Event()).wait(10)
            return _304

        pm = PollingManager(
            fetch_flags=fetch,
            store_flags=lambda _: [],
            emit_change=lambda _: None,
            emit_success=lambda: None,
            emit_failure=lambda _: None,
            interval_s=_FAST_INTERVAL,
        )
        pm.start()
        assert started.wait(timeout=1.0)
        t0 = time.monotonic()
        pm.stop()
        assert time.monotonic() - t0 < 0.5, "stop() should return promptly"
