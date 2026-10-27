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


_LONG_INTERVAL = 600.0
_LM_HEADER = "Tue, 02 Jan 2024 00:00:00 GMT"
# _LM_HEADER as epoch seconds
_LM_EPOCH = 1704153600


def _wait_until(condition, timeout: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


class _GatedFetch:
    """Fetch stub whose first call blocks until released; later calls walk ``results``."""

    def __init__(self, results: list[FetchResult]) -> None:
        self._results = results
        self.calls = 0
        self.first_started = threading.Event()
        self.release = threading.Event()
        self._lock = threading.Lock()

    def __call__(self, cancel: threading.Event | None = None) -> FetchResult:
        with self._lock:
            self.calls += 1
            n = self.calls
        if n == 1:
            self.first_started.set()
            self.release.wait(2.0)
        return self._results[min(n, len(self._results)) - 1]


class TestPollingManagerSetInterval:
    def test_set_interval_re_anchors_the_ticker(self) -> None:
        fired = threading.Event()
        pm, _ = _make_pm(
            fetch_fn=lambda cancel=None: (_304, fired.set())[0],
            interval_s=_LONG_INTERVAL,
        )
        pm.start()
        try:
            pm.set_interval(_FAST_INTERVAL)
            # the new cadence counts from now, not from the old 10-minute tick
            assert fired.wait(timeout=1.0), "ticker did not adopt the new interval"
            assert pm.interval_s == _FAST_INTERVAL
        finally:
            pm.stop()

    def test_slowing_down_postpones_the_next_tick(self) -> None:
        calls: list[int] = []
        pm, _ = _make_pm(
            fetch_fn=lambda cancel=None: (_304, calls.append(1))[0],
            interval_s=_FAST_INTERVAL,
        )
        pm.start()
        try:
            pm.set_interval(_LONG_INTERVAL)
            time.sleep(_FAST_INTERVAL * 4)
            assert calls == []
        finally:
            pm.stop()

    def test_unchanged_interval_does_not_re_anchor(self) -> None:
        pm, _ = _make_pm(interval_s=_LONG_INTERVAL)
        pm.set_interval(_LONG_INTERVAL)
        assert pm._rescheduled is False


class TestPollingManagerSseRefetch:
    def test_free_slot_fetches_immediately(self) -> None:
        fetch = _GatedFetch([_304])
        fetch.release.set()
        pm, calls = _make_pm(fetch_fn=fetch, interval_s=_LONG_INTERVAL)
        pm.start()
        try:
            pm.trigger_immediate_fetch(_LM_EPOCH)
            assert _wait_until(lambda: calls["success"] == [True])
            assert fetch.calls == 1
        finally:
            pm.stop()

    def test_contended_refetches_are_deferred_and_coalesced(self) -> None:
        fetch = _GatedFetch([_304])
        pm, _ = _make_pm(fetch_fn=fetch, interval_s=_LONG_INTERVAL)
        pm.start()
        try:
            pm.trigger_immediate_fetch(None)
            assert fetch.first_started.wait(1.0)
            pm.trigger_immediate_fetch(_LM_EPOCH)
            pm.trigger_immediate_fetch(_LM_EPOCH + 60)
            assert fetch.calls == 1, "a contended re-fetch must not overlap"
            fetch.release.set()
            assert _wait_until(lambda: fetch.calls == 2)
            time.sleep(0.1)
            assert fetch.calls == 2, "deferred re-fetches must coalesce into one"
        finally:
            pm.stop()

    def test_deferred_refetch_dropped_when_completed_fetch_is_as_new(self) -> None:
        fresh = FetchResult(200, '{"flags":{}}', None, _LM_HEADER)
        fetch = _GatedFetch([fresh, _304])
        pm, _ = _make_pm(fetch_fn=fetch, interval_s=_LONG_INTERVAL)
        pm.start()
        try:
            pm.trigger_immediate_fetch(None)
            assert fetch.first_started.wait(1.0)
            pm.trigger_immediate_fetch(_LM_EPOCH)
            fetch.release.set()
            time.sleep(0.2)
            assert fetch.calls == 1
        finally:
            pm.stop()

    @pytest.mark.parametrize(
        ("completed_last_modified", "announced"),
        [
            ("Mon, 01 Jan 2024 00:00:00 GMT", _LM_EPOCH),
            (None, _LM_EPOCH),
            # an announcement without lastModified is never dropped
            (_LM_HEADER, None),
        ],
        ids=["completed-older", "completed-unknown", "announced-unknown"],
    )
    def test_deferred_refetch_issued_unless_proven_satisfied(
        self, completed_last_modified: str | None, announced: int | None
    ) -> None:
        completed = FetchResult(200, '{"flags":{}}', None, completed_last_modified)
        fetch = _GatedFetch([completed, _304])
        pm, _ = _make_pm(fetch_fn=fetch, interval_s=_LONG_INTERVAL)
        pm.start()
        try:
            pm.trigger_immediate_fetch(None)
            assert fetch.first_started.wait(1.0)
            pm.trigger_immediate_fetch(announced)
            fetch.release.set()
            assert _wait_until(lambda: fetch.calls == 2)
        finally:
            pm.stop()

    def test_deferred_refetch_issued_after_a_failed_fetch(self) -> None:
        started = threading.Event()
        gate = threading.Event()
        count = [0]

        def fetch(cancel=None) -> FetchResult:
            count[0] += 1
            if count[0] == 1:
                started.set()
                gate.wait(2.0)
                raise OSError("network")
            return _304

        pm, calls = _make_pm(fetch_fn=fetch, interval_s=_LONG_INTERVAL)
        pm.start()
        try:
            pm.trigger_immediate_fetch(None)
            assert started.wait(1.0)
            pm.trigger_immediate_fetch(_LM_EPOCH)
            gate.set()
            assert _wait_until(lambda: calls["success"] == [True])
            assert calls["failure"] == ["OSError"]
        finally:
            pm.stop()

    def test_skipped_refetch_stays_pending_until_a_fetch_reaches_the_cdn(
        self,
    ) -> None:
        results = iter([_SKIPPED, _304, _304])
        seen: list[FetchResult] = []

        def fetch(cancel=None) -> FetchResult:
            r = next(results)
            seen.append(r)
            return r

        pm, _ = _make_pm(fetch_fn=fetch, interval_s=_LONG_INTERVAL)
        pm.start()
        try:
            pm.trigger_immediate_fetch(_LM_EPOCH)
            assert _wait_until(lambda: len(seen) == 1)
            time.sleep(0.1)
            # re-issuing a skipped re-fetch at once would only be skipped again and spin
            assert len(seen) == 1
            assert pm._pending_sse_fetch_last_modified == _LM_EPOCH
            # the next fetch that makes a request issues the kept one after it
            pm.trigger_immediate_fetch(None)
            assert _wait_until(lambda: len(seen) == 3)
        finally:
            pm.stop()

    def test_contended_tick_neither_fetches_nor_emits(self) -> None:
        fetch = _GatedFetch([_304])
        pm, calls = _make_pm(fetch_fn=fetch, interval_s=_FAST_INTERVAL)
        pm.start()
        try:
            assert fetch.first_started.wait(1.0)
            pm.trigger_immediate_fetch(_LM_EPOCH)
            # ticks keep firing while the first fetch is blocked
            time.sleep(_FAST_INTERVAL * 4)
            assert fetch.calls == 1
            assert calls["success"] == []
        finally:
            fetch.release.set()
            pm.stop()

    def test_trigger_after_stop_is_ignored(self) -> None:
        fetch = _GatedFetch([_304])
        fetch.release.set()
        pm, _ = _make_pm(fetch_fn=fetch, interval_s=_LONG_INTERVAL)
        pm.start()
        pm.stop()
        pm.trigger_immediate_fetch(_LM_EPOCH)
        time.sleep(0.1)
        assert fetch.calls == 0
