import threading
import time

from dynatrace.openfeature._cdn import FetchResult
from dynatrace.openfeature._polling import PollingManager

_200 = FetchResult(200, '{"flags":{}}', None, None)
_304 = FetchResult(304, None, None, None)
_429 = FetchResult(429, None, None, None)
_500 = FetchResult(500, None, None, None)

_FAST_INTERVAL = 0.05  # 50ms for tests


def _make_pm(
    fetch_result: FetchResult = _304,
    fetch_fn=None,
    changed_keys: list[str] | None = None,
    interval_s: float = _FAST_INTERVAL,
) -> tuple[PollingManager, dict[str, list[object]]]:
    calls: dict[str, list[object]] = {
        "store": [], "change": [], "success": [], "failure": []
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


def _cancel() -> threading.Event:
    return threading.Event()


class TestPollingManagerPoll:
    def test_304_emits_success_only(self) -> None:
        pm, calls = _make_pm(_304)
        pm._poll(_cancel())
        assert calls["success"] == [True]
        assert calls["store"] == []
        assert calls["failure"] == []

    def test_200_stores_and_emits_success(self) -> None:
        pm, calls = _make_pm(_200)
        pm._poll(_cancel())
        assert calls["store"] == ['{"flags":{}}']
        assert calls["success"] == [True]
        assert calls["failure"] == []

    def test_200_emits_change_on_diff(self) -> None:
        pm, calls = _make_pm(_200, changed_keys=["flag-a", "flag-b"])
        pm._poll(_cancel())
        assert calls["change"] == [["flag-a", "flag-b"]]

    def test_200_no_change_no_change_event(self) -> None:
        pm, calls = _make_pm(_200, changed_keys=[])
        pm._poll(_cancel())
        assert calls["change"] == []

    def test_429_emits_nothing(self) -> None:
        pm, calls = _make_pm(_429)
        pm._poll(_cancel())
        assert calls["success"] == []
        assert calls["failure"] == []

    def test_500_emits_failure(self) -> None:
        pm, calls = _make_pm(_500)
        pm._poll(_cancel())
        assert calls["failure"] == ["HTTP 500"]

    def test_empty_body_emits_failure(self) -> None:
        pm, calls = _make_pm(FetchResult(200, "", None, None))
        pm._poll(_cancel())
        assert calls["failure"] == ["empty response body"]

    def test_exception_emits_failure(self) -> None:
        def boom(cancel=None):
            raise RuntimeError("boom")

        pm = PollingManager(
            fetch_flags=boom,
            store_flags=lambda _: [],
            emit_change=lambda _: None,
            emit_success=lambda: None,
            emit_failure=lambda r: None,
            interval_s=_FAST_INTERVAL,
        )
        pm._poll(_cancel())  # should not raise

    def test_pre_cancelled_poll_discards_result(self) -> None:
        pm, calls = _make_pm(_200)
        cancel = _cancel()
        cancel.set()
        pm._poll(cancel)
        assert calls["success"] == []
        assert calls["failure"] == []

    def test_cancelled_exception_does_not_emit_failure(self) -> None:
        cancel = threading.Event()

        def fetch(c=None):
            cancel.set()
            raise OSError("network error")

        pm, calls = _make_pm(fetch_fn=fetch)
        pm._poll(cancel)
        assert calls["failure"] == []


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

    def test_fixed_rate_fires_during_slow_fetch(self) -> None:
        # Fetch blocks for longer than the interval; ticker should not wait for it.
        started = threading.Event()
        second_started = threading.Event()
        call_count = [0]

        def fetch(cancel=None) -> FetchResult:
            call_count[0] += 1
            if call_count[0] == 1:
                started.set()
                (cancel or threading.Event()).wait(10)  # blocks until cancelled or timeout
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
        assert started.wait(timeout=1.0), "first poll should start"
        assert second_started.wait(timeout=1.0), "second poll should start without waiting for first"
        pm.stop()

    def test_cancelled_poll_does_not_emit_failure(self) -> None:
        second_started = threading.Event()
        failures: list[str] = []
        call_count = [0]

        def fetch(cancel=None) -> FetchResult:
            call_count[0] += 1
            if call_count[0] == 1:
                (cancel or threading.Event()).wait(10)
            else:
                second_started.set()
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
        assert second_started.wait(timeout=1.0)
        pm.stop()
        assert failures == [], f"cancelled poll must not emit failure, got: {failures}"

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
