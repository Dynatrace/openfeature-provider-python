import threading

from dynatrace.openfeature._cdn import FetchResult
from dynatrace.openfeature._polling import PollingManager

_200 = FetchResult(200, '{"flags":{}}', None, None)
_304 = FetchResult(304, None, None, None)
_429 = FetchResult(429, None, None, None)
_500 = FetchResult(500, None, None, None)

_FAST_INTERVAL = 0.05  # 50ms for tests


def _make_pm(
    fetch_result: FetchResult = _304,
    changed_keys: list[str] | None = None,
) -> tuple[PollingManager, dict[str, list[object]]]:
    calls: dict[str, list[object]] = {
        'store': [], 'change': [], 'success': [], 'failure': []
    }
    pm = PollingManager(
        fetch_flags=lambda: fetch_result,
        store_flags=lambda body: (calls['store'].append(body), changed_keys or [])[1],
        emit_change=lambda keys: calls['change'].append(keys),
        emit_success=lambda: calls['success'].append(True),
        emit_failure=lambda reason: calls['failure'].append(reason),
        interval_s=_FAST_INTERVAL,
    )
    return pm, calls


class TestPollingManagerPoll:
    def test_304_emits_success_only(self) -> None:
        pm, calls = _make_pm(_304)
        pm._poll()
        assert calls['success'] == [True]
        assert calls['store'] == []
        assert calls['failure'] == []

    def test_200_stores_and_emits_success(self) -> None:
        pm, calls = _make_pm(_200)
        pm._poll()
        assert calls['store'] == ['{"flags":{}}']
        assert calls['success'] == [True]
        assert calls['failure'] == []

    def test_200_emits_change_on_diff(self) -> None:
        pm, calls = _make_pm(_200, changed_keys=['flag-a', 'flag-b'])
        pm._poll()
        assert calls['change'] == [['flag-a', 'flag-b']]

    def test_200_no_change_no_change_event(self) -> None:
        pm, calls = _make_pm(_200, changed_keys=[])
        pm._poll()
        assert calls['change'] == []

    def test_429_emits_nothing(self) -> None:
        pm, calls = _make_pm(_429)
        pm._poll()
        assert calls['success'] == []
        assert calls['failure'] == []

    def test_500_emits_failure(self) -> None:
        pm, calls = _make_pm(_500)
        pm._poll()
        assert calls['failure'] == ['HTTP 500']

    def test_empty_body_emits_failure(self) -> None:
        pm, calls = _make_pm(FetchResult(200, '', None, None))
        pm._poll()
        assert calls['failure'] == ['empty response body']

    def test_exception_emits_failure(self) -> None:
        pm = PollingManager(
            fetch_flags=lambda: (_ for _ in ()).throw(RuntimeError('boom')),
            store_flags=lambda _: [],
            emit_change=lambda _: None,
            emit_success=lambda: None,
            emit_failure=lambda r: None,
            interval_s=_FAST_INTERVAL,
        )
        # should not raise
        pm._poll()


class TestPollingManagerThreading:
    def test_starts_and_stops(self) -> None:
        success = threading.Event()
        pm = PollingManager(
            fetch_flags=lambda: (_304, success.set())[0],
            store_flags=lambda _: [],
            emit_change=lambda _: None,
            emit_success=lambda: None,
            emit_failure=lambda _: None,
            interval_s=_FAST_INTERVAL,
        )
        pm.start()
        assert success.wait(timeout=2.0), 'poller never fired'
        pm.stop()
        assert pm._thread is not None
        assert not pm._thread.is_alive()

    def test_polls_multiple_times(self) -> None:
        counter = [0]
        done = threading.Event()

        def fetch() -> FetchResult:
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
        assert done.wait(timeout=2.0), 'did not poll 3 times'
        pm.stop()
        assert counter[0] >= 3
