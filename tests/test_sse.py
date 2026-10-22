import logging
import threading
import time

import pytest
from sse_server import SseServer, wait_until

from dynatrace.openfeature._sse import SseManager, mask_url
from dynatrace.openfeature._sse_message import SseMessage


class _Recorder:
    def __init__(self) -> None:
        self.connected = 0
        self.disconnected = 0
        self.messages: list[SseMessage] = []
        self._lock = threading.Lock()

    def on_connected(self) -> None:
        with self._lock:
            self.connected += 1

    def on_disconnected(self) -> None:
        with self._lock:
            self.disconnected += 1

    def on_message(self, msg: SseMessage) -> None:
        with self._lock:
            self.messages.append(msg)


def _start(url: str, rec: _Recorder) -> SseManager:
    manager = SseManager(url, rec.on_connected, rec.on_disconnected, rec.on_message)
    manager.start()
    return manager


class TestMaskUrl:
    @pytest.mark.parametrize(
        ("url", "masked"),
        [
            (
                "https://sse.example/sse?key=secret",
                "https://sse.example/sse?<redacted>",
            ),
            ("https://sse.example/sse", "https://sse.example/sse"),
        ],
    )
    def test_masks_query_string(self, url: str, masked: str) -> None:
        assert mask_url(url) == masked


class TestSseManager:
    def test_connect_reports_connected(self, sse_server: SseServer) -> None:
        rec = _Recorder()
        manager = _start(sse_server.url, rec)
        try:
            assert wait_until(lambda: rec.connected == 1)
            assert sse_server.client_count == 1
        finally:
            manager.stop()

    def test_parsed_events_reach_on_message(self, sse_server: SseServer) -> None:
        rec = _Recorder()
        manager = _start(sse_server.url, rec)
        try:
            assert wait_until(lambda: sse_server.client_count == 1)
            sse_server.emit('{"type":"refetchConfig","lastModified":1704153600}')
            sse_server.emit('{"data":"{\\"type\\":\\"refetchConfig\\"}"}', event="msg")
            assert wait_until(lambda: len(rec.messages) == 2)
            assert rec.messages == [
                SseMessage(type="refetchConfig", last_modified=1704153600),
                SseMessage(type="refetchConfig"),
            ]
        finally:
            manager.stop()

    def test_unparseable_events_and_comments_are_dropped(
        self, sse_server: SseServer
    ) -> None:
        rec = _Recorder()
        manager = _start(sse_server.url, rec)
        try:
            assert wait_until(lambda: sse_server.client_count == 1)
            sse_server.emit("not-json")
            sse_server.comment("keep-alive")
            sse_server.emit('{"type":"marker"}')
            assert wait_until(lambda: len(rec.messages) == 1)
            assert rec.messages == [SseMessage(type="marker")]
            assert rec.disconnected == 0
        finally:
            manager.stop()

    def test_drop_reports_disconnect_and_library_reconnects(
        self, sse_server: SseServer
    ) -> None:
        rec = _Recorder()
        manager = _start(sse_server.url, rec)
        try:
            assert wait_until(lambda: rec.connected == 1)
            sse_server.drop()
            assert wait_until(lambda: rec.disconnected >= 1)
            # the reconnect itself is ld_eventsource's, after its initial backoff
            assert wait_until(lambda: rec.connected == 2, timeout=5.0)
        finally:
            manager.stop()

    def test_unreachable_endpoint_reports_faults_but_never_connects(self) -> None:
        rec = _Recorder()
        # port 1 is never listening, so every attempt is refused
        manager = _start("http://127.0.0.1:1/sse", rec)
        try:
            assert wait_until(lambda: rec.disconnected >= 1)
            assert rec.connected == 0
        finally:
            manager.stop()

    def test_refused_stream_is_retried(self, sse_server: SseServer) -> None:
        rec = _Recorder()
        sse_server.refuse = True
        manager = _start(sse_server.url, rec)
        try:
            assert wait_until(lambda: rec.disconnected >= 1)
            sse_server.refuse = False
            assert wait_until(lambda: rec.connected == 1, timeout=5.0)
        finally:
            manager.stop()

    def test_stop_closes_the_stream_and_silences_callbacks(
        self, sse_server: SseServer
    ) -> None:
        rec = _Recorder()
        manager = _start(sse_server.url, rec)
        assert wait_until(lambda: sse_server.client_count == 1)
        manager.stop()
        # no event to wake the reader first: the socket itself must close, on Windows too
        assert wait_until(lambda: sse_server.client_count == 0)
        sse_server.emit('{"type":"refetchConfig"}')
        assert not wait_until(lambda: rec.messages != [], timeout=0.3)

    def test_a_reconnect_racing_stop_is_closed(self, sse_server: SseServer) -> None:
        rec = _Recorder()
        sse_server.refuse = True
        manager = _start(sse_server.url, rec)
        # the first attempt is refused, so the reader sleeps out a backoff of up to a second
        assert wait_until(lambda: rec.disconnected >= 1)
        manager.stop()
        sse_server.refuse = False
        # the library reconnects once the sleep ends, despite the stop; that stream must close again
        assert wait_until(lambda: len(sse_server.paths) >= 2, timeout=3.0)
        assert wait_until(lambda: sse_server.client_count == 0)
        assert rec.connected == 0

    def test_stop_returns_promptly_while_the_reader_is_blocked(
        self, sse_server: SseServer
    ) -> None:
        rec = _Recorder()
        manager = _start(sse_server.url, rec)
        assert wait_until(lambda: rec.connected == 1)
        # the server sends nothing, so the reader stays in recv(); on Windows the library's close()
        # would block on it, so stop() must not wait for the close
        t0 = time.monotonic()
        manager.stop()
        assert time.monotonic() - t0 < 0.5, "stop() blocked on the SSE close"
        sse_server.emit('{"type":"refetchConfig"}')
        assert not wait_until(lambda: rec.messages != [], timeout=0.3)

    def test_a_failing_callback_does_not_kill_the_stream(
        self, sse_server: SseServer
    ) -> None:
        received: list[SseMessage] = []

        def on_message(msg: SseMessage) -> None:
            received.append(msg)
            if len(received) == 1:
                raise RuntimeError("boom")

        manager = SseManager(sse_server.url, lambda: None, lambda: None, on_message)
        manager.start()
        try:
            assert wait_until(lambda: sse_server.client_count == 1)
            sse_server.emit('{"type":"a"}')
            sse_server.emit('{"type":"b"}')
            assert wait_until(lambda: len(received) == 2)
        finally:
            manager.stop()

    def test_logs_never_carry_the_query_string(
        self, sse_server: SseServer, caplog: pytest.LogCaptureFixture
    ) -> None:
        rec = _Recorder()
        caplog.set_level(logging.DEBUG)
        manager = _start(sse_server.url + "?key=secret", rec)
        try:
            assert wait_until(lambda: rec.connected == 1)
            sse_server.drop()
            assert wait_until(lambda: rec.disconnected >= 1)
        finally:
            manager.stop()
        assert sse_server.paths[0] == "/sse?key=secret"
        # urllib3's own DEBUG request log is out of our hands, as it is for CDN fetches
        ours = [r for r in caplog.records if r.name.startswith("dynatrace.")]
        assert ours
        assert not any("secret" in r.getMessage() for r in ours)
