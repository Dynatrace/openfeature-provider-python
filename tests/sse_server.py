"""A real-socket SSE endpoint for unit tests.

``responses`` patches ``requests`` and cannot stream ``text/event-stream``, and
the SSE client talks to ``urllib3`` directly anyway, so SSE tests run against a
real socket: a ThreadingHTTPServer serving one stream path.
"""

from __future__ import annotations

import queue
import select
import socket
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SSE_PATH = "/sse"
# how often an idle stream handler checks whether the client went away
_IDLE_CHECK_S = 0.05


class _SseHandler(BaseHTTPRequestHandler):
    # chunked, like a real SSE server: urllib3 hands a chunked body over chunk by chunk, but blocks on a
    # fixed-size read of an unframed one, so events would sit in its buffer
    protocol_version = "HTTP/1.1"
    server: _OwnedServer

    def do_GET(self) -> None:  # BaseHTTPRequestHandler dispatch name
        owner = self.server.owner
        owner.record_path(self.path)
        if self.path.split("?", 1)[0] != SSE_PATH:
            self.send_error(404)
            return
        if owner.refuse:
            self.send_error(503)
            self.close_connection = True
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Transfer-Encoding", "chunked")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.flush()
        events: queue.Queue[bytes | None] = queue.Queue()
        owner.register(events)
        try:
            self._stream(events)
        except OSError:
            pass  # client went away mid-write
        finally:
            owner.unregister(events)

    def _stream(self, events: queue.Queue[bytes | None]) -> None:
        while True:
            try:
                item = events.get(timeout=_IDLE_CHECK_S)
            except queue.Empty:
                if self._client_closed():
                    return
                continue
            if item is None:
                # dropped by the test: end the body cleanly, then close
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
                self.close_connection = True
                return
            self.wfile.write(b"%x\r\n%s\r\n" % (len(item), item))
            self.wfile.flush()

    def _client_closed(self) -> bool:
        readable, _, _ = select.select([self.connection], [], [], 0)
        if not readable:
            return False
        try:
            return self.connection.recv(1, socket.MSG_PEEK) == b""
        except OSError:
            return True

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - signature of the overridden method
        pass


class _OwnedServer(ThreadingHTTPServer):
    daemon_threads = True
    owner: SseServer


class SseServer:
    """A real SSE endpoint the tests can push events through, drop, and refuse."""

    def __init__(self) -> None:
        self._httpd = _OwnedServer(("127.0.0.1", 0), _SseHandler)
        self._httpd.owner = self
        self._lock = threading.Lock()
        self._clients: list[queue.Queue[bytes | None]] = []
        self.paths: list[str] = []
        #: answer new connections with 503 while set
        self.refuse = False
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    @property
    def origin(self) -> str:
        host, port = self._httpd.server_address[:2]
        return f"http://{host!s}:{port}"

    @property
    def url(self) -> str:
        return self.origin + SSE_PATH

    @property
    def client_count(self) -> int:
        with self._lock:
            return len(self._clients)

    def record_path(self, path: str) -> None:
        with self._lock:
            self.paths.append(path)

    def register(self, events: queue.Queue[bytes | None]) -> None:
        with self._lock:
            self._clients.append(events)

    def unregister(self, events: queue.Queue[bytes | None]) -> None:
        with self._lock:
            if events in self._clients:
                self._clients.remove(events)

    def emit(self, data: str, event: str | None = None) -> None:
        """Send ``data`` as one SSE event to every connected client."""
        frame = "".join(f"data: {line}\n" for line in data.split("\n"))
        if event is not None:
            frame = f"event: {event}\n" + frame
        self._broadcast((frame + "\n").encode())

    def comment(self, text: str) -> None:
        self._broadcast(f": {text}\n\n".encode())

    def drop(self) -> None:
        """End every open stream; clients may reconnect unless ``refuse`` is set."""
        with self._lock:
            clients = list(self._clients)
        for events in clients:
            events.put(None)

    def _broadcast(self, frame: bytes) -> None:
        with self._lock:
            clients = list(self._clients)
        for events in clients:
            events.put(frame)

    def stop(self) -> None:
        self.refuse = True
        self.drop()
        self._httpd.shutdown()
        self._httpd.server_close()


def wait_until(condition: Callable[[], bool], timeout: float = 5.0) -> bool:
    """Poll ``condition`` until it holds or ``timeout`` passes; report whether it held."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return condition()
