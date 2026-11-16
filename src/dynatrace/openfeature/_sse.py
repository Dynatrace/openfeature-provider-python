from __future__ import annotations

import logging
import threading
from collections.abc import Callable

from ld_eventsource import SSEClient
from ld_eventsource.actions import Event, Fault, Start
from ld_eventsource.config import ConnectStrategy, ErrorStrategy, RetryDelayStrategy
from urllib3.util import Timeout as Urllib3Timeout

from ._cdn import REQUEST_TIMEOUT_S
from ._sse_message import SseMessage

log = logging.getLogger(__name__)

# the stream is long-lived and idle between notifications, so only a dead connection should trip this
SSE_IDLE_TIMEOUT_S: float = 150
# reconnect backoff: doubles from the initial delay up to the max, with jitter
SSE_INITIAL_RETRY_DELAY_S: float = 1.0
SSE_MAX_RETRY_DELAY_S: float = 30.0
_SSE_RETRY_JITTER = 0.5


def mask_url(url: str) -> str:
    """Mask the query string, which carries the Ably credentials, for safe logging."""
    q = url.find("?")
    return url[:q] + "?<redacted>" if q >= 0 else url


class SseManager:
    """Manages the SSE connection to the Ably event stream (an opaque HTTP endpoint, no Ably SDK).

    One daemon thread routes ``Start``, ``Fault`` and parsed ``Event`` actions to the callbacks;
    reconnection and backoff are delegated to ``ld_eventsource``.
    """

    def __init__(
        self,
        sse_url: str,
        on_connected: Callable[[], None],
        on_disconnected: Callable[[], None],
        on_message: Callable[[SseMessage], None],
    ) -> None:
        self._sse_url = sse_url
        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._on_message = on_message
        self._stopped = threading.Event()
        self._client: SSEClient | None = None
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        """The stream URL, credentials included; mask it before logging."""
        return self._sse_url

    def start(self) -> None:
        """Open the stream on a daemon thread; returns without waiting for it to connect."""
        if self._thread is not None:
            return
        self._client = SSEClient(
            connect=ConnectStrategy.http(
                self._sse_url,
                urllib3_request_options={
                    "timeout": Urllib3Timeout(
                        connect=REQUEST_TIMEOUT_S, read=SSE_IDLE_TIMEOUT_S
                    )
                },
            ),
            initial_retry_delay=SSE_INITIAL_RETRY_DELAY_S,
            retry_delay_strategy=RetryDelayStrategy.default(
                max_delay=SSE_MAX_RETRY_DELAY_S, jitter_multiplier=_SSE_RETRY_JITTER
            ),
            # never give up: every failure surfaces as a Fault and the client reconnects
            error_strategy=ErrorStrategy.always_continue(),
        )
        self._thread = threading.Thread(
            target=self._run, name="dt-provider-sse", daemon=True
        )
        self._thread.start()
        log.info("SSE manager started (url=%s)", mask_url(self._sse_url))

    def stop(self) -> None:
        """Close the stream; callbacks stop at once. The library wakes a reader blocked mid-read
        before closing the socket, so this returns promptly and is safe under a caller's lock."""
        if self._stopped.is_set():
            return
        self._stopped.set()
        client = self._client
        if client is not None:
            self._close_client(client)
            log.info("SSE manager stopped")

    def _close_client(self, client: SSEClient) -> None:
        try:
            # the library owns its pool, so close() also closes the pooled connection
            # known limitation: on Windows ld_eventsource only releases the socket (left to GC, server sees no FIN);
            # see launchdarkly/python-eventsource#74
            client.close()
        except Exception as exc:
            log.warning("error closing SSE connection: %s", type(exc).__name__)

    def _run(self) -> None:
        client = self._client
        if client is None:
            return
        try:
            for action in client.all:
                # the client reconnects even after close() if it was sleeping out a backoff
                if self._stopped.is_set():
                    break
                try:
                    self._dispatch(action)
                except Exception:
                    # a failing callback must not kill the stream
                    log.warning("SSE callback failed", exc_info=True)
        except Exception as exc:
            if not self._stopped.is_set():
                # always_continue keeps the iterator alive, so only a library fault lands here
                log.warning("SSE reader stopped unexpectedly: %s", type(exc).__name__)
                self._on_disconnected()
        finally:
            # closes a connection opened after stop() raced a pending reconnect
            self._close_client(client)

    def _dispatch(self, action: object) -> None:
        if isinstance(action, Start):
            log.info("SSE connection opened (url=%s)", mask_url(self._sse_url))
            self._on_connected()
        elif isinstance(action, Fault):
            if action.error is None:
                log.info("SSE connection closed")
            else:
                # type only: the message of a connect error can carry the credentialed URL
                log.warning(
                    "SSE error (url=%s): %s",
                    mask_url(self._sse_url),
                    type(action.error).__name__,
                )
            self._on_disconnected()
        elif isinstance(action, Event):
            parsed = SseMessage.parse(action.data)
            if parsed is None:
                log.debug(
                    "SSE event received (event=%s) but payload unparseable; ignoring",
                    action.event,
                )
                return
            log.debug(
                "SSE event received (event=%s, type=%s, lastModified=%s)",
                action.event,
                parsed.type,
                parsed.last_modified,
            )
            self._on_message(parsed)
        # comments are keep-alives; ignore
