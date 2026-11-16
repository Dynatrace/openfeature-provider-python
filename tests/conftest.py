from collections.abc import Iterator

import pytest
from sse_server import SseServer


@pytest.fixture
def sse_server() -> Iterator[SseServer]:
    """A live SSE endpoint on an ephemeral localhost port, stopped after the test."""
    server = SseServer()
    yield server
    server.stop()
