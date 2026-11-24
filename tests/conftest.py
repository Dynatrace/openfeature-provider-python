from collections.abc import Iterator

import pytest
from sse_server import SseServer

from dynatrace.openfeature import _env


@pytest.fixture(autouse=True)
def env(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """A fake process environment; tests set DT_FM_* vars here instead of in os.environ."""
    fake: dict[str, str] = {}
    monkeypatch.setattr(_env, "_getenv", fake.get)
    return fake


@pytest.fixture
def sse_server() -> Iterator[SseServer]:
    """A live SSE endpoint on an ephemeral localhost port, stopped after the test."""
    server = SseServer()
    yield server
    server.stop()
