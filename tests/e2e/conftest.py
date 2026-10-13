from collections.abc import Callable, Iterator

import pytest
from mock_server import MockServer

# needs an SSE-triggered re-fetch; SSE support is on hold
_SKIPPED_TAGS = {"single-flight"}


def pytest_bdd_apply_tag(tag: str, function: Callable[..., object]) -> bool:
    # gherkin tags are not registered pytest markers, so skip or ignore them rather than mark
    if tag in _SKIPPED_TAGS:
        pytest.mark.skip(reason=f"@{tag} is not supported yet")(function)
    return True


@pytest.fixture(scope="session")
def mock_server() -> Iterator[MockServer]:
    """One mock server container for the whole run; scenarios reset it for isolation."""
    server = MockServer.start()
    yield server
    server.stop()
