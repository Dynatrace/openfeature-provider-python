import json

import pytest

from dynatrace.openfeature._cdn_config import extract_sse_url

_ORIGIN = "https://cdn.us.fm.dynatrace.com"


def _config(*streams: object) -> str:
    return json.dumps({"flags": {}, "eventStreams": list(streams)})


class TestExtractSseUrl:
    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            (
                _config({"type": "sse", "url": "https://sse.example/s?k=1"}),
                "https://sse.example/s?k=1",
            ),
            (
                _config({"type": "sse", "endpoint": {"requestUri": "/sse"}}),
                _ORIGIN + "/sse",
            ),
            (
                _config(
                    {
                        "type": "sse",
                        "endpoint": {
                            "origin": "https://rt.example",
                            "requestUri": "/sse",
                        },
                    }
                ),
                "https://rt.example/sse",
            ),
            # a non-blank url wins over the endpoint
            (
                _config(
                    {
                        "type": "sse",
                        "url": "https://direct.example/sse",
                        "endpoint": {"requestUri": "/other"},
                    }
                ),
                "https://direct.example/sse",
            ),
            # a blank url falls through to the endpoint, a blank origin to the fallback
            (
                _config(
                    {
                        "type": "sse",
                        "url": " ",
                        "endpoint": {"origin": "", "requestUri": "/sse"},
                    }
                ),
                _ORIGIN + "/sse",
            ),
            # entries of other types are skipped, unknown attributes ignored
            (
                _config(
                    {"type": "websocket", "url": "wss://ws.example"},
                    {"type": "sse", "url": "https://sse.example", "future": 1},
                ),
                "https://sse.example",
            ),
        ],
        ids=[
            "direct-url",
            "endpoint-fallback-origin",
            "endpoint-explicit-origin",
            "url-wins",
            "blank-url-and-origin",
            "skips-other-types",
        ],
    )
    def test_resolves(self, config: str, expected: str) -> None:
        assert extract_sse_url(config, _ORIGIN) == expected

    @pytest.mark.parametrize(
        "config",
        [
            None,
            "",
            "not-json",
            "[]",
            json.dumps({"flags": {}}),
            json.dumps({"eventStreams": {"type": "sse"}}),
            _config(),
            _config("sse", None),
            _config({"type": "websocket", "url": "wss://ws.example"}),
            _config({"type": "sse"}),
            _config({"type": "sse", "endpoint": {"origin": _ORIGIN}}),
            # only the first sse entry counts, even when it is unusable
            _config({"type": "sse"}, {"type": "sse", "url": "https://late.example"}),
        ],
        ids=[
            "none",
            "empty",
            "not-json",
            "array",
            "no-event-streams",
            "event-streams-not-a-list",
            "empty-list",
            "non-object-entries",
            "no-sse-entry",
            "sse-without-url-or-endpoint",
            "endpoint-without-request-uri",
            "first-sse-entry-unusable",
        ],
    )
    def test_no_usable_entry(self, config: str | None) -> None:
        assert extract_sse_url(config, _ORIGIN) is None

    def test_endpoint_without_any_origin(self) -> None:
        config = _config({"type": "sse", "endpoint": {"requestUri": "/sse"}})
        assert extract_sse_url(config, None) is None
