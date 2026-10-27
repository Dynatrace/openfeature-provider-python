import pytest

from dynatrace.openfeature._sse_message import SseMessage


class TestSseMessageParse:
    @pytest.mark.parametrize(
        ("payload", "expected"),
        [
            (
                '{"type":"refetchConfig","etag":"\\"v2\\"","lastModified":1704153600}',
                SseMessage(type="refetchConfig", etag='"v2"', last_modified=1704153600),
            ),
            (
                '{"data":"{\\"type\\":\\"refetchConfig\\",\\"lastModified\\":1704153600}"}',
                SseMessage(type="refetchConfig", last_modified=1704153600),
            ),
            # unknown types parse; ignoring them is the caller's call
            ('{"type":"unknownType"}', SseMessage(type="unknownType")),
            # unknown fields, including metadata, are dropped
            (
                '{"type":"refetchConfig","metadata":{"a":1},"extra":true}',
                SseMessage(type="refetchConfig"),
            ),
            # a malformed lastModified reads as unknown rather than rejecting the message
            (
                '{"type":"refetchConfig","lastModified":"soon"}',
                SseMessage(type="refetchConfig"),
            ),
            (
                '{"type":"refetchConfig","lastModified":true}',
                SseMessage(type="refetchConfig"),
            ),
        ],
        ids=[
            "flat",
            "ably-envelope",
            "unknown-type",
            "unknown-fields",
            "bad-last-modified",
            "bool-last-modified",
        ],
    )
    def test_parses(self, payload: str, expected: SseMessage) -> None:
        assert SseMessage.parse(payload) == expected

    @pytest.mark.parametrize(
        "payload",
        [
            None,
            "",
            "   ",
            "not-json",
            "{}",
            "[]",
            '"refetchConfig"',
            '{"lastModified":1704153600}',
            '{"type":""}',
            '{"type":"   "}',
            '{"type":42}',
            '{"data":"not-json"}',
            '{"data":"[1]"}',
        ],
        ids=[
            "none",
            "empty",
            "blank",
            "not-json",
            "empty-object",
            "array",
            "string",
            "missing-type",
            "empty-type",
            "blank-type",
            "non-string-type",
            "envelope-not-json",
            "envelope-not-object",
        ],
    )
    def test_rejects(self, payload: str | None) -> None:
        assert SseMessage.parse(payload) is None

    def test_non_string_data_is_read_as_a_flat_message(self) -> None:
        # only a JSON-encoded string is an Ably envelope
        assert SseMessage.parse('{"type":"refetchConfig","data":{"x":1}}') == (
            SseMessage(type="refetchConfig")
        )
