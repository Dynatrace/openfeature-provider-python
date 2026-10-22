from __future__ import annotations

import json
import typing

#: ``eventStreams[].type`` value identifying the SSE change-notification stream
STREAM_TYPE_SSE = "sse"


def _non_blank(value: typing.Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def extract_sse_url(config_json: str | None, fallback_origin: str | None) -> str | None:
    """Return the SSE URL of the first ``sse`` entry in ``eventStreams``, or None (ADR-0008).
    A non-blank ``url`` wins; else ``endpoint.requestUri`` on ``endpoint.origin`` or ``fallback_origin``."""
    if not config_json or not config_json.strip():
        return None
    try:
        config: typing.Any = json.loads(config_json)
    except ValueError:
        return None
    streams = config.get("eventStreams") if isinstance(config, dict) else None
    if not isinstance(streams, list):
        return None
    for stream in streams:
        if not isinstance(stream, dict) or stream.get("type") != STREAM_TYPE_SSE:
            continue
        url = _non_blank(stream.get("url"))
        if url is not None:
            return url
        endpoint = stream.get("endpoint")
        if not isinstance(endpoint, dict):
            return None
        request_uri = _non_blank(endpoint.get("requestUri"))
        if request_uri is None:
            return None
        origin = _non_blank(endpoint.get("origin")) or _non_blank(fallback_origin)
        return origin + request_uri if origin is not None else None
    return None
