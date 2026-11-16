from __future__ import annotations

import json
import typing
from dataclasses import dataclass

#: server-side signal to re-fetch the flagd config JSON from the CDN
TYPE_REFETCH_CONFIG = "refetchConfig"


@dataclass(frozen=True)
class SseMessage:
    """Parsed SSE message envelope per the SSE message format spec.

    Callers must ignore unknown ``type``s; ``etag`` is parsed for completeness only.
    ``metadata`` is not exposed: providers must not forward it to OFREP clients.
    """

    type: str
    etag: str | None = None
    last_modified: int | None = None

    @classmethod
    def parse(cls, payload: str | None) -> SseMessage | None:
        """Parse an Ably envelope (message JSON-encoded in ``data``) or a flat message.
        Returns None for invalid JSON or a missing/blank ``type``."""
        if not payload or not payload.strip():
            return None
        try:
            outer: typing.Any = json.loads(payload)
            data = outer.get("data") if isinstance(outer, dict) else None
            inner: typing.Any = json.loads(data) if isinstance(data, str) else outer
        except ValueError:
            return None
        if not isinstance(inner, dict):
            return None
        msg_type = inner.get("type")
        if not isinstance(msg_type, str) or not msg_type.strip():
            return None
        etag = inner.get("etag")
        last_modified = inner.get("lastModified")
        return cls(
            type=msg_type,
            etag=etag if isinstance(etag, str) else None,
            # a malformed lastModified reads as unknown, so the re-fetch it announces is never dropped
            last_modified=last_modified
            if isinstance(last_modified, int) and not isinstance(last_modified, bool)
            else None,
        )
