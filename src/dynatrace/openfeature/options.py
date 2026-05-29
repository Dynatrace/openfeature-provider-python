from __future__ import annotations

import re

# SDK key format per providers.md §3.0:
# {token}.server_{geo}_{publicRandom}.{secret}_{sha1}
_KEY_PATTERN = re.compile(
    r"^(dt[0-9]{2})\.(server)_([a-z]{2,})_([a-zA-Z0-9]{10,})\.([a-fA-F0-9]{32,})_([a-f0-9]{10,})$"
)


class DynatraceProviderOptions:
    __slots__ = ("_cdn_url", "_key")

    def __init__(self, key: str, cdn_url: str) -> None:
        self._key = key
        self._cdn_url = cdn_url

    @classmethod
    def from_sdk_key(
        cls,
        sdk_key: str,
        cdn_base_url: str | None = None,
    ) -> DynatraceProviderOptions:
        if not sdk_key or not sdk_key.strip():
            raise ValueError("sdk_key must not be null or blank")
        trimmed = sdk_key.strip()
        key_match = _KEY_PATTERN.match(trimmed)
        if not key_match:
            last_dot = trimmed.rfind(".")
            public_part = trimmed[:last_dot] if last_dot > 0 else "<redacted>"
            raise ValueError(
                f"invalid key format (public: {public_part!r}); "
                "expected dt{NN}.server_{geo}_{rand}.{secret}_{sha}"
            )
        _token, _platform, geo, _rand, _secret, _sha = key_match.groups()
        if cdn_base_url is not None:
            cdn_base_url = cdn_base_url.strip()
            if not cdn_base_url:
                raise ValueError("cdn_base_url must not be blank")
            base = cdn_base_url if cdn_base_url.endswith("/") else cdn_base_url + "/"
        else:
            base = f"https://cdn.{geo}.fm.dynatrace.com/server/"
        cdn_url = base + trimmed + ".json"
        return cls(key=trimmed, cdn_url=cdn_url)

    @property
    def public_key(self) -> str:
        # Everything before the last '.' — safe to log; never exposes the secret segment.
        idx = self._key.rfind(".")
        return self._key[:idx] if idx > 0 else "<redacted>"

    @property
    def cdn_url(self) -> str:
        return self._cdn_url
