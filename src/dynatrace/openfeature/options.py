from __future__ import annotations

import re
from urllib.parse import urlparse

# SDK key format per providers.md §3.0:
# {token}.server_{geo}_{publicRandom}.{secret}_{sha1}
_KEY_PATTERN = re.compile(
    r"^(dt[0-9]{2})\.(server)_([a-z]{2,})_([a-zA-Z0-9]{10,})\.([a-fA-F0-9]{32,})_([a-f0-9]{10,})$"
)

_ALLOWED_SCHEMES = ("http", "https")
_METRICS_PATH = "/v1/metrics"


def _normalise_origin(value: str, field: str) -> str:
    """Validate that ``value`` is a bare origin and return it in canonical form.

    An origin is a scheme, host, and optional port; nothing else. Anything that
    could not be safely appended to is rejected here, so a malformed override
    fails at construction rather than as a 404 on the first request.
    """
    try:
        parsed = urlparse(value)
    except ValueError as exc:
        raise ValueError(f"{field} is not a valid URL; got {value!r}") from exc
    if parsed.scheme not in _ALLOWED_SCHEMES:
        raise ValueError(f"{field} must use http or https; got {value!r}")
    try:
        # Accessing .port raises if the port is present but not a valid integer.
        _ = parsed.port
    except ValueError as exc:
        raise ValueError(f"{field} has an invalid port; got {value!r}") from exc
    if not parsed.hostname:
        raise ValueError(f"{field} must include a host; got {value!r}")
    if parsed.path.rstrip("/"):
        raise ValueError(f"{field} must not include a path; got {value!r}")
    if parsed.query:
        raise ValueError(f"{field} must not include a query string; got {value!r}")
    if parsed.fragment:
        raise ValueError(f"{field} must not include a fragment; got {value!r}")
    if parsed.username or parsed.password:
        raise ValueError(f"{field} must not include user info; got {value!r}")
    # netloc rather than hostname + port: it preserves the brackets around an IPv6
    # literal, which hostname strips. User info is already rejected above, so at
    # this point netloc is exactly the host and optional port.
    return f"{parsed.scheme}://{parsed.netloc.lower()}"


class DynatraceProviderOptions:
    __slots__ = ("_cdn_url", "_key", "_metrics_url")

    def __init__(self, key: str, cdn_url: str, metrics_url: str) -> None:
        self._key = key
        self._cdn_url = cdn_url
        self._metrics_url = metrics_url

    @classmethod
    def from_sdk_key(
        cls,
        sdk_key: str,
        config_origin: str | None = None,
        metrics_origin: str | None = None,
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
        _token, platform, geo, _rand, _secret, _sha = key_match.groups()
        if config_origin is not None:
            config_origin = config_origin.strip()
            if not config_origin:
                raise ValueError("config_origin must not be blank")
            origin = _normalise_origin(config_origin, "config_origin")
            base = origin + "/" + platform + "/"
        else:
            base = f"https://cdn.{geo}.fm.dynatrace.com/{platform}/"
        cdn_url = base + trimmed + ".json"
        if metrics_origin is not None:
            metrics_origin = metrics_origin.strip()
            if not metrics_origin:
                raise ValueError("metrics_origin must not be blank")
            origin = _normalise_origin(metrics_origin, "metrics_origin")
            resolved_metrics_url = origin + _METRICS_PATH
        else:
            resolved_metrics_url = (
                f"https://metrics.{geo}.fm.dynatrace.com{_METRICS_PATH}"
            )
        return cls(key=trimmed, cdn_url=cdn_url, metrics_url=resolved_metrics_url)

    @property
    def public_key(self) -> str:
        # Everything before the last '.' — safe to log; never exposes the secret segment.
        idx = self._key.rfind(".")
        return self._key[:idx] if idx > 0 else "<redacted>"

    @property
    def cdn_url(self) -> str:
        return self._cdn_url

    @property
    def metrics_url(self) -> str:
        return self._metrics_url
