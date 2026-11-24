from __future__ import annotations

import re
from urllib.parse import urlparse

from ._env import ENV_CONFIG_ORIGIN, ENV_METRICS_ORIGIN, ENV_SDK_KEY, resolve

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
    if parsed.path and parsed.path != "/":
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


def _resolve_origin(code_value: str | None, field: str, env_name: str) -> str | None:
    """Return the normalised origin from code or env, or None to use the default."""
    value, source = resolve(code_value, field, env_name)
    if value is None:
        return None
    value = value.strip()
    if not value:
        raise ValueError(f"{field} must not be blank")
    return _normalise_origin(value, source)


class DynatraceProviderOptions:
    __slots__ = ("_cdn_url", "_key", "_metrics_url")

    def __init__(self, key: str, cdn_url: str, metrics_url: str) -> None:
        self._key = key
        self._cdn_url = cdn_url
        self._metrics_url = metrics_url

    @classmethod
    def from_sdk_key(
        cls,
        sdk_key: str | None = None,
        config_origin: str | None = None,
        metrics_origin: str | None = None,
    ) -> DynatraceProviderOptions:
        """Build options; any argument left as None falls back to its DT_FM_* env var.

        If ``config_origin`` is set, carefully consider whether ``metrics_origin`` should be
        set too; an SDK proxy or custom host typically serves both.
        """
        if sdk_key is not None and not sdk_key.strip():
            sdk_key = None
        sdk_key, key_field = resolve(sdk_key, "sdk_key", ENV_SDK_KEY)
        if sdk_key is None:
            raise ValueError(f"SDK key is required; pass sdk_key or set {ENV_SDK_KEY}")
        trimmed = sdk_key.strip()
        key_match = _KEY_PATTERN.match(trimmed)
        if not key_match:
            last_dot = trimmed.rfind(".")
            public_part = trimmed[:last_dot] if last_dot > 0 else "<redacted>"
            raise ValueError(
                f"invalid key format in {key_field} (public: {public_part!r}); "
                "expected dt{NN}.server_{geo}_{rand}.{secret}_{sha}"
            )
        _token, platform, geo, _rand, _secret, _sha = key_match.groups()
        config = _resolve_origin(config_origin, "config_origin", ENV_CONFIG_ORIGIN)
        base = config or f"https://cdn.{geo}.fm.dynatrace.com"
        cdn_url = f"{base}/{platform}/{trimmed}.json"
        metrics = _resolve_origin(metrics_origin, "metrics_origin", ENV_METRICS_ORIGIN)
        resolved_metrics_url = (
            metrics or f"https://metrics.{geo}.fm.dynatrace.com"
        ) + _METRICS_PATH
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
