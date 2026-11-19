from __future__ import annotations

import os

ENV_SDK_KEY = "DT_FM_SDK_KEY"
ENV_CONFIG_ORIGIN = "DT_FM_CONFIG_ORIGIN"
ENV_METRICS_ORIGIN = "DT_FM_METRICS_ORIGIN"
ENV_SSE_DISABLED = "DT_FM_SSE_DISABLED"


def _getenv(name: str) -> str | None:
    return os.environ.get(name)


def lookup(name: str) -> str | None:
    """Return the trimmed value of env var ``name``, or None if it is unset or blank."""
    raw = _getenv(name)
    if raw is None or not raw.strip():
        return None
    return raw.strip()


def resolve(
    code_value: str | None, field: str, env_name: str
) -> tuple[str | None, str]:
    """Return the effective value (code wins over env) and the name to cite in errors."""
    if code_value is not None:
        return code_value, field
    return lookup(env_name), env_name


def resolve_bool(code_value: bool | None, env_name: str, default: bool) -> bool:
    if code_value is not None:
        return code_value
    raw = lookup(env_name)
    if raw is None:
        return default
    lowered = raw.lower()
    if lowered not in ("true", "false"):
        raise ValueError(f"{env_name} must be 'true' or 'false'; got {raw!r}")
    return lowered == "true"
