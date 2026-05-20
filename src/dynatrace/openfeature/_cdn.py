from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime

import requests

from ._retry import RetryStrategy, default_should_retry_status
from .options import DynatraceProviderOptions

log = logging.getLogger(__name__)

REQUEST_TIMEOUT_S: float = 5.0
_DEFAULT_RETRY_AFTER_MS: float = 60_000.0


@dataclass
class FetchResult:
    status: int
    body: str | None
    etag: str | None
    last_modified: str | None

    @property
    def is_not_modified(self) -> bool:
        return self.status == 304

    @property
    def is_rate_limited(self) -> bool:
        return self.status == 429

    @property
    def is_success(self) -> bool:
        return 200 <= self.status < 300


@dataclass
class _CacheState:
    etag: str | None = None
    last_modified_header: str | None = None
    last_modified_at: datetime | None = None


class CdnFetcher:
    def __init__(
        self,
        options: DynatraceProviderOptions,
        retry_strategy: RetryStrategy | None = None,
        *,
        session: requests.Session | None = None,
        clock: object = None,
    ) -> None:
        self._url = options.cdn_url
        self._retry = retry_strategy or RetryStrategy()
        self._session = session or requests.Session()
        self._clock: object = clock or time.time
        self._cache = _CacheState()
        self._rate_limited_until: float = 0.0
        self._fetch_lock = threading.Lock()

    def is_rate_limited(self, now: float | None = None) -> bool:
        t = now if now is not None else float(self._clock())  # type: ignore[operator]
        return t < self._rate_limited_until

    def fetch(self, cancel: threading.Event | None = None) -> FetchResult:
        if self.is_rate_limited():
            log.debug("rate limited; skipping fetch")
            return FetchResult(429, None, None, None)
        if not self._fetch_lock.acquire(blocking=False):
            log.debug("fetch already in progress; skipping")
            return FetchResult(304, None, None, None)
        try:
            return self._retry.execute(
                self._do_fetch,
                should_retry=lambda r: default_should_retry_status(r.status),
                retryable_exceptions=(OSError, requests.RequestException),
                cancel=cancel,
            )
        finally:
            self._fetch_lock.release()

    def _do_fetch(self) -> FetchResult:
        cache = self._cache
        headers: dict[str, str] = {}
        if cache.etag is not None:
            headers["If-None-Match"] = cache.etag
        if cache.last_modified_header is not None:
            headers["If-Modified-Since"] = cache.last_modified_header

        try:
            resp = self._session.get(
                self._url,
                headers=headers,
                timeout=REQUEST_TIMEOUT_S,
            )
        except requests.RequestException:
            raise

        if resp.status_code == 304:
            return FetchResult(304, None, cache.etag, cache.last_modified_header)

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            delay_ms = _parse_retry_after_ms(retry_after, float(self._clock()))  # type: ignore[operator]
            self._rate_limited_until = float(self._clock()) + delay_ms / 1000.0  # type: ignore[operator]
            log.warning("rate limited (429); pausing fetches for %.0fms", delay_ms)
            return FetchResult(429, None, None, None)

        new_etag = resp.headers.get("ETag")
        new_last_modified = resp.headers.get("Last-Modified")
        body = resp.text

        if resp.status_code >= 200 and resp.status_code < 300:
            new_lm_at = _parse_http_date(new_last_modified)
            if (
                new_lm_at is not None
                and cache.last_modified_at is not None
                and not new_lm_at > cache.last_modified_at
            ):
                log.warning(
                    "CDN returned 200 but Last-Modified (%s) is not later than stored (%s);"
                    " treating as 304 (suspected stale node)",
                    new_last_modified,
                    cache.last_modified_header,
                )
                return FetchResult(304, None, cache.etag, cache.last_modified_header)
            self._cache = _CacheState(
                etag=new_etag,
                last_modified_header=new_last_modified,
                last_modified_at=new_lm_at,
            )

        return FetchResult(resp.status_code, body, new_etag, new_last_modified)


def _parse_http_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return parsedate_to_datetime(value.strip())
    except Exception:
        log.warning("unparseable HTTP-date %r", value)
        return None


def _parse_retry_after_ms(retry_after: str | None, now: float) -> float:
    if not retry_after or not retry_after.strip():
        return _DEFAULT_RETRY_AFTER_MS
    try:
        return float(retry_after.strip()) * 1000.0
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(retry_after.strip())
        return max((dt.timestamp() - now) * 1000.0, 0.0)
    except Exception:
        log.debug("unparseable Retry-After %r; using default", retry_after)
    return _DEFAULT_RETRY_AFTER_MS
