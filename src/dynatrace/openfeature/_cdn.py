from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime

import requests
from urllib3.util import Timeout as Urllib3Timeout

from ._retry import (
    MAX_ATTEMPTS,
    MAX_BACKOFF_S,
    RetryStrategy,
    default_should_retry_status,
)
from .options import DynatraceProviderOptions

log = logging.getLogger(__name__)

REQUEST_TIMEOUT_S: float = 4.0

#: worst-case wall time of a fetch (all attempts + backoff); stays under the shortest poll interval so two polls never overlap (providers.md 2.1); asserted in DynatraceProvider
MAX_FETCH_DURATION_S: float = REQUEST_TIMEOUT_S * MAX_ATTEMPTS + MAX_BACKOFF_S

_DEFAULT_RETRY_AFTER_MS: float = 60_000.0
_BODY_CHUNK_BYTES = 64 * 1024
_BUDGET_EXCEEDED = "response body exceeded the fetch budget"


@dataclass
class FetchResult:
    status: int
    body: str | None
    etag: str | None
    last_modified: str | None
    skipped: bool = False

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


def _should_retry_fetch(result: FetchResult) -> bool:
    return default_should_retry_status(result.status)


class CdnFetcher:
    def __init__(
        self,
        options: DynatraceProviderOptions,
        retry_strategy: RetryStrategy | None = None,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._url = options.cdn_url
        self._retry = retry_strategy or RetryStrategy()
        self._session = requests.Session()
        self._clock: Callable[[], float] = clock or time.time
        self._cache = _CacheState()
        self._rate_limited_until: float = 0.0
        self._fetch_lock = threading.RLock()

    def is_rate_limited(self, now: float | None = None) -> bool:
        t = now if now is not None else self._clock()
        return t < self._rate_limited_until

    def invalidate_cache(self) -> None:
        """Forget the cached validators.

        They are stored as soon as a 2xx arrives, before the caller applies the
        body. If applying it fails they describe a config we never loaded, and
        the next conditional request would come back 304 over an empty store.
        """
        with self._fetch_lock:
            self._cache = _CacheState()

    def fetch(self, cancel: threading.Event | None = None) -> FetchResult:
        """Fetch the config, or skip without making a request.

        A skip (fetch in flight, section 2.1.1; or an open Retry-After window,
        section 2.3) is neither success nor failure and must not be reported as
        either. PollingManager serialises its own fetches, so the in-flight
        guard is for callers outside it -- initialize() fetches on the caller's
        thread -- and it keeps a conditional request from storing an ETag and a
        Last-Modified that came from different responses.
        """
        if self.is_rate_limited():
            log.debug("rate limited; skipping fetch")
            return FetchResult(0, None, None, None, skipped=True)
        if not self._fetch_lock.acquire(blocking=False):
            log.debug("fetch already in progress; skipping")
            return FetchResult(0, None, None, None, skipped=True)
        try:
            return self._retry.execute(
                self._do_fetch,
                should_retry=_should_retry_fetch,
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

        deadline = time.monotonic() + REQUEST_TIMEOUT_S
        # total bounds connect + time-to-first-byte as one budget (a tuple would give each phase the full timeout)
        timeout = Urllib3Timeout(
            connect=REQUEST_TIMEOUT_S,
            read=REQUEST_TIMEOUT_S,
            total=REQUEST_TIMEOUT_S,
        )
        with self._session.get(
            self._url,
            headers=headers,
            timeout=timeout,  # type: ignore[arg-type]
            # each hop gets a fresh timeout, so following a chain would blow the budget (Java never follows one either)
            allow_redirects=False,
            stream=True,
        ) as resp:
            if resp.status_code == 304:
                return FetchResult(304, None, cache.etag, cache.last_modified_header)

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                delay_ms = _parse_retry_after_ms(retry_after, self._clock())
                self._rate_limited_until = self._clock() + delay_ms / 1000.0
                log.warning("rate limited (429); pausing fetches for %.0fms", delay_ms)
                return FetchResult(429, None, None, None)

            new_etag = resp.headers.get("ETag")
            new_last_modified = resp.headers.get("Last-Modified")
            body = _read_body_within(resp, deadline)

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


def _read_body_within(resp: requests.Response, deadline: float) -> str:
    """Read the response body, giving up once ``deadline`` passes.

    No timeout in the stack bounds this: they all measure inactivity between
    socket reads, so a server trickling bytes resets the clock on every one and
    MAX_FETCH_DURATION_S stops being a bound. Checking the clock between chunks
    is not enough either, since a read blocks until its chunk is full, so the
    deadline interrupts the read itself via HTTPResponse.shutdown.
    """
    raw = resp.raw
    expired = threading.Event()

    def _abort() -> None:
        expired.set()
        try:
            raw.shutdown()
        except Exception:  # connection already released, or nothing to interrupt
            log.debug("could not shut down the response socket", exc_info=True)

    watchdog = threading.Timer(max(0.0, deadline - time.monotonic()), _abort)
    watchdog.daemon = True
    watchdog.start()
    chunks: list[bytes] = []
    try:
        chunks.extend(resp.iter_content(_BODY_CHUNK_BYTES))
    except (requests.RequestException, OSError):
        # A torn-down socket surfaces as a read error.
        if expired.is_set():
            raise requests.Timeout(_BUDGET_EXCEEDED) from None
        raise
    finally:
        watchdog.cancel()

    if expired.is_set():
        # Arrived, just not in time.
        raise requests.Timeout(_BUDGET_EXCEEDED)

    return b"".join(chunks).decode(resp.encoding or "utf-8", errors="replace")


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
        log.warning("unparseable Retry-After %r; using default", retry_after)
    return _DEFAULT_RETRY_AFTER_MS
