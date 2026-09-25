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

#: Worst case wall time of a single fetch: every attempt timing out, plus the
#: backoff between them. providers.md section 2.1 makes it an invariant that this
#: stays below the shortest poll interval, so two polls can never be in flight at
#: once. Any change to the timeout, the attempt count or the backoff must
#: preserve it; :class:`DynatraceProvider` asserts it at construction.
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

        The validators are stored as soon as a 2xx arrives, before the caller
        has had a chance to apply the body. When applying it fails, the cached
        ETag/Last-Modified describe a config the provider never loaded, and the
        next conditional request would come back 304 -- which reads as "your
        copy is current" and would quietly confirm an empty flag store. Dropping
        them forces the next fetch to ask for the full body again.
        """
        self._cache = _CacheState()

    def fetch(self, cancel: threading.Event | None = None) -> FetchResult:
        # No request is made inside the Retry-After window, so this is a skip
        # like lock contention and not a 429 from the CDN: the caller learned
        # nothing and must report neither success nor failure.
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
        # urllib3's total covers connect plus time-to-first-byte. A plain float
        # or (connect, read) tuple would let those two phases each take the full
        # timeout, doubling the worst case. HTTPAdapter.send hands a urllib3
        # Timeout straight through; only requests' type stubs are unaware.
        timeout = Urllib3Timeout(
            connect=REQUEST_TIMEOUT_S,
            read=REQUEST_TIMEOUT_S,
            total=REQUEST_TIMEOUT_S,
        )
        with self._session.get(
            self._url,
            headers=headers,
            timeout=timeout,  # type: ignore[arg-type]
            # The body is read against the same deadline below; see
            # _read_body_within for why the library cannot bound it for us.
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

    No timeout the HTTP stack offers bounds this. A float or (connect, read)
    timeout measures inactivity between socket reads, and urllib3's ``total``
    only subtracts the connect duration before being applied as the socket's
    per-read timeout. A server that keeps trickling bytes resets the clock on
    every one of them, so the fetch runs as long as it likes. That would make
    MAX_FETCH_DURATION_S a description rather than a bound, and the bound is
    what keeps two polls from ever being in flight at once.

    Chunking and checking the clock between chunks is not enough either: a read
    blocks until the full chunk arrives. The deadline has to interrupt the read
    itself, which is what :meth:`urllib3.HTTPResponse.shutdown` is for.
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
        # A torn-down socket surfaces as a read error; report it as what it is.
        if expired.is_set():
            raise requests.Timeout(_BUDGET_EXCEEDED) from None
        raise
    finally:
        watchdog.cancel()

    if expired.is_set():
        # The body did arrive, just not inside the budget.
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
