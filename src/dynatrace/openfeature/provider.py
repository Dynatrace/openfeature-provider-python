from __future__ import annotations

import logging
import threading
from collections.abc import Mapping, Sequence
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from urllib.parse import urlsplit

import requests

from openfeature.contrib.tools.flagd.core import FlagdCore
from openfeature.evaluation_context import EvaluationContext
from openfeature.event import ProviderEventDetails
from openfeature.exception import GeneralError
from openfeature.flag_evaluation import FlagResolutionDetails, FlagValueType
from openfeature.hook import Hook
from openfeature.provider import AbstractProvider, Metadata, ProviderStatus

from ._cdn import MAX_FETCH_DURATION_S as _MAX_FETCH_DURATION_S
from ._cdn import CdnFetcher
from ._cdn_config import extract_sse_url
from ._metrics_aggregator import (
    HARD_CAP_DISTINCT_FLAGS,
    SOFT_CAP_DISTINCT_FLAGS,
    DynatraceMetricsAggregator,
)
from ._metrics_flusher import EvaluationMetricsFlusher
from ._metrics_hook import DynatraceMetricsHook
from ._polling import PollingManager
from ._sse import SseManager
from ._sse_message import TYPE_REFETCH_CONFIG, SseMessage
from .options import DynatraceProviderOptions

log = logging.getLogger(__name__)

_PROVIDER_NAME = "dynatrace"
_POLL_INTERVAL_S: float = 10.0
#: poll cadence while SSE is connected: SSE delivers notifications, polling is a safety net
_SSE_CONNECTED_POLL_INTERVAL_S: float = 600.0
_GRACE_PERIOD_S: float = 60.0
_METRICS_FLUSH_INTERVAL_S: float = 30.0
_VERSION_METADATA_KEY = "version"


def _resolve_sdk_version() -> str:
    try:
        return _pkg_version("dynatrace-openfeature-provider-python")
    except PackageNotFoundError:
        return "0.0.0"


class DynatraceProvider(AbstractProvider):
    def __init__(  # noqa: PLR0913 - public constructor with optional injectable seams
        self,
        sdk_key: str,
        *,
        config_origin: str | None = None,
        metrics_origin: str | None = None,
        flagd_core: FlagdCore | None = None,
        fetcher: CdnFetcher | None = None,
        metrics_flusher: EvaluationMetricsFlusher | None = None,
        poll_interval_s: float = _POLL_INTERVAL_S,
        sse_connected_poll_interval_s: float = _SSE_CONNECTED_POLL_INTERVAL_S,
        disable_sse: bool = False,
        grace_period_s: float = _GRACE_PERIOD_S,
        metrics_flush_interval_s: float = _METRICS_FLUSH_INTERVAL_S,
    ) -> None:
        options = DynatraceProviderOptions.from_sdk_key(
            sdk_key, config_origin=config_origin, metrics_origin=metrics_origin
        )
        # providers.md 2.1: a poll must not outlive its interval; the bound is the whole fetch, not one request
        if fetcher is None:
            for name, interval in (
                ("poll_interval_s", poll_interval_s),
                ("sse_connected_poll_interval_s", sse_connected_poll_interval_s),
            ):
                if interval <= _MAX_FETCH_DURATION_S:
                    raise ValueError(
                        f"{name} ({interval}s) must be greater than the "
                        f"maximum fetch duration ({_MAX_FETCH_DURATION_S:.2f}s)"
                    )
        if metrics_flush_interval_s <= 0:
            raise ValueError(
                f"metrics_flush_interval_s ({metrics_flush_interval_s}s) must be positive"
            )
        self._options = options
        self._flagd_core = flagd_core or FlagdCore()
        self._fetcher = fetcher or CdnFetcher(options)
        self._poll_interval_s = poll_interval_s
        self._sse_connected_poll_interval_s = sse_connected_poll_interval_s
        self._sse_enabled = not disable_sse
        self._grace_period_s = grace_period_s
        self._poller: PollingManager | None = None
        self._sse_manager: SseManager | None = None
        # true while the SSE stream is open, i.e. the provider is in SSE mode rather than polling mode
        self._sse_connected = False
        self._grace_timer: threading.Timer | None = None
        self._state = ProviderStatus.NOT_READY
        self._lock = threading.RLock()
        # bumped by initialize/shutdown so a slow init can detect it was superseded
        self._generation = 0

        self._metrics_aggregator = DynatraceMetricsAggregator(HARD_CAP_DISTINCT_FLAGS)
        self._metrics_flusher = metrics_flusher or EvaluationMetricsFlusher(
            self._metrics_aggregator,
            sdk_key=sdk_key.strip(),
            metrics_url=options.metrics_url,
            flush_interval_s=metrics_flush_interval_s,
            capacity_threshold=SOFT_CAP_DISTINCT_FLAGS,
            sdk_version=_resolve_sdk_version(),
            provider_name=_PROVIDER_NAME,
        )
        self._metrics_hook = DynatraceMetricsHook(
            self._metrics_aggregator, on_record=self._metrics_flusher.on_record
        )

    def get_metadata(self) -> Metadata:
        return Metadata(name=_PROVIDER_NAME)

    def get_provider_hooks(self) -> list[Hook]:
        return [self._metrics_hook]

    def initialize(self, evaluation_context: EvaluationContext | None = None) -> None:
        log.info("initializing Dynatrace provider (key=%s)", self._options.public_key)
        if not self._sse_enabled:
            log.debug("SSE disabled by configuration; running in polling-only mode")
        with self._lock:
            self._generation += 1
            generation = self._generation

        body = self._fetch_initial_body(generation)

        try:
            self._flagd_core.set_flags(body)
        except Exception as exc:
            # not just ParseError: a non-JSON body raises JSONDecodeError, which must still leave a recoverable provider
            self._fail_initialization(generation, f"invalid config: {exc}")
            raise GeneralError(f"initial CDN config is invalid: {exc}") from exc

        # set the aggregator version eagerly so it is authoritative before any evaluation is recorded
        self._update_metrics_version_from_flag_set()

        with self._lock:
            if self._generation != generation:
                # a concurrent shutdown/re-initialize superseded us mid-fetch; do not resurrect the provider
                log.info(
                    "initialize superseded by a concurrent shutdown/initialize; "
                    "not starting poller"
                )
                return
            self._cancel_grace_timer()
            self._state = ProviderStatus.READY
            self._start_sse_locked(body)
            self._start_poller_locked()
        self._metrics_flusher.start()
        log.info("Dynatrace provider initialized successfully")

    def _fetch_initial_body(self, generation: int) -> str:
        """Fetch the startup config, or fail initialization and raise.

        Bounded like any other fetch (providers.md section 2.4). Failing is not
        terminal: the provider enters ERROR and the polling timer carries it to
        READY if the failure was transient.
        """
        try:
            result = self._fetcher.fetch()
        except (OSError, requests.RequestException) as exc:
            self._fail_initialization(generation, f"fetch failed: {type(exc).__name__}")
            raise GeneralError(
                f"initial CDN fetch failed: {type(exc).__name__}"
            ) from exc

        if result.skipped:
            # No request was made, so there is no status to report.
            self._fail_initialization(generation, "fetch skipped")
            raise GeneralError("initial CDN fetch was skipped; no request was made")

        if not result.is_success:
            self._fail_initialization(generation, f"HTTP {result.status}")
            raise GeneralError(f"initial CDN fetch returned HTTP {result.status}")

        body = result.body
        if not body or not body.strip():
            self._fail_initialization(generation, "empty response body")
            raise GeneralError("initial CDN response body was empty")

        return body

    def _start_poller_locked(self) -> None:
        """Replace any existing poller with a fresh one. Caller holds ``_lock``."""
        if self._poller is not None:
            self._poller.stop()
        self._poller = PollingManager(
            fetch_flags=self._fetcher.fetch,
            store_flags=self._store_flags,
            emit_change=self._on_configuration_changed,
            emit_success=self._on_fetch_success,
            emit_failure=self._on_fetch_failure,
            interval_s=self._sse_connected_poll_interval_s
            if self._sse_connected
            else self._poll_interval_s,
        )
        self._poller.start()

    def _start_sse_locked(self, body: str) -> None:
        """Open the SSE stream advertised in ``body``, if SSE is enabled. Caller holds ``_lock``."""
        if not self._sse_enabled or self._sse_manager is not None:
            return
        cdn = urlsplit(self._options.cdn_url)
        sse_url = extract_sse_url(body, f"{cdn.scheme}://{cdn.netloc}")
        if sse_url is None:
            # the server always sends eventStreams, so this is not worth logging
            return

        manager: SseManager | None = None

        def on_connected() -> None:
            self._on_sse_connected(manager)

        def on_disconnected() -> None:
            self._on_sse_disconnected(manager)

        def on_message(msg: SseMessage) -> None:
            self._on_sse_message(manager, msg)

        manager = SseManager(sse_url, on_connected, on_disconnected, on_message)
        try:
            manager.start()
        except Exception as exc:
            log.warning(
                "failed to start SSE connection; falling back to polling-only mode: %s",
                type(exc).__name__,
            )
            return
        self._sse_manager = manager

    def _stop_sse_locked(self) -> None:
        """Close the SSE stream and forget its state. Caller holds ``_lock``."""
        self._sse_connected = False
        if self._sse_manager is not None:
            self._sse_manager.stop()
            self._sse_manager = None

    def _fail_initialization(self, generation: int, reason: str) -> None:
        """Enter ERROR and start polling so the provider can still recover.

        The caller raises afterwards, so initialization is still seen to fail;
        the polling timer is what brings the provider back (section 2.4). Auth
        failures take this path too, since 401/403 are ERROR and never FATAL.
        """
        with self._lock:
            if self._generation != generation:
                # A newer generation owns the fetcher; its cache is not ours.
                log.info(
                    "initialize superseded by a concurrent shutdown/initialize; "
                    "not starting recovery poller"
                )
                return
            # nothing was applied, so cached validators would let the recovery poll 304 its way to READY over an empty store
            self._fetcher.invalidate_cache()
            log.warning("initialization failed (%s); polling for recovery", reason)
            self._state = ProviderStatus.ERROR
            self._cancel_grace_timer()
            self._start_poller_locked()
        self._metrics_flusher.start()

    def shutdown(self) -> None:
        with self._lock:
            # always bump the generation, even when idle, so an in-flight initialize detects it was superseded
            self._generation += 1
            already_idle = (
                self._state == ProviderStatus.NOT_READY and self._poller is None
            )
            if not already_idle:
                log.info("shutting down Dynatrace provider")
                # providers.md 6.1: close SSE first, so no notification triggers a fetch mid-shutdown
                self._stop_sse_locked()
                self._state = ProviderStatus.NOT_READY
                self._cancel_grace_timer()
                if self._poller is not None:
                    self._poller.stop()
                    self._poller = None
        # always release the flusher and its session (created in __init__), even if init never finished; shutdown() is idempotent
        try:
            self._metrics_flusher.shutdown()
        except Exception as exc:
            log.warning("metrics flusher shutdown failed: %r", exc)
        if already_idle:
            return
        super().shutdown()
        log.info("Dynatrace provider shut down")

    def resolve_boolean_details(
        self,
        flag_key: str,
        default_value: bool,
        evaluation_context: EvaluationContext | None = None,
    ) -> FlagResolutionDetails[bool]:
        return self._flagd_core.resolve_boolean_value(
            flag_key, default_value, evaluation_context
        )

    def resolve_string_details(
        self,
        flag_key: str,
        default_value: str,
        evaluation_context: EvaluationContext | None = None,
    ) -> FlagResolutionDetails[str]:
        return self._flagd_core.resolve_string_value(
            flag_key, default_value, evaluation_context
        )

    def resolve_integer_details(
        self,
        flag_key: str,
        default_value: int,
        evaluation_context: EvaluationContext | None = None,
    ) -> FlagResolutionDetails[int]:
        return self._flagd_core.resolve_integer_value(
            flag_key, default_value, evaluation_context
        )

    def resolve_float_details(
        self,
        flag_key: str,
        default_value: float,
        evaluation_context: EvaluationContext | None = None,
    ) -> FlagResolutionDetails[float]:
        return self._flagd_core.resolve_float_value(
            flag_key, default_value, evaluation_context
        )

    def resolve_object_details(
        self,
        flag_key: str,
        default_value: Sequence[FlagValueType] | Mapping[str, FlagValueType],
        evaluation_context: EvaluationContext | None = None,
    ) -> FlagResolutionDetails[Sequence[FlagValueType] | Mapping[str, FlagValueType]]:
        return self._flagd_core.resolve_object_value(
            flag_key, default_value, evaluation_context
        )

    def _store_flags(self, body: str) -> list[str]:
        # seal + enqueue + flush the old-version batch before applying the new config, so pre-rotation counts flush under the old version
        sealed = self._metrics_aggregator.drain()
        self._metrics_flusher.enqueue_sealed_batch(sealed)
        self._metrics_flusher.trigger_flush()
        try:
            changed = self._flagd_core.set_flags_and_get_changed_keys(body)
        except Exception:
            # fetched but not applied, so its validators would make the next poll a 304 over the previous config
            self._fetcher.invalidate_cache()
            raise
        self._update_metrics_version_from_flag_set()
        with self._lock:
            # covers the failed-init case, where there was no config to read an SSE URL from; a shut-down provider has no poller
            if self._poller is not None:
                self._start_sse_locked(body)
        return changed

    def _update_metrics_version_from_flag_set(self) -> None:
        version = self._flagd_core.get_flag_set_metadata().get(_VERSION_METADATA_KEY)
        if isinstance(version, str) and version.strip():
            self._metrics_aggregator.set_version(version)

    def _on_fetch_success(self) -> None:
        with self._lock:
            if self._state == ProviderStatus.NOT_READY:
                return
            self._cancel_grace_timer()
            if self._state in (ProviderStatus.STALE, ProviderStatus.ERROR):
                prev_state = self._state
                self._state = ProviderStatus.READY
                log.info("recovered from %s to READY", prev_state)
                self.emit_provider_ready(ProviderEventDetails(message="recovered"))

    def _on_fetch_failure(self, reason: str) -> None:
        with self._lock:
            if self._state == ProviderStatus.NOT_READY:
                return
            if self._state == ProviderStatus.READY:
                self._state = ProviderStatus.STALE
                log.warning("poll failed, transitioning to STALE: %s", reason)
                self.emit_provider_stale(ProviderEventDetails(message=reason))
                self._start_grace_timer()
            else:
                log.warning("poll failed (state=%s): %s", self._state, reason)

    def _on_configuration_changed(self, changed_keys: list[str]) -> None:
        with self._lock:
            if self._state == ProviderStatus.NOT_READY:
                return
            self.emit_provider_configuration_changed(
                ProviderEventDetails(flags_changed=changed_keys)
            )

    # SSE callbacks: each names the manager it came from, so a stream already closed by shutdown cannot act.
    # SSE only prompts fetches, so its state picks the poll cadence and never touches READY/STALE/ERROR.

    def _on_sse_connected(self, source: SseManager | None) -> None:
        """Switch to SSE mode: safety-net cadence plus a catch-up fetch for anything missed while down."""
        with self._lock:
            if source is None or source is not self._sse_manager:
                return
            self._sse_connected = True
            poller = self._poller
            if poller is None:
                return
            poller.set_interval(self._sse_connected_poll_interval_s)
            log.info(
                "SSE connected; poll interval switched to %ss",
                self._sse_connected_poll_interval_s,
            )
            # no announced lastModified, so a deferred catch-up is never dropped
            poller.trigger_immediate_fetch(None)

    def _on_sse_disconnected(self, source: SseManager | None) -> None:
        """Fall back to polling mode; fetch failures from here on are handled like any poll failure."""
        with self._lock:
            if source is None or source is not self._sse_manager:
                return
            if not self._sse_connected:
                # never connected, or a repeat fault while the library retries
                return
            self._sse_connected = False
            log.warning(
                "SSE disconnected; falling back to polling every %ss",
                self._poll_interval_s,
            )
            if self._poller is not None:
                self._poller.set_interval(self._poll_interval_s)

    def _on_sse_message(self, source: SseManager | None, msg: SseMessage) -> None:
        """Trigger a conditional re-fetch on ``refetchConfig``; ignore every other type.
        lastModified is forwarded only so a deferred re-fetch can be dropped (providers.md 2.1.1)."""
        if msg.type != TYPE_REFETCH_CONFIG:
            log.debug(
                "ignoring SSE message of type %r (server provider only handles %r)",
                msg.type,
                TYPE_REFETCH_CONFIG,
            )
            return
        with self._lock:
            if source is None or source is not self._sse_manager:
                return
            poller = self._poller
        log.debug(
            "SSE refetchConfig received; triggering CDN re-fetch (lastModified=%s)",
            msg.last_modified,
        )
        if poller is not None:
            poller.trigger_immediate_fetch(msg.last_modified)

    def _start_grace_timer(self) -> None:
        self._cancel_grace_timer()
        t = threading.Timer(self._grace_period_s, self._grace_expired)
        t.daemon = True
        t.name = "dt-provider-grace-timer"
        t.start()
        self._grace_timer = t

    def _cancel_grace_timer(self) -> None:
        if self._grace_timer is not None:
            self._grace_timer.cancel()
            self._grace_timer = None

    def _grace_expired(self) -> None:
        with self._lock:
            if self._state != ProviderStatus.STALE:
                return
            self._state = ProviderStatus.ERROR
            log.error("grace period expired, transitioning to ERROR")
            self.emit_provider_error(
                ProviderEventDetails(message="grace period expired; CDN unreachable")
            )
