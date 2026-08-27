from __future__ import annotations

import logging
import threading
from collections.abc import Mapping, Sequence
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

import requests

from openfeature.contrib.tools.flagd.core import FlagdCore
from openfeature.evaluation_context import EvaluationContext
from openfeature.event import ProviderEventDetails
from openfeature.exception import GeneralError, ParseError
from openfeature.flag_evaluation import FlagResolutionDetails, FlagValueType
from openfeature.hook import Hook
from openfeature.provider import AbstractProvider, Metadata, ProviderStatus

from ._cdn import REQUEST_TIMEOUT_S as _REQUEST_TIMEOUT_S
from ._cdn import CdnFetcher
from ._metrics_aggregator import (
    HARD_CAP_DISTINCT_FLAGS,
    SOFT_CAP_DISTINCT_FLAGS,
    DynatraceMetricsAggregator,
)
from ._metrics_flusher import EvaluationMetricsFlusher
from ._metrics_hook import DynatraceMetricsHook
from ._polling import PollingManager
from .options import DynatraceProviderOptions

log = logging.getLogger(__name__)

_PROVIDER_NAME = "dynatrace"
_POLL_INTERVAL_S: float = 10.0
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
        grace_period_s: float = _GRACE_PERIOD_S,
        metrics_flush_interval_s: float = _METRICS_FLUSH_INTERVAL_S,
    ) -> None:
        options = DynatraceProviderOptions.from_sdk_key(
            sdk_key, config_origin=config_origin, metrics_origin=metrics_origin
        )
        if fetcher is None and poll_interval_s <= _REQUEST_TIMEOUT_S:
            raise ValueError(
                f"poll_interval_s ({poll_interval_s}s) must be greater than the "
                f"request timeout ({_REQUEST_TIMEOUT_S}s)"
            )
        if metrics_flush_interval_s <= 0:
            raise ValueError(
                f"metrics_flush_interval_s ({metrics_flush_interval_s}s) must be positive"
            )
        self._options = options
        self._flagd_core = flagd_core or FlagdCore()
        self._fetcher = fetcher or CdnFetcher(options)
        self._poll_interval_s = poll_interval_s
        self._grace_period_s = grace_period_s
        self._poller: PollingManager | None = None
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
        with self._lock:
            self._generation += 1
            generation = self._generation

        try:
            result = self._fetcher.fetch()
        except (OSError, requests.RequestException) as exc:
            raise GeneralError(
                f"initial CDN fetch failed: {type(exc).__name__}"
            ) from exc

        if not result.is_success:
            raise GeneralError(f"initial CDN fetch returned HTTP {result.status}")

        body = result.body
        if not body or not body.strip():
            raise GeneralError("initial CDN response body was empty")

        try:
            self._flagd_core.set_flags(body)
        except ParseError as exc:
            raise GeneralError(f"initial CDN config is invalid: {exc}") from exc

        # Set the aggregator version eagerly so it is authoritative before any
        # evaluation is recorded.
        self._update_metrics_version_from_flag_set()

        with self._lock:
            if self._generation != generation:
                # A concurrent shutdown() or re-initialize() superseded us while
                # the fetch was in flight; do not resurrect the provider.
                log.info(
                    "initialize superseded by a concurrent shutdown/initialize; "
                    "not starting poller"
                )
                return
            self._cancel_grace_timer()
            if self._poller is not None:
                self._poller.stop()
            self._state = ProviderStatus.READY
            self._poller = PollingManager(
                fetch_flags=self._fetcher.fetch,
                store_flags=self._store_flags,
                emit_change=self._on_configuration_changed,
                emit_success=self._on_fetch_success,
                emit_failure=self._on_fetch_failure,
                interval_s=self._poll_interval_s,
            )
            self._poller.start()
        self._metrics_flusher.start()
        log.info("Dynatrace provider initialized successfully")

    def shutdown(self) -> None:
        with self._lock:
            # Always bump the generation, even when already idle, so an
            # in-flight initialize() detects it was superseded and aborts.
            self._generation += 1
            already_idle = (
                self._state == ProviderStatus.NOT_READY and self._poller is None
            )
            if not already_idle:
                log.info("shutting down Dynatrace provider")
                self._state = ProviderStatus.NOT_READY
                self._cancel_grace_timer()
                if self._poller is not None:
                    self._poller.stop()
                    self._poller = None
        # Always release the flusher (and the session it owns), even when the
        # provider never finished initializing: the flusher's session is created
        # in __init__, so an early return here would otherwise leak it.
        # flusher.shutdown() is idempotent and safe when never started.
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
        # Seal the old-version batch and enqueue/trigger its flush BEFORE applying
        # the new config and bumping the version, so pre-rotation counts are
        # flushed under the old version.
        sealed = self._metrics_aggregator.drain()
        self._metrics_flusher.enqueue_sealed_batch(sealed)
        self._metrics_flusher.trigger_flush()
        changed = self._flagd_core.set_flags_and_get_changed_keys(body)
        self._update_metrics_version_from_flag_set()
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
