from __future__ import annotations

import logging
import threading
from collections.abc import Mapping, Sequence

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
from ._polling import PollingManager
from .options import DynatraceProviderOptions

log = logging.getLogger(__name__)

_PROVIDER_NAME = 'dynatrace'
_POLL_INTERVAL_S: float = 10.0
_GRACE_PERIOD_S: float = 60.0


class DynatraceProvider(AbstractProvider):
    def __init__(
        self,
        sdk_key: str,
        *,
        cdn_base_url: str | None = None,
        flagd_core: FlagdCore | None = None,
        fetcher: CdnFetcher | None = None,
        poll_interval_s: float = _POLL_INTERVAL_S,
        grace_period_s: float = _GRACE_PERIOD_S,
    ) -> None:
        options = DynatraceProviderOptions.from_sdk_key(sdk_key, cdn_base_url=cdn_base_url)
        if fetcher is None and poll_interval_s <= _REQUEST_TIMEOUT_S:
            raise ValueError(
                f"poll_interval_s ({poll_interval_s}s) must be greater than the "
                f"request timeout ({_REQUEST_TIMEOUT_S}s)"
            )
        self._options = options
        self._flagd_core = flagd_core or FlagdCore()
        self._fetcher = fetcher or CdnFetcher(options)
        self._poll_interval_s = poll_interval_s
        self._grace_period_s = grace_period_s
        self._poller: PollingManager | None = None
        self._grace_timer: threading.Timer | None = None
        self._state = ProviderStatus.NOT_READY
        self._state_lock = threading.Lock()
        self._shutdown_lock = threading.Lock()
        self._shutting_down = False

    def get_metadata(self) -> Metadata:
        return Metadata(name=_PROVIDER_NAME)

    def get_provider_hooks(self) -> list[Hook]:
        return []

    def initialize(self, evaluation_context: EvaluationContext | None = None) -> None:
        with self._shutdown_lock:
            self._shutting_down = False
        log.info('initializing Dynatrace provider (key=%s)', self._options.public_key)
        try:
            result = self._fetcher.fetch()
        except (OSError, requests.RequestException) as exc:
            raise GeneralError(f'initial CDN fetch failed: {type(exc).__name__}') from exc

        if not result.is_success:
            raise GeneralError(f'initial CDN fetch returned HTTP {result.status}')

        body = result.body
        if not body or not body.strip():
            raise GeneralError('initial CDN response body was empty')

        try:
            self._flagd_core.set_flags(body)
        except ParseError as exc:
            raise GeneralError(f'initial CDN config is invalid: {exc}') from exc

        with self._state_lock:
            self._state = ProviderStatus.READY

        if self._poller is not None:
            self._poller.stop()
            self._poller = None

        self._poller = PollingManager(
            fetch_flags=self._fetcher.fetch,
            store_flags=self._store_flags,
            emit_change=self._on_configuration_changed,
            emit_success=self._on_fetch_success,
            emit_failure=self._on_fetch_failure,
            interval_s=self._poll_interval_s,
        )
        self._poller.start()
        log.info('Dynatrace provider initialized successfully')

    def shutdown(self) -> None:
        with self._shutdown_lock:
            if self._shutting_down:
                return
            self._shutting_down = True
        log.info('shutting down Dynatrace provider')
        if self._poller is not None:
            self._poller.stop()
        self._cancel_grace_timer()
        with self._state_lock:
            self._state = ProviderStatus.NOT_READY
        super().shutdown()
        log.info('Dynatrace provider shut down')

    def resolve_boolean_details(
        self,
        flag_key: str,
        default_value: bool,
        evaluation_context: EvaluationContext | None = None,
    ) -> FlagResolutionDetails[bool]:
        return self._flagd_core.resolve_boolean_value(flag_key, default_value, evaluation_context)

    def resolve_string_details(
        self,
        flag_key: str,
        default_value: str,
        evaluation_context: EvaluationContext | None = None,
    ) -> FlagResolutionDetails[str]:
        return self._flagd_core.resolve_string_value(flag_key, default_value, evaluation_context)

    def resolve_integer_details(
        self,
        flag_key: str,
        default_value: int,
        evaluation_context: EvaluationContext | None = None,
    ) -> FlagResolutionDetails[int]:
        return self._flagd_core.resolve_integer_value(flag_key, default_value, evaluation_context)

    def resolve_float_details(
        self,
        flag_key: str,
        default_value: float,
        evaluation_context: EvaluationContext | None = None,
    ) -> FlagResolutionDetails[float]:
        return self._flagd_core.resolve_float_value(flag_key, default_value, evaluation_context)

    def resolve_object_details(
        self,
        flag_key: str,
        default_value: Sequence[FlagValueType] | Mapping[str, FlagValueType],
        evaluation_context: EvaluationContext | None = None,
    ) -> FlagResolutionDetails[Sequence[FlagValueType] | Mapping[str, FlagValueType]]:
        return self._flagd_core.resolve_object_value(flag_key, default_value, evaluation_context)

    def _store_flags(self, body: str) -> list[str]:
        return self._flagd_core.set_flags_and_get_changed_keys(body)

    def _on_fetch_success(self) -> None:
        with self._state_lock:
            self._cancel_grace_timer()
            if self._state in (ProviderStatus.STALE, ProviderStatus.ERROR):
                prev_state = self._state
                self._state = ProviderStatus.READY
                log.info('recovered from %s to READY', prev_state)
                self.emit_provider_ready(ProviderEventDetails(message='recovered'))

    def _on_fetch_failure(self, reason: str) -> None:
        with self._state_lock:
            if self._state == ProviderStatus.READY:
                self._state = ProviderStatus.STALE
                log.warning('poll failed, transitioning to STALE: %s', reason)
                self.emit_provider_stale(ProviderEventDetails(message=reason))
                self._start_grace_timer()
            else:
                log.warning('poll failed (state=%s): %s', self._state, reason)

    def _on_configuration_changed(self, changed_keys: list[str]) -> None:
        self.emit_provider_configuration_changed(
            ProviderEventDetails(flags_changed=changed_keys)
        )

    def _start_grace_timer(self) -> None:
        self._cancel_grace_timer()
        t = threading.Timer(self._grace_period_s, self._grace_expired)
        t.daemon = True
        t.name = 'dt-provider-grace-timer'
        t.start()
        self._grace_timer = t

    def _cancel_grace_timer(self) -> None:
        if self._grace_timer is not None:
            self._grace_timer.cancel()
            self._grace_timer = None

    def _grace_expired(self) -> None:
        with self._state_lock:
            if self._state == ProviderStatus.STALE:
                self._state = ProviderStatus.ERROR
                log.error('grace period expired, transitioning to ERROR')
                self.emit_provider_error(
                    ProviderEventDetails(message='grace period expired; CDN unreachable')
                )
