"""Step definitions for the shared CDN gherkin spec (fm-provider-test-harness/gherkin/cdn.feature).

Black-box: each scenario binds a fresh provider to its own OpenFeature domain and
observes it only through the SDK client, against the containerized mock server.
"""

from __future__ import annotations

import itertools
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http import HTTPStatus

import pytest
from mock_server import (
    HARNESS_DIR,
    CdnResponse,
    MockServer,
    RecordedRequest,
    build_cdn_response,
)
from openfeature import api
from openfeature.client import OpenFeatureClient
from openfeature.exception import ErrorCode
from openfeature.provider import ProviderStatus
from pytest_bdd import parsers, scenarios, step

from dynatrace.openfeature import DynatraceProvider
from dynatrace.openfeature._cdn import REQUEST_TIMEOUT_S
from dynatrace.openfeature.provider import _POLL_INTERVAL_S

# well-formed server SDK key used by arranged providers
BASE_KEY = "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
# shared flag configuration fixtures from the harness submodule
FLAGS_DIR = HARNESS_DIR / "flags"
# read from the provider, so scenarios assert against the real cadence
POLL_INTERVAL = _POLL_INTERVAL_S
# bound on one CDN attempt
REQUEST_TIMEOUT = REQUEST_TIMEOUT_S
# longer than the poll cadence, so a refresh is observed
POLL_TIMEOUT = POLL_INTERVAL + 1
# covers the time taken by all retries for 5xx codes
RETRY_TIMEOUT = 2.0
# window over which a "continues to" assertion must keep holding
STABLE_WINDOW = 2.0
# time a state change may take to arrive, since state rides OpenFeature events
STATE_TIMEOUT = 2.0
# slack added when sleeping to a tick boundary, so the tick has fired by the time we look
TICK_MARGIN = 0.5
# cap on waiting for expected requests; a fetch is at most two bounded attempts
COUNT_TIMEOUT = REQUEST_TIMEOUT * 3
# how long an exact request count must keep holding, so a stray extra request still fails
COUNT_SETTLE = 1.0
# tolerance on a measured interval; wide for CI jitter, narrow enough to catch a wrong anchor
CADENCE_TOLERANCE = 1.5
# requests closer than this are attempts of one tick
SAME_TICK_GAP = POLL_INTERVAL / 2
# one poll interval plus a full fetch, so the first tick is observed
FIRST_TICK_TIMEOUT = POLL_INTERVAL + COUNT_TIMEOUT
# outlives a whole fetch, two attempts each abandoned at the timeout
SLOW_RESPONSE_DELAY = REQUEST_TIMEOUT * 4
# takes real time without timing out, so fetch start and end are far apart
SLOW_BUT_WITHIN_TIMEOUT_DELAY = REQUEST_TIMEOUT - 1
# longer than any scenario, so metrics never flush mid-scenario
METRICS_FLUSH_INTERVAL = 3600
# cadence needs a gap, so at least two ticks
MIN_TICKS_TO_COMPARE = 2
# sleep between checks while waiting for a condition
WAIT_POLL_STEP = 0.1
# sleep between checks while asserting a condition keeps holding
HOLD_CHECK_STEP = 0.05

scenarios(str(HARNESS_DIR / "gherkin" / "cdn.feature"))

# gives every scenario a unique OpenFeature domain so providers never leak across scenarios
_SCENARIO_SEQ = itertools.count(1)


@dataclass(kw_only=True)
class Scenario:
    """Holds one scenario's state, shared across its steps."""

    domain: str
    client: OpenFeatureClient
    sdk_key: str = ""
    provider: DynatraceProvider | None = None
    init_error: object | None = None
    # counts read as "since the last thing that happened"
    count_baseline: int = 0
    # polling timer starts when initialization returns; tick boundaries derive from it
    polling_started_at: float = 0.0
    cdn_responses: list[CdnResponse] = field(default_factory=list)
    retry_after: float = 0.0
    mock: MockServer


@pytest.fixture
def scenario(mock_server: MockServer) -> Iterator[Scenario]:
    """Give each scenario a unique domain and a reset mock; shut the SDK down afterwards."""
    mock_server.reset()
    domain = f"e2e-test-{next(_SCENARIO_SEQ)}"
    yield Scenario(domain=domain, client=api.get_client(domain), mock=mock_server)
    api.shutdown()


# GIVEN


@step("a mock server is running")
def _a_mock_server_is_running(scenario: Scenario) -> None:
    pass


@step(parsers.re(r'the SDK key "(?P<key>[^"]*)"'))
def _the_sdk_key(scenario: Scenario, key: str) -> None:
    scenario.sdk_key = key


@step(parsers.re(r'the CDN serves the "(?P<name>[^"]+)" flag configuration'))
def _the_cdn_serves_the_flag_configuration(scenario: Scenario, name: str) -> None:
    _set_cdn_responses(
        scenario, build_cdn_response(HTTPStatus.OK, _load_flag_config(name))
    )


@step(
    parsers.re(
        r'an initialized, READY provider serving the "(?P<name>[^"]+)" flag configuration'
        r'(?: with "(?P<header>[^"]+)" header "(?P<value>[^"]*)")?$'
    )
)
def _an_initialized_ready_provider_serving_the_flag_configuration(
    scenario: Scenario, name: str, header: str | None, value: str | None
) -> None:
    response = build_cdn_response(HTTPStatus.OK, _load_flag_config(name))
    if header:
        response.with_header(header, value or "")
    scenario.sdk_key = BASE_KEY
    _set_cdn_responses(scenario, response)
    _register_provider_with_sdk(scenario)
    assert scenario.init_error is None, scenario.init_error
    # end of arrangement: counts and tick boundaries run from here
    scenario.polling_started_at = time.time()
    _reset_count_baseline(scenario)


@step("the CDN responds slower than the request timeout")
def _the_cdn_responds_slower_than_the_request_timeout(scenario: Scenario) -> None:
    _delay_cdn_responses(scenario, SLOW_RESPONSE_DELAY)


@step("the CDN responds slowly but within the request timeout")
def _the_cdn_responds_slowly_but_within_the_request_timeout(
    scenario: Scenario,
) -> None:
    _delay_cdn_responses(scenario, SLOW_BUT_WITHIN_TIMEOUT_DELAY)


# WHEN


@step("the provider is initialized")
def _the_provider_is_initialized(scenario: Scenario) -> None:
    try:
        _register_provider_with_sdk(scenario)
    except ValueError as exc:
        scenario.init_error = exc
    # anchor, not baseline: requests made by initialization count toward this scenario
    scenario.polling_started_at = time.time()


@step(
    parsers.re(r"the CDN responds with status (?P<status>\d+)"),
    converters={"status": int},
)
def _the_cdn_responds_with_status(scenario: Scenario, status: int) -> None:
    _set_cdn_responses(scenario, build_cdn_response(status))


# unquoted null in the outline means no body at all, unlike the empty string
@step(
    parsers.re(
        r'the CDN responds with status (?P<status>\d+) and body (?P<body>"[^"]*"|null)$'
    ),
    converters={"status": int},
)
def _the_cdn_responds_with_status_and_body(
    scenario: Scenario, status: int, body: str
) -> None:
    _set_cdn_responses(
        scenario,
        build_cdn_response(status, None if body == "null" else body.strip('"')),
    )


@step(
    parsers.re(
        r'the CDN responds with status (?P<status>\d+) and the "(?P<name>[^"]+)" flag configuration'
        r' with "(?P<header>[^"]+)" header "(?P<value>[^"]*)"'
    ),
    converters={"status": int},
)
def _the_cdn_responds_with_status_and_the_flag_configuration_with_header(
    scenario: Scenario, status: int, name: str, header: str, value: str
) -> None:
    _set_cdn_responses(
        scenario,
        build_cdn_response(status, _load_flag_config(name)).with_header(header, value),
    )


@step(
    parsers.re(
        r'the CDN responds (?P<first>\d+) then (?P<second>\d+) with the "(?P<name>[^"]+)" configuration'
    ),
    converters={"first": int, "second": int},
)
def _the_cdn_responds_then_with_the_configuration(
    scenario: Scenario, first: int, second: int, name: str
) -> None:
    _set_cdn_responses(
        scenario,
        build_cdn_response(first),
        build_cdn_response(second, _load_flag_config(name)),
    )


@step(
    parsers.re(
        r"the CDN responds with status (?P<status>\d+) and Retry-After (?P<seconds>\d+) seconds"
    ),
    converters={"status": int, "seconds": int},
)
def _the_cdn_responds_with_status_and_retry_after_seconds(
    scenario: Scenario, status: int, seconds: int
) -> None:
    scenario.retry_after = seconds
    _set_cdn_responses(
        scenario, build_cdn_response(status).with_header("Retry-After", str(seconds))
    )


@step("polling triggers a configuration refetch")
def _polling_triggers_a_configuration_refetch(scenario: Scenario) -> None:
    baseline = scenario.mock.count_cdn_requests()
    _wait_for(POLL_TIMEOUT, lambda: scenario.mock.count_cdn_requests() > baseline)


# baseline is taken before sleeping, so the next assertion counts what these intervals produced
@step(
    parsers.re(r"(?P<intervals>\d+) poll intervals? elapses?"),
    converters={"intervals": int},
)
def _poll_intervals_elapse(scenario: Scenario, intervals: int) -> None:
    _reset_count_baseline(scenario)
    _sleep_until(
        _compute_next_tick_at(scenario) + (intervals - 1) * POLL_INTERVAL + TICK_MARGIN
    )


# waits for the next cadence-anchored tick specifically; a Retry-After window expiring does not fetch
@step("the next poll tick arrives")
def _the_next_poll_tick_arrives(scenario: Scenario) -> None:
    _reset_count_baseline(scenario)
    _sleep_until(_compute_next_tick_at(scenario) + TICK_MARGIN)


@step("the Retry-After window expires")
def _the_retry_after_window_expires(scenario: Scenario) -> None:
    _reset_count_baseline(scenario)
    time.sleep(scenario.retry_after + TICK_MARGIN)


# THEN


@step("initialization succeeds")
def _initialization_succeeds(scenario: Scenario) -> None:
    assert scenario.init_error is None, (
        f"expected init to succeed but got: {scenario.init_error}"
    )


@step("initialization fails")
def _initialization_fails(scenario: Scenario) -> None:
    assert scenario.init_error is not None, "expected initialization to fail"


@step(parsers.re(r'the provider state is "(?P<state>[A-Z_]+)"'))
def _the_provider_state_is(scenario: Scenario, state: str) -> None:
    want = ProviderStatus[state]
    # state rides OpenFeature events, so allow it a moment; no bound provider (construction raised)
    # reads as NOT_READY, since the SDK would report its NoOp default as READY
    _wait_for(
        STATE_TIMEOUT,
        lambda: (
            (
                ProviderStatus.NOT_READY
                if scenario.provider is None
                else scenario.client.get_provider_status()
            )
            == want
        ),
    )


@step(parsers.re(r'the initial CDN request has no "(?P<header>[^"]+)" header'))
def _the_initial_cdn_request_has_no_header(scenario: Scenario, header: str) -> None:
    assert not _get_nth_cdn_request(scenario, 0).has_header(header), (
        f"initial request unexpectedly carried header: {header}"
    )


@step(
    parsers.re(
        r'the (?P<index>\d+)\. CDN request has the "(?P<header>[^"]+)" header "(?P<value>[^"]*)"'
    ),
    converters={"index": int},
)
def _the_cdn_request_has_the_header(
    scenario: Scenario, index: int, header: str, value: str
) -> None:
    request = _get_nth_cdn_request(scenario, index - 1)
    assert request.has_header(header), (
        f"{index}. CDN request missing expected header: {header}"
    )
    assert request.get_header(header) == value, (
        f"{index}. CDN request {header} header mismatch"
    )


@step(
    parsers.re(r"the CDN has received (?P<count>\d+) further requests?"),
    converters={"count": int},
)
def _the_cdn_has_received_further_requests(scenario: Scenario, count: int) -> None:
    # must settle, not just be reached: the interesting assertions are upper bounds
    if count > 0:
        _wait_for(COUNT_TIMEOUT, lambda: _count_further_requests(scenario) >= count)
    _assert_holds_for(
        COUNT_SETTLE,
        lambda: _count_further_requests(scenario) == count,
        lambda: (
            f"expected {count} further CDN request(s), saw {_count_further_requests(scenario)}"
        ),
    )
    _reset_count_baseline(scenario)


@step("consecutive poll ticks are one poll interval apart")
def _consecutive_poll_ticks_are_one_poll_interval_apart(scenario: Scenario) -> None:
    # requests less than half a poll interval apart are attempts of one tick, so keep only the first of each run
    ticks: list[float] = []
    previous: float | None = None
    for r in scenario.mock.fetch_cdn_requests():
        if previous is None or r.received_at - previous >= SAME_TICK_GAP:
            ticks.append(r.received_at)
        previous = r.received_at
    assert len(ticks) >= MIN_TICKS_TO_COMPARE, (
        f"need at least {MIN_TICKS_TO_COMPARE} poll ticks to compare, saw {len(ticks)}"
    )
    for i in range(1, len(ticks)):
        _assert_cadence(
            ticks[i] - ticks[i - 1], f"gap between poll tick {i} and {i + 1}"
        )


@step("the first poll tick is one poll interval after initialization completed")
def _the_first_poll_tick_is_one_poll_interval_after_initialization_completed(
    scenario: Scenario,
) -> None:
    anchor = scenario.polling_started_at

    def find_first_request_after_anchor() -> float | None:
        """Return the arrival time of the first CDN request after initialization completed."""
        return next(
            (
                r.received_at
                for r in scenario.mock.fetch_cdn_requests()
                if r.received_at > anchor
            ),
            None,
        )

    _wait_for(FIRST_TICK_TIMEOUT, lambda: find_first_request_after_anchor() is not None)
    first_tick = find_first_request_after_anchor()
    assert first_tick is not None, (
        "no poll tick observed after initialization completed"
    )
    _assert_cadence(
        first_tick - anchor,
        "gap between initialization completing and the first poll tick",
    )


@step(parsers.re(r'flag "(?P<flag>[^"]+)" evaluates to (?P<word>true|false)'))
def _flag_evaluates_to(scenario: Scenario, flag: str, word: str) -> None:
    assert _is_flag_value(scenario, flag, _parse_bool(word)), (
        f"flag {flag} did not evaluate to {word}"
    )


@step(
    parsers.re(r'flag "(?P<flag>[^"]+)" eventually evaluates to (?P<word>true|false)')
)
def _flag_eventually_evaluates_to(scenario: Scenario, flag: str, word: str) -> None:
    _wait_for(RETRY_TIMEOUT, lambda: _is_flag_value(scenario, flag, _parse_bool(word)))


@step(
    parsers.re(r'flag "(?P<flag>[^"]+)" continues to evaluate to (?P<word>true|false)')
)
def _flag_continues_to_evaluate_to(scenario: Scenario, flag: str, word: str) -> None:
    want = _parse_bool(word)
    assert _is_flag_value(scenario, flag, want), (
        f"flag {flag} did not evaluate to {word}"
    )
    _assert_holds_for(
        STABLE_WINDOW,
        lambda: _is_flag_value(scenario, flag, want),
        lambda: f"flag {flag} changed away from {word}",
    )


@step(parsers.re(r'evaluating flag "(?P<flag>[^"]+)" returns a Flag-Not-Found error'))
def _evaluating_flag_returns_a_flag_not_found_error(
    scenario: Scenario, flag: str
) -> None:
    details = scenario.client.get_boolean_details(flag, False)
    assert details.error_code == ErrorCode.FLAG_NOT_FOUND, (
        f"expected FLAG_NOT_FOUND for unknown flag {flag}, got {details.error_code}"
    )


def _set_cdn_responses(scenario: Scenario, *responses: CdnResponse) -> None:
    """Set the CDN responses, remembering them so a later step can delay them."""
    scenario.cdn_responses = list(responses)
    scenario.mock.set_cdn_responses(scenario.cdn_responses)


def _delay_cdn_responses(scenario: Scenario, delay: float) -> None:
    """Hold back every current CDN response by delay seconds before it is written."""
    assert scenario.cdn_responses, "no CDN responses set to delay"
    for r in scenario.cdn_responses:
        r.with_delay(delay)
    scenario.mock.set_cdn_responses(scenario.cdn_responses)


def _register_provider_with_sdk(scenario: Scenario) -> None:
    """Bind a fresh provider to this scenario's domain, recording a failed init."""
    scenario.provider = DynatraceProvider(
        scenario.sdk_key,
        config_origin=scenario.mock.origin,
        metrics_origin=scenario.mock.origin,
        metrics_flush_interval_s=METRICS_FLUSH_INTERVAL,
    )
    api.set_provider(scenario.provider, scenario.domain)
    # set_provider runs initialize inline and reports failure as PROVIDER_ERROR rather than raising
    status = scenario.client.get_provider_status()
    if status != ProviderStatus.READY:
        scenario.init_error = f"provider is {status} after initialization"


def _reset_count_baseline(scenario: Scenario) -> None:
    """Start counting further CDN requests from now."""
    scenario.count_baseline = scenario.mock.count_cdn_requests()


def _count_further_requests(scenario: Scenario) -> int:
    """Return the CDN requests received since the count baseline."""
    return scenario.mock.count_cdn_requests() - scenario.count_baseline


def _compute_next_tick_at(scenario: Scenario) -> float:
    """Return the next tick boundary on the cadence anchored at polling start."""
    elapsed = time.time() - scenario.polling_started_at
    return scenario.polling_started_at + (elapsed // POLL_INTERVAL + 1) * POLL_INTERVAL


def _get_nth_cdn_request(scenario: Scenario, index: int) -> RecordedRequest:
    """Return the 0-indexed recorded CDN request, asserting it exists."""
    requests = scenario.mock.fetch_cdn_requests()
    assert len(requests) > index, (
        f"expected at least {index + 1} CDN request(s), saw {len(requests)}"
    )
    return requests[index]


def _is_flag_value(scenario: Scenario, flag: str, expected: bool) -> bool:
    """Check a boolean flag through the client; defaults to the opposite so a missing flag never passes."""
    return scenario.client.get_boolean_value(flag, not expected) == expected


def _parse_bool(word: str) -> bool:
    """Parse the true/false word captured from a step."""
    return word == "true"


def _assert_cadence(measured: float, what: str) -> None:
    """Assert a measured gap is one poll interval, within the cadence tolerance."""
    assert abs(measured - POLL_INTERVAL) <= CADENCE_TOLERANCE, (
        f"{what} was {measured:.3f}s, expected {POLL_INTERVAL}s +/- {CADENCE_TOLERANCE}s"
    )


def _load_flag_config(name: str) -> str:
    """Read a flag config fixture from the harness by name, e.g. "flags-v1"."""
    return (FLAGS_DIR / f"{name}.json").read_text()


def _wait_for(timeout: float, condition: Callable[[], bool]) -> None:
    """Poll until condition holds, failing once timeout passes."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return
        time.sleep(WAIT_POLL_STEP)
    assert condition(), f"condition not met within {timeout}s"


def _assert_holds_for(
    duration: float, condition: Callable[[], bool], message: Callable[[], str]
) -> None:
    """Assert condition keeps holding for the whole duration."""
    deadline = time.time() + duration
    while time.time() < deadline:
        assert condition(), message()
        time.sleep(HOLD_CHECK_STEP)
    assert condition(), message()


def _sleep_until(at: float) -> None:
    """Sleep until the given epoch time, returning at once if it has passed."""
    remaining = at - time.time()
    if remaining > 0:
        time.sleep(remaining)
