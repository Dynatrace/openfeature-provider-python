"""Step definitions for the shared gherkin specs (fm-provider-test-harness/gherkin/).

Black-box: each scenario binds a fresh provider to its own OpenFeature domain and
observes it only through the SDK client, against the containerized mock server.
"""

from __future__ import annotations

import itertools
import json
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http import HTTPStatus
from urllib.parse import urlsplit

import pytest
from mock_server import (
    HARNESS_DIR,
    SSE_PATH,
    CdnResponse,
    MockServer,
    RecordedRequest,
    build_cdn_response,
)
from openfeature import api
from openfeature.client import OpenFeatureClient
from openfeature.event import EventDetails, ProviderEvent
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
# timings the harness shortens for every provider; the rest are production defaults
TIMINGS = json.loads((HARNESS_DIR / "gherkin" / "timings.json").read_text())
SSE_CONNECTED_POLL_INTERVAL = TIMINGS["SSE_CONNECTED_POLL_INTERVAL_MS"] / 1000
GRACE_PERIOD = TIMINGS["GRACE_PERIOD_MS"] / 1000
# the cadence while SSE is disabled or disconnected; the CDN spec runs with SSE disabled
SSE_DISCONNECTED_POLL_INTERVAL = _POLL_INTERVAL_S
# scheduling slack on top of the event source's worst-case reconnect after a blip
BLIP_RECONNECT_SLACK = 1.0
# read from the provider, so scenarios assert against the real cadence
POLL_INTERVAL = SSE_DISCONNECTED_POLL_INTERVAL
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
# slack for something the step just caused, and the window for asserting its absence
SETTLE = 2.0
# how long to wait for a provider event the step just caused
EVENT_TIMEOUT = 5.0
# how long to wait for the provider to open its SSE stream
SSE_CONNECT_TIMEOUT = 5.0
# placeholder for the mock server's authority in a spec'd URL
MOCK_SERVER_PLACEHOLDER = "<mockserver>"
# how far before a tick an SSE re-fetch is triggered, so it is still in flight then
SHORTLY_BEFORE = 0.5
# payload a shut-down provider must not react to
REFETCH_MESSAGE = '{"type":"refetchConfig","lastModified":1704153600}'

scenarios(str(HARNESS_DIR / "gherkin" / "cdn.feature"))
scenarios(str(HARNESS_DIR / "gherkin" / "sse.feature"))

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
    # SSE is off throughout the CDN spec; the SSE spec's background turns it on
    sse_enabled: bool = False
    # counts read as "since the last thing that happened"
    count_baseline: int = 0
    # polling timer starts when initialization returns; tick boundaries derive from it
    polling_started_at: float = 0.0
    cdn_responses: list[CdnResponse] = field(default_factory=list)
    retry_after: float = 0.0
    # provider events seen on this scenario's client, in arrival order
    events: list[tuple[ProviderEvent, EventDetails]] = field(default_factory=list)
    events_lock: threading.Lock = field(default_factory=threading.Lock)
    # index into events; each "a PROVIDER_* event is emitted" step consumes one
    event_cursor: int = 0
    mock: MockServer


@pytest.fixture
def scenario(mock_server: MockServer) -> Iterator[Scenario]:
    """Give each scenario a unique domain and a reset mock; shut the SDK down afterwards."""
    mock_server.reset()
    # reset clears the SSE outage gate but not a stream the previous scenario's provider is
    # still tearing down; wait it out so "opens no SSE connection" sees a clean slate
    _poll_until(SETTLE, lambda: mock_server.sse_client_count() == 0)
    domain = f"e2e-test-{next(_SCENARIO_SEQ)}"
    s = Scenario(domain=domain, client=api.get_client(domain), mock=mock_server)
    for event in ProviderEvent:
        s.client.add_handler(event, _event_recorder(s, event))
    yield s
    api.shutdown()


# GIVEN


@step("a mock server is running")
def _a_mock_server_is_running(scenario: Scenario) -> None:
    scenario.sse_enabled = False


@step("a mock server with SSE enabled is running")
def _a_mock_server_with_sse_enabled_is_running(scenario: Scenario) -> None:
    scenario.sse_enabled = True


@step(parsers.re(r'the SDK key "(?P<key>[^"]*)"'))
def _the_sdk_key(scenario: Scenario, key: str) -> None:
    scenario.sdk_key = key


@step("SSE is disabled by configuration")
def _sse_is_disabled_by_configuration(scenario: Scenario) -> None:
    scenario.sse_enabled = False


@step(parsers.re(r'the CDN serves the "(?P<name>[^"]+)" flag configuration'))
def _the_cdn_serves_the_flag_configuration(scenario: Scenario, name: str) -> None:
    _set_cdn_responses(
        scenario, build_cdn_response(HTTPStatus.OK, _load_flag_config(name))
    )


@step(
    parsers.re(
        r'the CDN serves the "(?P<name>[^"]+)" flag configuration'
        r' advertising an SSE stream at url "(?P<url>[^"]+)"'
    )
)
def _the_cdn_serves_the_flag_configuration_advertising_an_sse_stream_at_url(
    scenario: Scenario, name: str, url: str
) -> None:
    _set_cdn_responses(
        scenario,
        build_cdn_response(
            HTTPStatus.OK, _advertising_sse_url(scenario, _load_flag_config(name), url)
        ),
    )


# a header value is double-quoted, or single-quoted when it holds double quotes itself (an ETag: '"v1"')
_HEADER_VALUE = r"(?P<value>\"[^\"]*\"|'[^']*')"


@step(
    parsers.re(
        r'an initialized, READY provider serving the "(?P<name>[^"]+)" flag configuration'
        r'(?: with "(?P<header>[^"]+)" header ' + _HEADER_VALUE + r")?"
    )
)
def _an_initialized_ready_provider_serving_the_flag_configuration(
    scenario: Scenario, name: str, header: str | None, value: str | None
) -> None:
    response = build_cdn_response(HTTPStatus.OK, _load_flag_config(name))
    if header:
        response.with_header(header, _unquote(value) if value else "")
    scenario.sdk_key = BASE_KEY
    _set_cdn_responses(scenario, response)
    _register_provider_with_sdk(scenario)
    assert scenario.init_error is None, scenario.init_error
    _await_sse_settled(scenario)
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
        r'the CDN responds with status (?P<status>\d+) and body (?P<body>"[^"]*"|null)'
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
        r' with "(?P<header>[^"]+)" header ' + _HEADER_VALUE
    ),
    converters={"status": int},
)
def _the_cdn_responds_with_status_and_the_flag_configuration_with_header(
    scenario: Scenario, status: int, name: str, header: str, value: str
) -> None:
    _set_cdn_responses(
        scenario,
        build_cdn_response(status, _load_flag_config(name)).with_header(
            header, _unquote(value)
        ),
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
    _await_cdn_request_within(scenario, POLL_TIMEOUT)


@step(
    "polling triggers a configuration refetch after the SSE_DISCONNECTED_POLL_INTERVAL"
)
def _polling_triggers_a_configuration_refetch_after_the_sse_disconnected_poll_interval(
    scenario: Scenario,
) -> None:
    _await_cdn_request_within(scenario, SSE_DISCONNECTED_POLL_INTERVAL)


@step("polling triggers a configuration refetch after the SSE_CONNECTED_POLL_INTERVAL")
def _polling_triggers_a_configuration_refetch_after_the_sse_connected_poll_interval(
    scenario: Scenario,
) -> None:
    _await_cdn_request_within(scenario, SSE_CONNECTED_POLL_INTERVAL)


# a blip: the backend drops the stream and refuses connections for a while, then accepts them again
@step(
    parsers.re(r"the SSE stream drops for (?P<seconds>\d+) seconds and reconnects"),
    converters={"seconds": int},
)
def _the_sse_stream_drops_and_reconnects(scenario: Scenario, seconds: int) -> None:
    # the bound is the event source's doubling backoff, whose retry after an outage can come up to
    # twice the outage later. It must stay under the fallback poll interval, or a poll lands mid-blip.
    reconnect_timeout = 3 * seconds + BLIP_RECONNECT_SLACK
    assert reconnect_timeout < POLL_INTERVAL, (
        f"a {seconds}s blip cannot be told apart from polling at {POLL_INTERVAL}s"
    )
    scenario.mock.drop_sse(seconds)
    # the drop is asynchronous: wait for the old stream to go, so its count is not mistaken for the reconnect
    _poll_until(SETTLE, lambda: scenario.mock.sse_client_count() == 0)
    assert _poll_until(
        reconnect_timeout,
        lambda: scenario.mock.sse_client_count() >= 1,
    ), f"SSE did not reconnect within {reconnect_timeout}s of a {seconds}s outage"


# a sustained outage: connections stay refused until the stream is restored or reset
@step("the SSE stream drops and stays down")
def _the_sse_stream_drops_and_stays_down(scenario: Scenario) -> None:
    scenario.mock.drop_sse()
    # the provider switches cadence as soon as it sees the fault; the Then steps assert what it did
    assert _poll_until(SETTLE, lambda: scenario.mock.sse_client_count() == 0), (
        "the mock server did not drop the SSE stream"
    )


@step(parsers.re(r"the server emits an SSE message (?P<payload>.*)"))
def _the_server_emits_an_sse_message(scenario: Scenario, payload: str) -> None:
    baseline = scenario.mock.count_cdn_requests()
    # pytest-bdd doubles backslashes when it substitutes an outline cell into the step text,
    # so "\"v2\"" would reach the wire as invalid JSON; Cucumber passes the cell as written
    scenario.mock.emit_sse(_unquote(payload).replace("\\\\", "\\"))
    # essentially a sleep, cut short once the message has triggered a CDN request
    _poll_until(SETTLE, lambda: scenario.mock.count_cdn_requests() > baseline)


@step("the GRACE_PERIOD expires")
def _the_grace_period_expires(scenario: Scenario) -> None:
    _poll_until(
        GRACE_PERIOD + SETTLE,
        lambda: scenario.client.get_provider_status() == ProviderStatus.ERROR,
    )


@step("the provider is shut down")
def _the_provider_is_shut_down(scenario: Scenario) -> None:
    api.shutdown()


# occupies the fetch slot just before a tick, so the tick has something to contend with
@step("an SSE re-fetch is triggered shortly before the next poll tick")
def _an_sse_refetch_is_triggered_shortly_before_the_next_poll_tick(
    scenario: Scenario,
) -> None:
    _sleep_until(_compute_next_tick_at(scenario) - SHORTLY_BEFORE)
    before = scenario.mock.count_cdn_requests()
    _trigger_sse_refetch(scenario)
    # the re-fetch runs on a poller thread, so wait for its request to reach the CDN; otherwise it
    # lands after the next step takes its baseline and is miscounted as the contended tick's
    _wait_for(SHORTLY_BEFORE, lambda: scenario.mock.count_cdn_requests() > before)


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


@step(
    parsers.re(
        r'a PROVIDER_CONFIGURATION_CHANGED event is emitted with changed flag "(?P<flag>[^"]+)"'
    )
)
def _a_configuration_changed_event_is_emitted_with_changed_flag(
    scenario: Scenario, flag: str
) -> None:
    details = _consume_event(scenario, ProviderEvent.PROVIDER_CONFIGURATION_CHANGED)
    changed = details.flags_changed
    assert changed is not None, (
        "PROVIDER_CONFIGURATION_CHANGED carried no changed flags"
    )
    assert flag in changed, (
        f"PROVIDER_CONFIGURATION_CHANGED changed flags {changed} do not include {flag}"
    )


@step(parsers.re(r"a (?P<event_name>PROVIDER_[A-Z_]+) event is emitted"))
def _a_provider_event_is_emitted(scenario: Scenario, event_name: str) -> None:
    _consume_event(scenario, ProviderEvent[event_name])


@step(parsers.re(r"no (?P<event_name>PROVIDER_[A-Z_]+) event is emitted"))
def _no_provider_event_is_emitted(scenario: Scenario, event_name: str) -> None:
    unwanted = ProviderEvent[event_name]

    def none_seen() -> bool:
        with scenario.events_lock:
            return all(e != unwanted for e, _ in scenario.events)

    # hold it: an event the earlier steps caused may still be on its way to the client
    _assert_holds_for(
        SETTLE, none_seen, lambda: f"provider unexpectedly emitted {event_name}"
    )


@step(parsers.re(r'the provider has an SSE connection to "(?P<path>[^"]+)"'))
def _the_provider_has_an_sse_connection_to(scenario: Scenario, path: str) -> None:
    # the backend serves a single stream path, so a live subscriber is a stream to that path
    assert path == SSE_PATH, f"the mock server only serves an SSE stream at {SSE_PATH}"
    assert _poll_until(
        SSE_CONNECT_TIMEOUT, lambda: scenario.mock.sse_client_count() == 1
    ), f"provider did not open an SSE connection to {path}"


@step("the provider opens no SSE connection")
def _the_provider_opens_no_sse_connection(scenario: Scenario) -> None:
    # hold the assertion: a connection attempt in flight must not be mistaken for none at all
    _assert_holds_for(
        SETTLE,
        lambda: scenario.mock.sse_client_count() == 0,
        lambda: "provider unexpectedly opened an SSE connection",
    )


@step("the provider closes the SSE connection")
def _the_provider_closes_the_sse_connection(scenario: Scenario) -> None:
    assert _poll_until(SETTLE, lambda: scenario.mock.sse_client_count() == 0), (
        "provider did not close the SSE connection"
    )


@step("a message after shutdown triggers no CDN request")
def _a_message_after_shutdown_triggers_no_cdn_request(scenario: Scenario) -> None:
    baseline = scenario.mock.count_cdn_requests()
    scenario.mock.emit_sse(REFETCH_MESSAGE)
    time.sleep(STABLE_WINDOW)
    assert scenario.mock.count_cdn_requests() == baseline, (
        "a shut-down provider still re-fetched on an SSE message"
    )


# the cadence itself isn't observable black-box, so it is asserted by its consequence: with SSE
# connected no fetch may arrive for longer than the aggressive cadence would allow
@step("the active poll interval is the SSE_CONNECTED_POLL_INTERVAL")
def _the_active_poll_interval_is_the_sse_connected_interval(
    scenario: Scenario,
) -> None:
    assert POLL_TIMEOUT < SSE_CONNECTED_POLL_INTERVAL, (
        f"observation window must be shorter than the {SSE_CONNECTED_POLL_INTERVAL}s interval under test"
    )
    # the connect catch-up fetch must be in before the baseline, or it lands mid-observation
    _await_sse_settled(scenario)
    baseline = scenario.mock.count_cdn_requests()
    time.sleep(POLL_TIMEOUT)
    assert scenario.mock.count_cdn_requests() == baseline, (
        f"provider kept fetching within {POLL_TIMEOUT}s, so polling did not relax to the "
        f"{SSE_CONNECTED_POLL_INTERVAL}s interval"
    )


# asserted by its consequence too: a fetch must arrive within the aggressive cadence
@step("the active poll interval is the SSE_DISCONNECTED_POLL_INTERVAL")
def _the_active_poll_interval_is_the_sse_disconnected_interval(
    scenario: Scenario,
) -> None:
    _await_cdn_request_within(scenario, SSE_DISCONNECTED_POLL_INTERVAL)


@step(
    parsers.re(r"the CDN received (?P<count>\d+) requests"),
    converters={"count": int},
)
def _the_cdn_received_requests(scenario: Scenario, count: int) -> None:
    # a fetch the provider was told to make (SSE connect, SSE message, poll) is in flight by the
    # time this step runs, so wait for the expected count before insisting on it
    _poll_until(SETTLE, lambda: scenario.mock.count_cdn_requests() >= count)
    seen = scenario.mock.count_cdn_requests()
    assert seen == count, f"expected {count} CDN requests, saw {seen}"


@step(parsers.re(r'the initial CDN request has no "(?P<header>[^"]+)" header'))
def _the_initial_cdn_request_has_no_header(scenario: Scenario, header: str) -> None:
    assert not _get_nth_cdn_request(scenario, 0).has_header(header), (
        f"initial request unexpectedly carried header: {header}"
    )


@step(
    parsers.re(
        r'the (?P<index>\d+)\. CDN request has the "(?P<header>[^"]+)" header '
        + _HEADER_VALUE
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
    assert request.get_header(header) == _unquote(value), (
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
        # the harness's shortened timings, so SSE scenarios finish in reasonable time
        sse_connected_poll_interval_s=SSE_CONNECTED_POLL_INTERVAL,
        grace_period_s=GRACE_PERIOD,
        disable_sse=not scenario.sse_enabled,
    )
    api.set_provider(scenario.provider, scenario.domain)
    # set_provider runs initialize inline and reports failure as PROVIDER_ERROR rather than raising
    status = scenario.client.get_provider_status()
    if status != ProviderStatus.READY:
        scenario.init_error = f"provider is {status} after initialization"


def _event_recorder(
    scenario: Scenario, event: ProviderEvent
) -> Callable[[EventDetails], None]:
    """Return a client handler that appends ``event`` to the scenario's event log."""

    def record(details: EventDetails) -> None:
        with scenario.events_lock:
            scenario.events.append((event, details))

    return record


def _consume_event(scenario: Scenario, expected: ProviderEvent) -> EventDetails:
    """Return the next recorded event of type ``expected`` and advance the cursor past it.

    The cursor makes repeated steps (READY at startup, READY again after
    recovery) consume distinct events.
    """
    deadline = time.time() + EVENT_TIMEOUT
    while True:
        with scenario.events_lock:
            for i in range(scenario.event_cursor, len(scenario.events)):
                event, details = scenario.events[i]
                if event == expected:
                    scenario.event_cursor = i + 1
                    return details
            seen = [e.name for e, _ in scenario.events[scenario.event_cursor :]]
        if time.time() >= deadline:
            pytest.fail(
                f"no {expected.name} event emitted within {EVENT_TIMEOUT}s; saw {seen}"
            )
        time.sleep(WAIT_POLL_STEP)


def _await_sse_settled(scenario: Scenario) -> None:
    """Wait for the SSE stream and the catch-up fetch its connect pulls.

    So a scenario starting from a READY provider never races the stream it is
    about to drop or message. No-op when SSE is off, as it is throughout the
    CDN spec.
    """
    if not scenario.sse_enabled:
        return
    assert _poll_until(
        SSE_CONNECT_TIMEOUT, lambda: scenario.mock.sse_client_count() >= 1
    ), "provider did not open its SSE stream"
    # fetch 1 comes from initialize, fetch 2 from the successful connect
    assert _poll_until(SETTLE, lambda: scenario.mock.count_cdn_requests() >= 2), (
        "SSE connected but pulled no catch-up fetch"
    )


def _await_cdn_request_within(scenario: Scenario, interval: float) -> None:
    """Wait for a CDN request to arrive within ``interval`` seconds."""
    baseline = scenario.mock.count_cdn_requests()
    _wait_for(interval, lambda: scenario.mock.count_cdn_requests() > baseline)


def _advertising_sse_url(scenario: Scenario, config_json: str, url: str) -> str:
    """Rewrite a flag configuration fixture to advertise a direct SSE url."""
    resolved = url.replace(
        MOCK_SERVER_PLACEHOLDER, urlsplit(scenario.mock.origin).netloc
    )
    config = json.loads(config_json)
    config["eventStreams"] = [{"type": "sse", "url": resolved}]
    return json.dumps(config)


def _trigger_sse_refetch(scenario: Scenario) -> None:
    """Request an SSE-triggered re-fetch as the SSE manager would, without a live stream.

    Connecting a real one would switch the provider onto the SSE-connected
    cadence, changing the timing the single-flight scenario exists to measure.
    """
    assert scenario.provider is not None, "provider has not been initialized"
    poller = scenario.provider._poller
    assert poller is not None, "provider has no polling manager"
    poller.trigger_immediate_fetch(None)


def _unquote(value: str) -> str:
    """Strip one matching pair of surrounding single or double quotes, if present."""
    trimmed = value.strip()
    if len(trimmed) >= 2 and trimmed[0] == trimmed[-1] and trimmed[0] in "'\"":
        return trimmed[1:-1]
    return trimmed


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


def _poll_until(timeout: float, condition: Callable[[], bool]) -> bool:
    """Poll until condition holds or timeout passes; report whether it held."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(WAIT_POLL_STEP)
    return condition()


def _wait_for(timeout: float, condition: Callable[[], bool]) -> None:
    """Poll until condition holds, failing once timeout passes."""
    assert _poll_until(timeout, condition), f"condition not met within {timeout}s"


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
