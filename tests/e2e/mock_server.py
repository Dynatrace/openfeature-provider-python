"""Client for the containerized mock server from fm-provider-test-harness.

The mock emulates every component the provider talks to (CDN config endpoint, SSE and
metrics ingest) and exposes an HTTP control plane to set CDN responses and inspect requests.
"""

from __future__ import annotations

import os
import typing
from dataclasses import dataclass, field
from http import HTTPStatus
from pathlib import Path

import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

# harness submodule at the repo root
HARNESS_DIR = Path(__file__).resolve().parents[2] / "fm-provider-test-harness"

# image repository; the tag comes from the pinned harness's version.txt
IMAGE_REPO = "ghcr.io/dynatrace/fm-provider-mock-server"
# port the mock server listens on inside the container
CONTAINER_PORT = 8080
# how long the container may take to report healthy
CONTAINER_STARTUP_TIMEOUT = 60
# bound on a single control-plane call
CONTROL_REQUEST_TIMEOUT = 10.0
# the control plane speaks epoch and delay millis; the suite works in seconds
MS_PER_SECOND = 1000


@dataclass
class CdnResponse:
    """One CDN response for the mock to serve; a None body means no body at all."""

    status: int
    body: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    delay_ms: int = 0

    def with_header(self, name: str, value: str) -> CdnResponse:
        """Add a response header, returning self for chaining."""
        self.headers[name] = value
        return self

    def with_delay(self, delay: float) -> CdnResponse:
        """Hold the response back by delay seconds; the mock records the request at arrival."""
        self.delay_ms = int(delay * MS_PER_SECOND)
        return self

    def to_json(self) -> dict[str, typing.Any]:
        """Return the control-plane representation of this response."""
        r: dict[str, typing.Any] = {"status": self.status, "headers": self.headers}
        if self.body is not None:
            r["body"] = self.body
        if self.delay_ms > 0:
            r["delayMs"] = self.delay_ms
        return r


def build_cdn_response(status: int, body: str | None = None) -> CdnResponse:
    """Build a CDN response with the given status and optional body."""
    return CdnResponse(status=status, body=body)


@dataclass
class RecordedRequest:
    """A CDN request the mock server recorded, header names lower-cased."""

    method: str
    path: str
    headers: dict[str, str]
    # epoch seconds at arrival, stamped before any response delay
    received_at: float

    def has_header(self, name: str) -> bool:
        """Check whether the request carried the header, case-insensitively."""
        return name.lower() in self.headers

    def get_header(self, name: str) -> str | None:
        """Return the header value, case-insensitively, or None if absent."""
        return self.headers.get(name.lower())


class MockServer:
    """Client for the mock server's control plane, owning its container."""

    def __init__(self, container: DockerContainer, origin: str) -> None:
        self._container = container
        self.origin = origin
        self._http = requests.Session()

    @classmethod
    def start(cls) -> MockServer:
        """Start the mock server container and return a client bound to it."""
        # image tag follows the pinned harness, so the mock can't drift from the spec
        image = os.environ.get("MOCKSERVER_IMAGE")
        if not image:
            version_file = HARNESS_DIR / "version.txt"
            if not version_file.exists():
                raise RuntimeError(
                    "missing fm-provider-test-harness; run `git submodule update --init fm-provider-test-harness`"
                )
            image = f"{IMAGE_REPO}:v{version_file.read_text().strip()}"
        container = (
            DockerContainer(image)
            .with_exposed_ports(CONTAINER_PORT)
            .waiting_for(
                HttpWaitStrategy(CONTAINER_PORT, "/__control__/health")
                .for_status_code(HTTPStatus.OK)
                .with_startup_timeout(CONTAINER_STARTUP_TIMEOUT)
            )
        )
        container.start()
        host = container.get_container_host_ip()
        port = container.get_exposed_port(CONTAINER_PORT)
        return cls(container, f"http://{host}:{port}")

    def reset(self) -> None:
        """Clear the CDN responses, recorded requests and rate-limit state."""
        self.send_control_request("POST", "/__control__/reset")

    def set_cdn_responses(self, responses: list[CdnResponse]) -> None:
        """Set the ordered CDN responses; the last one repeats once exhausted."""
        self.send_control_request(
            "PUT",
            "/__control__/cdn/responses",
            {"responses": [r.to_json() for r in responses], "repeatLast": True},
        )

    def fetch_cdn_requests(self) -> list[RecordedRequest]:
        """Return the CDN requests received since the last reset, in arrival order."""
        payload = self.send_control_request("GET", "/__control__/cdn/requests").json()
        return [
            RecordedRequest(
                method=r.get("method", ""),
                path=r.get("path", ""),
                headers={k.lower(): v for k, v in (r.get("headers") or {}).items()},
                received_at=r["receivedAtMs"] / MS_PER_SECOND,
            )
            for r in payload.get("requests") or []
        ]

    def count_cdn_requests(self) -> int:
        """Return how many CDN requests were received since the last reset."""
        return len(self.fetch_cdn_requests())

    def send_control_request(
        self, method: str, path: str, body: typing.Any = None
    ) -> requests.Response:
        """Call a control-plane endpoint, failing on any non-2xx status."""
        res = self._http.request(
            method, self.origin + path, json=body, timeout=CONTROL_REQUEST_TIMEOUT
        )
        res.raise_for_status()
        return res

    def stop(self) -> None:
        """Stop the mock server container."""
        self._container.stop()
