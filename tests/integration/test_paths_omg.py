"""L2: a place order on OMG's local stack shows in `/paths` as one flow from
`POST /v1/orders/place_order` across the listener's link to the processor and its partner call,
with the inbound proxy's shapes on the `SERVER` span's node and no canary anywhere.

Needs OMG's local stack with FIRE's collector override (dev/omg/README.md, section a): the API
at `OMG_API_UPSTREAM` (default http://localhost:8020) and the collector exporting to
`OMG_OTLP_LISTEN` (default 127.0.0.1:4318), where this test runs the tentacle's receiver next to
a proxy on a free port. The order is OMG's `test_order_payload.json` with fresh ids, as
`make test-order` posts it, and canaries in its ids and customer fields.

The body's clock is real time plus an offset. Spans are accepted on real time; once no span of
the order's flow has arrived for `QUIET_S`, the test measures how far apart the flow's spans
reached the body, checks that against `paths.settle_s`, and moves the clock past it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from canary import Canary

import fire.apps.incident_engine.app as app_module
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.apps.tentacle.runner import EXIT_OK, prepare, run

TOKEN = "paths-omg-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
SETTLE_S = 60
API = "order-management-api"
DISTRIBUTOR = "order-management-gateway-work-distributor"
SF = "order-management-gateway-sf-publisher"
ROUTE = "POST /v1/orders/place_order"
LISTENER = "dynamo_listener.distribute_work"
PROCESS = "sf-publisher.process_work_unit"
WRITE = "DynamoDB.PutItem omg-dev-order_domain"
REQUEST = f"http:inbound:{ROUTE}:request"
HEALTH = "http:inbound:GET /healthcheck:response:2xx"
# OMG's SDKs export every 5 s; the listener runs once LocalStack polls the table's stream.
FLOW_WAIT_S = 120.0
QUIET_S = 15.0
UPSTREAM_ABORTED = "upstream-aborted"
ATTEMPTS = 3

pytestmark = [pytest.mark.integration, pytest.mark.omg_local]


class RecordingTransport(httpx.AsyncBaseTransport):
    """Keeps every request body the sender posts, and when it was posted."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.bodies: list[tuple[float, str]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.bodies.append((time.monotonic(), request.content.decode()))
        return await self.inner.handle_async_request(request)

    def posted(self, kind: str) -> list[dict[str, Any]]:
        return [item for _at, body in self.bodies for item in json.loads(body)[kind]]

    def arrivals(self, traces: set[str]) -> list[float]:
        """When each batch holding a span of `traces` reached the body."""
        return [
            at
            for at, body in self.bodies
            if any(span["trace_id"] in traces for span in json.loads(body)["spans"])
        ]


@dataclass
class OffsetClock:
    offset: timedelta = field(default_factory=timedelta)

    def __call__(self) -> datetime:
        return datetime.now(UTC) + self.offset


@dataclass
class Stack:
    proxy: str
    transport: RecordingTransport
    clock: OffsetClock
    stop: asyncio.Event
    running: asyncio.Task[int]


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _key(channel: dict[str, Any]) -> str:
    key = f"{channel['kind']}:{channel['direction']}:{channel['name']}:{channel['part']}"
    return f"{key}:{channel['status_class']}" if channel.get("status_class") else key


async def _until(
    what: str, condition: Callable[[], object], timeout_s: float, poll: Any = None
) -> None:
    deadline = time.monotonic() + timeout_s
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"{what}: not met within {timeout_s}s")
        if poll is not None:
            await poll()
        await asyncio.sleep(0.5)


@pytest.fixture
async def stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Stack]:
    port = _free_port()
    fire_yml = tmp_path / "config" / "fire.yml"
    (fire_yml.parent / "systems").mkdir(parents=True)
    fire_yml.write_text(yaml.safe_dump({"tentacle": {"flush_ms": 50}}), encoding="utf-8")
    system = {
        "system": "omg",
        "display_name": "Order Management Gateway",
        "notify": {"slack": {"channel_id": "C0000000000"}},
        "sources": [
            {
                "id": "omg-api",
                "kind": "http_proxy",
                "listen_port": port,
                "upstream_env": "OMG_API_UPSTREAM",
            },
            {"id": "omg-traces", "kind": "otlp", "listen_env": "OMG_OTLP_LISTEN"},
        ],
        "paths": {"settle_s": SETTLE_S},
    }
    (fire_yml.parent / "systems" / "omg.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    clock = OffsetClock()
    app_module._container = build_container(fire_yml, clock=clock)
    environ = {
        "FIRE_API_TOKEN": TOKEN,
        "FIRE_BODY_URL": "http://body",
        "OMG_API_UPSTREAM": os.environ.get("OMG_API_UPSTREAM", "http://localhost:8020"),
        "OMG_OTLP_LISTEN": os.environ.get("OMG_OTLP_LISTEN", "127.0.0.1:4318"),
    }
    transport = RecordingTransport(httpx.ASGITransport(app=app))
    stop = asyncio.Event()
    running = asyncio.create_task(
        run(prepare("omg", config_path=fire_yml, environ=environ), stop, transport=transport)
    )
    proxy = f"http://127.0.0.1:{port}"
    async with httpx.AsyncClient(base_url=proxy, trust_env=False, timeout=10) as client:

        async def ping() -> None:
            await client.get("/healthcheck")

        await _until(
            "templates loaded",
            lambda: any(_key(s["channel"]) == HEALTH for s in transport.posted("shapes")),
            30,
            ping,
        )

    yield Stack(proxy, transport, clock, stop, running)

    if not running.done():
        stop.set()
        await asyncio.wait_for(running, 15)
    app_module._container = None


async def _place_order(proxy: str, canary: Canary) -> None:
    """Places a fresh order through the proxy, with canaries in its ids and customer fields."""
    payload = (
        Path(os.environ["OMG_REPO"]) / "scripts" / "test_payloads" / "test_order_payload.json"
    )
    order = json.loads(payload.read_text(encoding="utf-8"))
    order["referenceOrderId"] = order["checkoutId"] = canary.token()
    for key in ("firstName", "lastName"):
        order["customer"][key] = order["shippingAddress"][key] = canary.token()
    order["customer"]["email"] = f"{canary.token()}@example.com"
    for key in ("streetAddress", "extendedAddress", "locality"):
        order["shippingAddress"][key] = canary.token()
    async with httpx.AsyncClient(base_url=proxy, trust_env=False, timeout=60) as client:
        for _ in range(ATTEMPTS):
            response = await client.post("/v1/orders/place_order", json=order)
            if response.headers.get("x-fire-proxy") != UPSTREAM_ABORTED:
                assert response.status_code == 201, response.status_code
                return
    raise AssertionError(f"OMG dropped the connection {ATTEMPTS} times in a row")


def _find(spans: list[dict[str, Any]], **fields: Any) -> dict[str, Any] | None:
    return next(
        (span for span in spans if all(span[key] == value for key, value in fields.items())),
        None,
    )


async def test_a_place_order_shows_in_paths_across_the_listeners_link(
    stack: Stack, canary: Canary, caplog: pytest.LogCaptureFixture, quiet_httpx: None
) -> None:
    caplog.set_level(logging.DEBUG)
    await _place_order(stack.proxy, canary)
    await _until(
        "the order's request shape",
        lambda: any(_key(s["channel"]) == REQUEST for s in stack.transport.posted("shapes")),
        15,
    )
    [request] = [s for s in stack.transport.posted("shapes") if _key(s["channel"]) == REQUEST]
    api_trace = request["trace_id"]

    def listener() -> dict[str, Any] | None:
        server = _find(stack.transport.posted("spans"), operation=ROUTE, trace_id=api_trace)
        if server is None:
            return None
        link = {"trace_id": api_trace, "span_id": server["span_id"]}
        spans = stack.transport.posted("spans")
        return next((s for s in spans if s["operation"] == LISTENER and link in s["links"]), None)

    def processed() -> bool:
        found = listener()
        spans = stack.transport.posted("spans")
        return found is not None and bool(
            _find(spans, operation=PROCESS, trace_id=found["trace_id"])
        )

    await _until("the processor's span in the listener's trace", processed, FLOW_WAIT_S)
    found = listener()
    assert found is not None
    traces = {api_trace, found["trace_id"]}
    await _until(
        f"no span of the flow for {QUIET_S}s",
        lambda: time.monotonic() - max(stack.transport.arrivals(traces)) >= QUIET_S,
        FLOW_WAIT_S,
    )
    stack.stop.set()
    assert await asyncio.wait_for(stack.running, 15) == EXIT_OK
    arrivals = stack.transport.arrivals(traces)
    spread = max(arrivals) - min(arrivals)
    print(f"\nflow spans reached the body over {spread:.1f}s in {len(arrivals)} batches")
    assert spread < SETTLE_S

    stack.clock.offset = timedelta(seconds=SETTLE_S)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://body"
    ) as client:
        response = await client.get("/v0/systems/omg/paths", headers=HEADERS)
    assert response.status_code == 200
    body = response.json()

    [path] = [p for p in body["paths"] if p["entry"] == {"service": API, "operation": ROUTE}]
    assert path["flows"] >= 1
    nodes = [(node["service"], node["operation"]) for node in path["nodes"]]
    for node in ((API, WRITE), (DISTRIBUTOR, LISTENER), (SF, PROCESS)):
        assert node in nodes
    assert any(n[0] == DISTRIBUTOR and n[1].startswith("SNS.Publish ") for n in nodes)
    assert any(
        node["service"] == SF and node["kinds"] == ["client"] and node["operation"][:5] == "POST "
        for node in path["nodes"]
    )
    edges = {(nodes[e["from"]], nodes[e["to"]], e["via"]) for e in path["edges"]}
    assert ((API, ROUTE), (DISTRIBUTOR, LISTENER), "link") in edges
    server = path["nodes"][nodes.index((API, ROUTE))]
    assert REQUEST in server["channels"]
    assert any(key.startswith(f"http:inbound:{ROUTE}:response:") for key in server["channels"])
    assert canary.leaks(json.dumps(body), envelope=False) == []
    for _at, posted in stack.transport.bodies:
        assert canary.leaks(posted, envelope=False) == []
    assert canary.leaks(caplog.text, envelope=False) == []
