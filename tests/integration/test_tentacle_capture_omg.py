"""L2: sf-publisher's Helios capture of a fresh place order on OMG's local stack reaches the body
as outbound shapes joined to this order's `sf-publisher.business_logic` span, `/paths` shows
them on that node, and no canary, S3 key, or order id appears in what the tentacle sends or logs.

Needs OMG's local stack with FIRE's collector override (dev/omg/README.md, section a): the API at
`OMG_API_UPSTREAM` (default http://localhost:8020), LocalStack at `OMG_AWS_ENDPOINT` (default
http://localhost:4566) holding `OMG_TRANSMISSION_BUCKET`, and the collector exporting to
`OMG_OTLP_LISTEN` (default 127.0.0.1:4318). The test runs the tentacle with the `omg-traces`
receiver and the `transmissions` source of `config/systems/omg.yml`, reading with
`--start latest`. The order is OMG's `test_order_payload.json` with fresh ids and canaries in its
ids and customer fields, posted straight to the API under a `traceparent` the test chose, so the
order's `SERVER` span is in a known trace. Staging answers the Helios call with a 500 locally,
and sf-publisher writes the pair to the bucket and names its keys in a `transmission.capture`
event on its `sf-publisher.business_logic` span.

The body's clock stands still until the test moves it past `paths.settle_s`, once no span of the
order's flow has arrived for `QUIET_S`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from canary import Canary
from spans import Clock

import fire.apps.incident_engine.app as app_module
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.apps.tentacle.runner import EXIT_OK, prepare, run
from fire.core.ports.record_source import StartPosition

OMG_YML = Path(__file__).resolve().parents[2] / "config" / "systems" / "omg.yml"
TOKEN = "capture-omg-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
TRACES, CAPTURES = "omg-traces", "transmissions"
CAPTURE_SETTLE_S = 3
CAPTURE_POLL_S = 1
BUCKET = "order-management-gateway-transmission-data-dev"
PREFIX = "springfield-payloads/"
API = "order-management-api"
ROUTE = "POST /v1/orders/place_order"
BUSINESS_LOGIC = "sf-publisher.business_logic"
HELIOS = "http:outbound:helios:estimate_place"
COUNTERS = "tentacle counters: "
# The listener runs once LocalStack polls the table's stream, then sf-publisher calls Helios.
CAPTURE_WAIT_S = 180.0
FLOW_WAIT_S = 120.0
QUIET_S = 15.0
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
        return [item for _at, item in self.timed(kind)]

    def timed(self, kind: str) -> list[tuple[float, dict[str, Any]]]:
        """Each posted item of `kind` with when its batch reached the body."""
        return [(at, item) for at, body in self.bodies for item in json.loads(body)[kind]]

    def arrivals(self, traces: set[str]) -> list[float]:
        """When each batch holding a span of `traces` reached the body."""
        return [
            at
            for at, body in self.bodies
            if any(span["trace_id"] in traces for span in json.loads(body)["spans"])
        ]


@dataclass
class Stack:
    system: dict[str, Any]
    transport: RecordingTransport
    clock: Clock
    stop: asyncio.Event
    running: asyncio.Task[int]


def _system_config() -> dict[str, Any]:
    """omg.yml with only its span receiver and its transmission source, which settles fast."""
    system = yaml.safe_load(OMG_YML.read_text(encoding="utf-8"))
    declared = {source["id"]: source for source in system["sources"]}
    captures = {**declared[CAPTURES], "settle_s": CAPTURE_SETTLE_S, "poll_s": CAPTURE_POLL_S}
    system["sources"] = [declared[TRACES], captures]
    del system["discovery"]
    return system


def _key(channel: dict[str, Any]) -> str:
    key = f"{channel['kind']}:{channel['direction']}:{channel['name']}:{channel['part']}"
    return f"{key}:{channel['status_class']}" if channel.get("status_class") else key


async def _until(what: str, condition: Callable[[], object], timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"{what}: not met within {timeout_s}s")
        await asyncio.sleep(0.5)


@pytest.fixture
async def stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Stack]:
    for name in ("AWS_PROFILE", "AWS_SESSION_TOKEN", "AWS_ENDPOINT_URL_S3"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(
        "AWS_ENDPOINT_URL", os.environ.get("OMG_AWS_ENDPOINT", "http://localhost:4566")
    )
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "aws-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "aws-credentials"))
    fire_yml = tmp_path / "config" / "fire.yml"
    (fire_yml.parent / "systems").mkdir(parents=True)
    fire_yml.write_text(yaml.safe_dump({"tentacle": {"flush_ms": 50}}), encoding="utf-8")
    system = _system_config()
    (fire_yml.parent / "systems" / "omg.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    clock = Clock(datetime.now(UTC))
    app_module._container = build_container(fire_yml, clock=clock)
    environ = {
        "FIRE_API_TOKEN": TOKEN,
        "FIRE_BODY_URL": "http://body",
        "OMG_OTLP_LISTEN": os.environ.get("OMG_OTLP_LISTEN", "127.0.0.1:4318"),
        "OMG_TRANSMISSION_BUCKET": os.environ.get("OMG_TRANSMISSION_BUCKET", BUCKET),
    }
    # The receiver binds its port here, so the collector's exports queue until `run` serves.
    plan = prepare("omg", start=StartPosition.LATEST, config_path=fire_yml, environ=environ)
    transport = RecordingTransport(httpx.ASGITransport(app=app))
    stop = asyncio.Event()
    running = asyncio.create_task(run(plan, stop, transport=transport))

    yield Stack(system, transport, clock, stop, running)

    if not running.done():
        stop.set()
        await asyncio.wait_for(running, 15)
    app_module._container = None


async def _place_order(canary: Canary) -> tuple[str, list[str]]:
    """Places a fresh order straight to OMG's API, with canaries in its ids and customer fields,
    under a trace of its own; returns that trace and the ids of the orders OMG placed.
    """
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
    api = os.environ.get("OMG_API_UPSTREAM", "http://localhost:8020")
    async with httpx.AsyncClient(base_url=api, trust_env=False, timeout=60) as client:
        for _ in range(ATTEMPTS):
            trace = secrets.token_hex(16)
            traceparent = f"00-{trace}-{secrets.token_hex(8)}-01"
            try:
                response = await client.post(
                    "/v1/orders/place_order", json=order, headers={"traceparent": traceparent}
                )
            except httpx.RemoteProtocolError:
                continue
            assert response.status_code == 201, (response.status_code, response.text[:300])
            return trace, [placed["id"] for placed in response.json()]
    raise AssertionError(f"OMG dropped the connection {ATTEMPTS} times in a row")


def _final_counters(messages: list[str]) -> dict[str, str]:
    """The last counters line of each source."""
    lines: dict[str, str] = {}
    for message in messages:
        if message.startswith(COUNTERS):
            source = message.removeprefix(f"{COUNTERS}source=").split(" ", 1)[0]
            lines[source] = message
    return lines


async def _body_get(path: str) -> dict[str, Any]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://body") as client:
        response = await client.get(path, headers=HEADERS)
    assert response.status_code == 200, response.status_code
    return response.json()


async def test_a_place_orders_helios_capture_joins_its_business_logic_span(
    stack: Stack,
    canary: Canary,
    caplog: pytest.LogCaptureFixture,
    capfd: pytest.CaptureFixture[str],
    quiet_httpx: None,
) -> None:
    caplog.set_level(logging.DEBUG)

    def say(line: str) -> None:
        with capfd.disabled():
            print(line, flush=True)

    say("")
    posted_at = datetime.now(UTC)
    started = time.monotonic()
    trace, order_ids = await _place_order(canary)
    say(f"order posted: 201 after {time.monotonic() - started:.1f}s, {len(order_ids)} order(s)")
    transport = stack.transport

    def server() -> dict[str, Any] | None:
        spans = transport.posted("spans")
        return next((s for s in spans if s["trace_id"] == trace and s["operation"] == ROUTE), None)

    def linked_traces() -> set[str]:
        """Traces holding a span that links to the trace of this order's `SERVER` span."""
        if server() is None:
            return set()
        return {
            span["trace_id"]
            for span in transport.posted("spans")
            if any(link["trace_id"] == trace for link in span["links"])
        }

    def business_logic() -> dict[tuple[str, str], dict[str, Any]]:
        traces = linked_traces()
        return {
            (span["trace_id"], span["span_id"]): span
            for span in transport.posted("spans")
            if span["operation"] == BUSINESS_LOGIC and span["trace_id"] in traces
        }

    def joined() -> dict[str, Any] | None:
        """The first Helios request shape joined to a business_logic span of this order."""
        spans = business_logic()
        return next(
            (
                shape
                for shape in transport.posted("shapes")
                if _key(shape["channel"]) == f"{HELIOS}:request"
                and (shape["trace_id"], shape["span_id"]) in spans
            ),
            None,
        )

    def progress() -> str:
        requests = [
            s for s in transport.posted("shapes") if _key(s["channel"]) == f"{HELIOS}:request"
        ]
        return (
            f"server span {'arrived' if server() else 'missing'}, "
            f"{len(linked_traces())} linked trace(s), {len(business_logic())} business_logic "
            f"span(s), {len(requests)} Helios request shape(s) of which "
            f"{sum(s['trace_id'] is not None for s in requests)} joined to a span"
        )

    try:
        await _until("a Helios request shape joined to this order's span", joined, CAPTURE_WAIT_S)
    except AssertionError as error:
        raise AssertionError(f"{error}: {progress()}") from None
    request = joined()
    assert request is not None
    ids = (request["trace_id"], request["span_id"])
    span = business_logic()[ids]
    response_id = request["observation_id"].removesuffix(":request") + ":response"
    await _until(
        "the pair's response shape",
        lambda: any(s["observation_id"] == response_id for s in transport.posted("shapes")),
        10,
    )
    [response] = [s for s in transport.posted("shapes") if s["observation_id"] == response_id]
    status_class = response["channel"]["status_class"]
    response_key = f"{HELIOS}:response:{status_class}"
    assert _key(response["channel"]) == response_key
    pattern = next(s for s in stack.system["sources"] if s["id"] == CAPTURES)["work_id_pattern"]
    for shape in (request, response):
        assert (shape["trace_id"], shape["span_id"]) == ids
        assert shape["work_id"] is not None
        assert re.fullmatch(pattern, shape["work_id"]), shape["work_id"]
    assert response["work_id"] == request["work_id"]

    span_at = min(at for at, s in transport.timed("spans") if (s["trace_id"], s["span_id"]) == ids)
    read_at = next(
        at
        for at, s in transport.timed("shapes")
        if s["observation_id"] == request["observation_id"]
    )
    written = datetime.fromisoformat(request["occurred_at"]) - posted_at
    say(f"business_logic span received: +{span_at - started:.1f}s")
    say(f"pair written (key timestamp): +{written.total_seconds():.1f}s")
    say(f"pair read and sent: +{read_at - started:.1f}s")
    say(f"Helios response status class: {status_class}")

    traces = {trace, *linked_traces()}
    await _until(
        f"no span of the flow for {QUIET_S}s",
        lambda: time.monotonic() - max(transport.arrivals(traces)) >= QUIET_S,
        FLOW_WAIT_S,
    )
    stack.stop.set()
    assert await asyncio.wait_for(stack.running, 15) == EXIT_OK
    arrivals = transport.arrivals(traces)
    say(f"flow spans reached the body over {max(arrivals) - min(arrivals):.1f}s")
    counters = _final_counters(caplog.messages)
    for source in (TRACES, CAPTURES):
        say(counters[source])
    assert "capture_indexed:" in counters[TRACES]

    stack.clock.advance(stack.system["paths"]["settle_s"] + 1)
    paths = await _body_get("/v0/systems/omg/paths")
    streams = await _body_get("/v0/systems/omg/streams")
    [path] = [p for p in paths["paths"] if p["entry"] == {"service": API, "operation": ROUTE}]
    [node] = [
        n
        for n in path["nodes"]
        if (n["service"], n["operation"]) == (span["service"], BUSINESS_LOGIC)
    ]
    assert {f"{HELIOS}:request", response_key} <= set(node["channels"]), node["channels"]
    assert {f"{HELIOS}:request", response_key} <= {s["key"] for s in streams["streams"]}

    captured = capfd.readouterr()
    places = {
        **{f"posted {index}": body for index, (_at, body) in enumerate(transport.bodies)},
        "streams": json.dumps(streams),
        "paths": json.dumps(paths),
        "logs": caplog.text,
        "stdout": captured.out,
        "stderr": captured.err,
    }
    held = ["s3://", PREFIX, *order_ids]
    for place, text in places.items():
        assert canary.leaks(text, envelope=False) == [], place
        assert [value for value in held if value in text] == [], place
