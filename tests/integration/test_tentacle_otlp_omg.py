"""L2: OMG's place-order flow reaches the tentacle's OTLP receiver, and Jaeger keeps every trace.

Needs OMG's local stack with FIRE's collector override (dev/omg/README.md, section a): the API
at `OMG_API_UPSTREAM` (default http://localhost:8020), Jaeger's query API at `OMG_JAEGER_URL`
(default http://localhost:16686), and the collector exporting to `OMG_OTLP_LISTEN` (default
127.0.0.1:4318), where this test runs the receiver. Each order is OMG's `test_order_payload.json`
posted as `make test-order` posts it (`scripts/call_order_api.py`), with fresh order ids: with
the committed ids OMG returns the order it already placed, which writes nothing and starts no
listener. A `traceparent` with a known trace id finds the order's trace.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import socket
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
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

TOKEN = "otlp-omg-test-token"
ROUTE = "POST /v1/orders/place_order"
LISTENER = "dynamo_listener.distribute_work"
# OMG's SDKs export every 5 s; the listener runs once LocalStack polls the table's stream.
FLOW_WAIT_S = 120.0
JAEGER_WAIT_S = 60.0

pytestmark = [pytest.mark.integration, pytest.mark.omg_local]


class RecordingTransport(httpx.AsyncBaseTransport):
    """Keeps every request body the sender posts, then hands the request to the body."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.bodies: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.bodies.append(request.content.decode())
        return await self.inner.handle_async_request(request)

    def spans(self) -> list[dict[str, Any]]:
        return [span for body in self.bodies for span in json.loads(body)["spans"]]


@dataclass
class Traced:
    transport: RecordingTransport
    stop: asyncio.Event
    running: asyncio.Task[int]


def _listen_address() -> str:
    return os.environ.get("OMG_OTLP_LISTEN", "127.0.0.1:4318")


async def _until(what: str, condition: Callable[[], object], timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"{what}: not met within {timeout_s}s")
        await asyncio.sleep(0.5)


@pytest.fixture
async def traced(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Traced]:
    fire_yml = tmp_path / "config" / "fire.yml"
    (fire_yml.parent / "systems").mkdir(parents=True)
    fire_yml.write_text(yaml.safe_dump({"tentacle": {"flush_ms": 50}}), encoding="utf-8")
    system = {
        "system": "omg",
        "display_name": "Order Management Gateway",
        "notify": {"slack": {"channel_id": "C0000000000"}},
        "sources": [{"id": "omg-traces", "kind": "otlp", "listen_env": "OMG_OTLP_LISTEN"}],
    }
    (fire_yml.parent / "systems" / "omg.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    app_module._container = build_container(fire_yml)
    environ = {
        "FIRE_API_TOKEN": TOKEN,
        "FIRE_BODY_URL": "http://body",
        "OMG_OTLP_LISTEN": _listen_address(),
    }
    transport = RecordingTransport(httpx.ASGITransport(app=app))
    stop = asyncio.Event()
    running = asyncio.create_task(
        run(prepare("omg", config_path=fire_yml, environ=environ), stop, transport=transport)
    )

    yield Traced(transport, stop, running)

    if not running.done():
        stop.set()
        await asyncio.wait_for(running, 15)
    app_module._container = None


async def _place_order(canary: Canary) -> str:
    """Places a fresh order as `make test-order` does, with canaries in its ids and in the
    customer's names, email, and address; returns its trace id.
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
    trace_id, parent_id = secrets.token_hex(16), secrets.token_hex(8)
    upstream = os.environ.get("OMG_API_UPSTREAM", "http://localhost:8020")
    async with httpx.AsyncClient(base_url=upstream, trust_env=False, timeout=60) as client:
        response = await client.post(
            "/v1/orders/place_order",
            json=order,
            headers={"traceparent": f"00-{trace_id}-{parent_id}-01"},
        )
    assert response.status_code == 201, response.status_code
    return trace_id


def _find(spans: list[dict[str, Any]], **fields: Any) -> dict[str, Any] | None:
    return next(
        (span for span in spans if all(span[key] == value for key, value in fields.items())),
        None,
    )


async def _in_jaeger(trace_id: str) -> bool:
    jaeger = os.environ.get("OMG_JAEGER_URL", "http://localhost:16686")
    async with httpx.AsyncClient(base_url=jaeger, trust_env=False, timeout=10) as client:
        response = await client.get(f"/api/traces/{trace_id}")
    if response.status_code != 200:
        return False
    return any(trace["spans"] for trace in response.json().get("data") or [])


async def _wait_for_jaeger(trace_id: str) -> None:
    deadline = time.monotonic() + JAEGER_WAIT_S
    while not await _in_jaeger(trace_id):
        if time.monotonic() > deadline:
            raise AssertionError(f"trace {trace_id} not in Jaeger within {JAEGER_WAIT_S}s")
        await asyncio.sleep(0.5)


async def test_a_place_order_flow_reaches_the_tentacle_and_jaeger(
    traced: Traced, canary: Canary, caplog: pytest.LogCaptureFixture, quiet_httpx: None
) -> None:
    caplog.set_level(logging.DEBUG)
    trace_id = await _place_order(canary)

    def server() -> dict[str, Any] | None:
        return _find(traced.transport.spans(), operation=ROUTE, trace_id=trace_id)

    def listener() -> dict[str, Any] | None:
        found = server()
        if found is None:
            return None
        link = {"trace_id": trace_id, "span_id": found["span_id"]}
        return next(
            (
                s
                for s in traced.transport.spans()
                if s["operation"] == LISTENER and link in s["links"]
            ),
            None,
        )

    def sns() -> dict[str, Any] | None:
        found = listener()
        if found is None:
            return None
        return next(
            (
                s
                for s in traced.transport.spans()
                if s["operation"].startswith("SNS.Publish ") and s["trace_id"] == found["trace_id"]
            ),
            None,
        )

    await _until(
        "the order's SERVER span, its listener, and the listener's SNS span, from the collector",
        sns,
        FLOW_WAIT_S,
    )
    traced.stop.set()
    assert await asyncio.wait_for(traced.running, 15) == EXIT_OK

    spans = traced.transport.spans()
    root = server()
    assert root is not None and root["kind"] == "server"
    children = [span for span in spans if span["parent_span_id"] == root["span_id"]]
    dynamodb = {
        span["operation"] for span in children if span["operation"].startswith("DynamoDB.")
    }
    assert "DynamoDB.TransactWriteItems" in dynamodb
    assert {span["trace_id"] for span in children} == {trace_id}
    publish = sns()
    assert publish is not None and publish["kind"] == "producer"
    for body in traced.transport.bodies:
        assert canary.leaks(body, envelope=False) == []
    assert canary.leaks(caplog.text, envelope=False) == []
    for received in sorted({span["trace_id"] for span in spans}):
        await _wait_for_jaeger(received)


async def test_jaeger_receives_every_trace_with_the_tentacle_stopped(canary: Canary) -> None:
    host, _, port = _listen_address().rpartition(":")
    with socket.socket() as probe:
        # Connections the receiver closed may linger in TIME_WAIT; only a listener matters.
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind((host, int(port)))
        except OSError:
            pytest.fail(f"something listens on {host}:{port}; stop the tentacle first")
    trace_id = await _place_order(canary)
    await _wait_for_jaeger(trace_id)
