"""L2: OMG's order payloads posted through the inbound proxy in front of OMG's local API.

Needs OMG's local stack (dev/omg/README.md): the API at `OMG_API_UPSTREAM` (default
http://localhost:8020) and Jaeger's query API at `OMG_JAEGER_URL` (default
http://localhost:16686). The join to OMG's trace is checked in Jaeger: OMG's `SERVER` span for
`POST /v1/orders/place_order` must be a child of the span id the proxy's shapes carry.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

import fire.apps.incident_engine.app as app_module
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.apps.tentacle.runner import EXIT_OK, prepare, run

TOKEN = "proxy-omg-test-token"
ROUTE = "POST /v1/orders/place_order"
REQUEST = f"http:inbound:{ROUTE}:request"
HEALTH = "http:inbound:GET /healthcheck:response:2xx"
# The order payloads OMG's place_order takes, from scripts/test_payloads/.
PAYLOADS = (
    "test_order_payload.json",
    "test_springfield_order_payload.json",
    "test_rma_place_order_payload.json",
    "test_rma_exchange_lower_value.json",
)
JAEGER_WAIT_S = 60.0
# After a 500, OMG's local API closes the keep-alive connection without `Connection: close`,
# so a request sent on it in the next milliseconds is lost; the proxy answers it with this.
UPSTREAM_ABORTED = "upstream-aborted"
ATTEMPTS = 3

pytestmark = [pytest.mark.integration, pytest.mark.omg_local]


class RecordingTransport(httpx.AsyncBaseTransport):
    """Keeps every request body the sender posts, then hands the request to the body."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.bodies: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.bodies.append(request.content.decode())
        return await self.inner.handle_async_request(request)

    def keyed(self, key: str) -> list[dict[str, Any]]:
        shapes = [shape for body in self.bodies for shape in json.loads(body)["shapes"]]
        return [shape for shape in shapes if _key(shape["channel"]) == key]


def _key(channel: dict[str, Any]) -> str:
    key = f"{channel['kind']}:{channel['direction']}:{channel['name']}:{channel['part']}"
    return f"{key}:{channel['status_class']}" if channel.get("status_class") else key


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


async def _until(what: str, condition: Any, timeout_s: float, poll: Any = None) -> None:
    deadline = time.monotonic() + timeout_s
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"{what}: not met within {timeout_s}s")
        if poll is not None:
            await poll()
        await asyncio.sleep(0.2)


@pytest.fixture
async def proxied(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[tuple[str, RecordingTransport, asyncio.Event, asyncio.Task[int]]]:
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
            }
        ],
    }
    (fire_yml.parent / "systems" / "omg.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    app_module._container = build_container(fire_yml)
    environ = {
        "FIRE_API_TOKEN": TOKEN,
        "FIRE_BODY_URL": "http://body",
        "OMG_API_UPSTREAM": os.environ.get("OMG_API_UPSTREAM", "http://localhost:8020"),
    }
    transport = RecordingTransport(httpx.ASGITransport(app=app))
    stop = asyncio.Event()
    running = asyncio.create_task(
        run(prepare("omg", config_path=fire_yml, environ=environ), stop, transport=transport)
    )
    base = f"http://127.0.0.1:{port}"

    async with httpx.AsyncClient(base_url=base, trust_env=False, timeout=10) as client:

        async def ping() -> None:
            await client.get("/healthcheck")

        await _until("templates loaded", lambda: transport.keyed(HEALTH), 30, ping)

    yield base, transport, stop, running

    stop.set()
    await asyncio.wait_for(running, 15)
    app_module._container = None


async def _server_span_parents(jaeger: httpx.AsyncClient, trace_id: str) -> set[str]:
    """Parent span ids of the trace's spans named after the route."""
    response = await jaeger.get(f"/api/traces/{trace_id}")
    if response.status_code != 200:
        return set()
    spans = [span for trace in response.json().get("data") or [] for span in trace["spans"]]
    return {
        reference["spanID"]
        for span in spans
        if span["operationName"] == ROUTE
        for reference in span.get("references", [])
        if reference.get("refType") == "CHILD_OF"
    }


async def _wait_for_child(jaeger: httpx.AsyncClient, trace_id: str, span_id: str) -> None:
    """Polls Jaeger until the route's span in `trace_id` is a child of `span_id`."""
    parents: set[str] = set()

    async def fetch() -> None:
        parents.update(await _server_span_parents(jaeger, trace_id))

    await _until(f"OMG span under {span_id}", lambda: span_id in parents, JAEGER_WAIT_S, fetch)


async def _post_order(client: httpx.AsyncClient, payload: bytes) -> int:
    """Posts one order through the proxy, again when OMG dropped the connection under it; an
    exchange cut that way gives no shape.
    """
    for _ in range(ATTEMPTS):
        response = await client.post(
            "/v1/orders/place_order", content=payload, headers={"content-type": "application/json"}
        )
        if response.headers.get("x-fire-proxy") != UPSTREAM_ABORTED:
            return response.status_code
    raise AssertionError(f"OMG dropped the connection {ATTEMPTS} times in a row")


async def test_order_payloads_give_route_channels_joined_to_omg_traces(
    proxied: tuple[str, RecordingTransport, asyncio.Event, asyncio.Task[int]],
) -> None:
    base, transport, stop, running = proxied
    payloads = Path(os.environ["OMG_REPO"]) / "scripts" / "test_payloads"
    async with httpx.AsyncClient(base_url=base, trust_env=False, timeout=60) as client:
        statuses = [await _post_order(client, (payloads / name).read_bytes()) for name in PAYLOADS]
    await _until(
        f"{len(PAYLOADS)} request shapes sent",
        lambda: len(transport.keyed(REQUEST)) == len(PAYLOADS),
        15,
    )
    stop.set()
    assert await asyncio.wait_for(running, 15) == EXIT_OK

    requests = transport.keyed(REQUEST)
    responses = [
        shape
        for status in sorted(set(statuses))
        for shape in transport.keyed(f"http:inbound:{ROUTE}:response:{status // 100}xx")
    ]
    assert len(requests) == len(responses) == len(PAYLOADS)
    body = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=body, base_url="http://body") as client:
        streams = await client.get("/v0/systems/omg/streams", headers={"X-FIRE-Token": TOKEN})
    observed = {stream["key"]: stream["observations"] for stream in streams.json()["streams"]}
    assert observed[REQUEST] == len(PAYLOADS)
    jaeger_url = os.environ.get("OMG_JAEGER_URL", "http://localhost:16686")
    async with httpx.AsyncClient(base_url=jaeger_url, trust_env=False, timeout=10) as jaeger:
        for shape in requests:
            await _wait_for_child(jaeger, shape["trace_id"], shape["span_id"])


async def test_openapi_document_crosses_the_proxy_unchanged(
    proxied: tuple[str, RecordingTransport, asyncio.Event, asyncio.Task[int]],
) -> None:
    base = proxied[0]
    upstream = os.environ.get("OMG_API_UPSTREAM", "http://localhost:8020")
    results = []
    for url in (upstream, base):
        async with (
            httpx.AsyncClient(base_url=url, trust_env=False, timeout=30) as client,
            client.stream("GET", "/openapi.json", headers={"host": "omg.local"}) as response,
        ):
            body = b"".join([chunk async for chunk in response.aiter_raw()])
            headers = [(name, value) for name, value in response.headers.raw if name != b"date"]
            results.append((response.status_code, headers, body))

    assert results[0] == results[1]
