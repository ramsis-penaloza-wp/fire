"""L1: OMG's transmission captures through the tentacle into the body. Each capture fixture gives
its channels, and a Helios capture that the recorded place-order flow's `transmission.capture`
event names joins its span and feeds the work index, so a redelivery the event never named and a
WorkUnit, which carry the work unit alone, attach to the same flow.

The sources are the repo's omg.yml `omg-traces`, `transmissions`, and `work-unit-samples`, written
to `tmp_path` with a fast poll. S3 is moto's. The recording is
`tests/fixtures/omg/otlp/place_order_1`, with the flow's capture event pointed at a fixture pair;
its orphan keeps its own event, whose scrubbed URIs index the bare key `redacted`. The body runs
in-process through `httpx.ASGITransport` on a hand-moved clock. A test waits for each source's
counters line, then stops the tentacle.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import boto3
import httpx
import pytest
import yaml
from moto import mock_aws
from spans import Clock

import fire.apps.incident_engine.app as app_module
from fire.apps.config import OtlpSourceConfig, load_system_config
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.apps.tentacle.runner import EXIT_OK, prepare, run
from fire.core.ports.record_source import StartPosition

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "tests" / "fixtures" / "omg"
CAPTURES = FIXTURES / "captures"
RECORDING = FIXTURES / "otlp" / "place_order_1"
WORK_UNIT = FIXTURES / "test_payloads" / "sales_order_processor_order_charged_work_unit.json"
OMG = load_system_config(REPO / "config" / "systems" / "omg.yml")
SOURCES = {source.id: source for source in OMG.sources}
TRACES, TRANSMISSIONS, SAMPLES = "omg-traces", "transmissions", "work-unit-samples"
TRACES_CONFIG = SOURCES[TRACES]
assert isinstance(TRACES_CONFIG, OtlpSourceConfig) and TRACES_CONFIG.capture_event is not None
CAPTURE_EVENT = TRACES_CONFIG.capture_event
SETTLE = OMG.paths.settle_s
TOKEN = "place-order-captures-token"
HEADERS = {"X-FIRE-Token": TOKEN}
BUCKET = "order-management-gateway-transmission-data-dev"
REQUESTS, RESPONSES = "requests", "responses"
# The work unit of helios/002 and helios/003, a redelivery of the same order.
REDELIVERED = "0FAKE000000000000000000002"
API = "order-management-api"
SF = "order-management-gateway-sf-publisher"
ROUTE = "POST /v1/orders/place_order"
LISTENER = "dynamo_listener.distribute_work"
BUSINESS = "sf-publisher.business_logic"
HELIOS = "http:outbound:helios:estimate_place"
ORACLE = "http:outbound:oracle-om:create"
INSURANCE = "http:outbound:insurance:calculate_contracted_loss"
ORDER_CHARGED = "event:internal:order.charged:payload"
# The place-order flow's 30 spans and the orphan retry's 12.
RECORDED_SPANS = 42
FIXTURE_PAIRS = [
    "helios/001",
    "helios/002",
    "helios/003",
    "oracle-om/001",
    "oracle-om/002",
    "insurance/001",
]

Ids = tuple[str | None, str | None, str | None]


def _fixture(name: str) -> dict[str, Any]:
    return json.loads((CAPTURES / f"{name}.json").read_text(encoding="utf-8"))


def _key(pair: Mapping[str, Any], part: str) -> str:
    return f"{pair['prefix']}{part}/{pair['name']}"


def _pair_id(pair: Mapping[str, Any], label: str, action: str, work_id: str | None) -> str:
    """`{label}:{work_id or -}:{action}:{ts}`, the `ts` from the pair's name."""
    stem = pair["name"].rsplit("/", 1)[1].removesuffix(".json")
    stamp = "_".join(stem.rsplit("_", 3)[1:])
    return f"{label}:{work_id or '-'}:{action}:{stamp}"


def _ids(shape: Mapping[str, Any]) -> Ids:
    return (shape["trace_id"], shape["span_id"], shape["work_id"])


def _spans(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        item
        for document in documents
        for resource in document["resourceSpans"]
        for scope in resource["scopeSpans"]
        for item in scope["spans"]
    ]


def _pointed_recording(pair: Mapping[str, Any]) -> tuple[list[dict[str, Any]], tuple[str, str]]:
    """The recording's requests, in order, with the flow's capture event naming `pair`, and the
    trace and span ids of the span that carries it.

    The flow's event is on sf-publisher's business logic span in the listener's trace; the
    orphan's event, in another trace, is left as recorded.
    """
    documents = [
        json.loads(path.read_text(encoding="utf-8")) for path in sorted(RECORDING.glob("*.json"))
    ]
    spans = _spans(documents)
    [listener_trace] = {item["traceId"] for item in spans if item["name"] == LISTENER}
    captured = [
        item
        for item in spans
        if any(event["name"] == CAPTURE_EVENT.name for event in item.get("events", []))
    ]
    assert len(captured) == 2
    [business] = [item for item in captured if item["traceId"] == listener_trace]
    assert business["name"] == BUSINESS
    [event] = [event for event in business["events"] if event["name"] == CAPTURE_EVENT.name]
    uris = {
        CAPTURE_EVENT.request_uri: f"s3://{BUCKET}/{_key(pair, REQUESTS)}",
        CAPTURE_EVENT.response_uri: f"s3://{BUCKET}/{_key(pair, RESPONSES)}",
    }
    for attribute in event["attributes"]:
        if attribute["key"] in uris:
            attribute["value"] = {"stringValue": uris.pop(attribute["key"])}
    assert uris == {}
    return documents, (business["traceId"], business["spanId"])


def _system(*, capture_event: bool) -> dict[str, Any]:
    """The repo's omg.yml sources the tests run, with a fast `transmissions` poll."""
    traces = TRACES_CONFIG.model_dump(mode="json", exclude_none=True)
    if not capture_event:
        del traces["capture_event"]
    transmissions = (
        SOURCES[TRANSMISSIONS]
        .model_copy(update={"poll_s": 0.05, "settle_s": 0})
        .model_dump(mode="json", exclude_none=True)
    )
    samples = SOURCES[SAMPLES].model_dump(mode="json", exclude_none=True)
    return {
        "system": OMG.system,
        "display_name": OMG.display_name,
        "notify": OMG.notify.model_dump(mode="json", exclude_none=True),
        "sources": [traces, transmissions, samples],
        "fingerprinting": OMG.fingerprinting.model_dump(mode="json"),
        "drift": OMG.drift.model_dump(mode="json"),
        "paths": OMG.paths.model_dump(mode="json"),
    }


def _write_config(directory: Path, *, capture_event: bool) -> Path:
    fire_yml = directory / "fire.yml"
    (directory / "systems").mkdir(parents=True)
    fire_yml.write_text(yaml.safe_dump({"tentacle": {"flush_ms": 10}}), encoding="utf-8")
    system = yaml.safe_dump(_system(capture_event=capture_event))
    (directory / "systems" / f"{OMG.system}.yml").write_text(system, encoding="utf-8")
    return fire_yml


class RecordingTransport(httpx.AsyncBaseTransport):
    """Keeps every request body the sender posts, then hands the request to the body."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.bodies: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.bodies.append(request.content.decode())
        return await self.inner.handle_async_request(request)

    def posted(self, kind: str) -> list[dict[str, Any]]:
        return [item for body in self.bodies for item in json.loads(body)[kind]]


@dataclass
class Tentacle:
    receiver: str
    transport: RecordingTransport
    stop: asyncio.Event
    running: asyncio.Task[int]

    async def post(self, document: Any) -> httpx.Response:
        async with httpx.AsyncClient(trust_env=False, timeout=10) as client:
            return await client.post(
                f"{self.receiver}/v1/traces",
                content=json.dumps(document),
                headers={"content-type": "application/json"},
            )

    def shapes(self) -> dict[str, dict[str, Any]]:
        """The posted shapes by observation id."""
        return {shape["observation_id"]: shape for shape in self.transport.posted("shapes")}

    async def finish(self) -> int:
        self.stop.set()
        return await asyncio.wait_for(self.running, 10)


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@dataclass
class Harness:
    """The bucket, the body on its clock, and tentacles started on them."""

    tmp_path: Path
    fire_yml: Path
    s3: Any
    clock: Clock
    caplog: pytest.LogCaptureFixture
    started: list[Tentacle] = field(default_factory=list)

    def put_pair(self, pair: Mapping[str, Any]) -> None:
        """Writes the request, unless null, then the response, each with its metadata."""
        for part, half in ((REQUESTS, pair["request"]), (RESPONSES, pair["response"])):
            if half is not None:
                self.s3.put_object(
                    Bucket=BUCKET,
                    Key=_key(pair, part),
                    Body=json.dumps(half["document"]).encode(),
                    Metadata=half["metadata"],
                )

    async def start(self, *, source: str | None = None, capture_event: bool = True) -> Tentacle:
        """A tentacle reading every source, or only `source`, from the oldest object."""
        fire_yml = self.fire_yml
        if not capture_event:
            fire_yml = _write_config(self.tmp_path / "without-capture-event", capture_event=False)
        work_unit = json.loads(WORK_UNIT.read_text(encoding="utf-8"))
        work_unit["work_unit_id"] = REDELIVERED
        replay = self.tmp_path / "work_unit.json"
        replay.write_text(json.dumps(work_unit), encoding="utf-8")
        port = _free_port()
        environ = {
            "FIRE_API_TOKEN": TOKEN,
            "FIRE_BODY_URL": "http://body",
            SOURCES[TRACES].location_env: f"127.0.0.1:{port}",
            SOURCES[TRANSMISSIONS].location_env: BUCKET,
            SOURCES[SAMPLES].location_env: str(replay),
        }
        plan = prepare(
            OMG.system,
            source=source,
            start=StartPosition.OLDEST,
            config_path=fire_yml,
            environ=environ,
        )
        transport = RecordingTransport(httpx.ASGITransport(app=app))
        stop = asyncio.Event()
        running = asyncio.create_task(
            run(plan, stop, transport=transport, counters_interval_s=0.05)
        )
        tentacle = Tentacle(f"http://127.0.0.1:{port}", transport, stop, running)
        self.started.append(tentacle)
        return tentacle

    async def counted(self, source: str, read: int, *parts: str, timeout_s: float = 10) -> None:
        """Waits for a counters line of `source` with `read` records, none pending, and every
        part.
        """
        prefix = f"tentacle counters: source={source} read={read} "

        def done() -> bool:
            return any(
                message.startswith(prefix)
                and " pending=0 " in message
                and all(part in message for part in parts)
                for message in self.caplog.messages
            )

        async with asyncio.timeout(timeout_s):
            while not done():
                await asyncio.sleep(0.02)


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Fake credentials; no endpoint, profile, or AWS file from outside the repo."""
    for name in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_PROFILE", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "aws-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "aws-credentials"))


@pytest.fixture
async def harness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    aws: None,
) -> AsyncIterator[Harness]:
    caplog.set_level(logging.INFO)
    fire_yml = _write_config(tmp_path / "config", capture_event=True)
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    clock = Clock()
    app_module._container = build_container(fire_yml, clock=clock)
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET)
        harness = Harness(tmp_path, fire_yml, s3, clock, caplog)
        yield harness
        for tentacle in harness.started:
            if not tentacle.running.done():
                await tentacle.finish()
        # S3 calls still in worker threads end inside the mock.
        await asyncio.get_running_loop().shutdown_default_executor()
    app_module._container = None


async def _get(path: str) -> dict[str, Any]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://body") as client:
        response = await client.get(path, headers=HEADERS)
    assert response.status_code == 200
    return response.json()


async def _streams() -> dict[str, Any]:
    body = await _get(f"/v0/systems/{OMG.system}/streams")
    return {stream["key"]: stream for stream in body["streams"]}


async def _settled_path(clock: Clock) -> tuple[dict[str, Any], dict[str, Any]]:
    """Moves the body's clock by `paths.settle_s`; returns `/paths` and its one path."""
    clock.advance(SETTLE)
    report = await _get(f"/v0/systems/{OMG.system}/paths")
    [path] = report["paths"]
    assert path["entry"] == {"service": API, "operation": ROUTE}
    assert (path["flows"], report["open_flows"]) == (1, 0)
    return report, path


def _node(path: Mapping[str, Any], service: str, operation: str) -> dict[str, Any]:
    [found] = [
        node
        for node in path["nodes"]
        if (node["service"], node["operation"]) == (service, operation)
    ]
    return found


def _counters(unattached: int) -> dict[str, int]:
    """The recording's flow counters: its orphan retry, and `unattached` shapes."""
    return {
        "orphan_flows": 1,
        "evicted_flows": 0,
        "late_spans": 0,
        "late_shapes": 0,
        "unattached_shapes": unattached,
        "fan_in_links": 0,
    }


@pytest.mark.integration
async def test_each_capture_fixture_gives_its_channels(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    for name in FIXTURE_PAIRS:
        harness.put_pair(_fixture(name))
    tentacle = await harness.start(source=TRANSMISSIONS)

    await harness.counted(TRANSMISSIONS, len(FIXTURE_PAIRS))
    assert await tentacle.finish() == EXIT_OK

    streams = await _streams()
    assert {key: stream["observations"] for key, stream in streams.items()} == {
        f"{HELIOS}:request": 3,
        f"{HELIOS}:response:2xx": 1,
        f"{HELIOS}:response:5xx": 2,
        f"{INSURANCE}:request": 1,
        f"{INSURANCE}:response:2xx": 1,
        f"{ORACLE}:request": 2,
        f"{ORACLE}:response:2xx": 1,
        f"{ORACLE}:response:none": 1,
    }
    assert {stream["source"] for stream in streams.values()} == {TRANSMISSIONS}
    [helios_request] = streams[f"{HELIOS}:request"]["fingerprints"]
    assert helios_request["observations"] == 3

    shapes = tentacle.shapes()
    unattributed = _pair_id(_fixture("oracle-om/002"), "oracle-om", "create", None)
    request, response = shapes[f"{unattributed}:request"], shapes[f"{unattributed}:response"]
    assert {_ids(request), _ids(response)} == {(None, None, None)}
    assert response["channel"]["status_class"] == "none"
    assert response["shape"] == {"fields": []}
    assert request["shape"]["fields"] != []
    attributed = {shape["work_id"] for key, shape in shapes.items() if unattributed not in key}
    assert attributed == {
        "0FAKE000000000000000000001",
        REDELIVERED,
    }
    assert {shape["trace_id"] for shape in shapes.values()} == {None}
    assert (
        f"source={TRANSMISSIONS} read=6 built=12 sent=12 unsent=0 pending=0 rejected=- "
        "skipped=- noted=unjoined:6 body_duplicate=0 body_rejected=-"
    ) in caplog.text


async def _run_redelivery(harness: Harness, *, capture_event: bool) -> tuple[Tentacle, Ids]:
    """The recording with its capture event naming helios/002, posted to the receiver; then
    helios/002 and helios/003 written in turn, and the WorkUnit of their work unit replayed.

    Returns the tentacle, stopped, and the ids helios/002's shapes carry when it is joined.
    """
    joined = _fixture("helios/002")
    documents, (trace, span) = _pointed_recording(joined)
    tentacle = await harness.start(capture_event=capture_event)

    for document in documents:
        response = await tentacle.post(document)
        assert (response.status_code, response.json()) == (200, {})
    indexed = "noted=capture_indexed:4 " if capture_event else "noted=- "
    await harness.counted(TRACES, RECORDED_SPANS, indexed)
    harness.put_pair(joined)
    await harness.counted(TRANSMISSIONS, 1)
    harness.put_pair(_fixture("helios/003"))
    await harness.counted(TRANSMISSIONS, 2)
    await harness.counted(SAMPLES, 1)
    assert await tentacle.finish() == EXIT_OK
    return tentacle, (trace, span, REDELIVERED)


def _redelivery_ids() -> tuple[str, str]:
    return (
        _pair_id(_fixture("helios/002"), "helios", "estimate_place", REDELIVERED),
        _pair_id(_fixture("helios/003"), "helios", "estimate_place", REDELIVERED),
    )


@pytest.mark.integration
async def test_a_joined_capture_feeds_the_work_index(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    tentacle, joined_ids = await _run_redelivery(harness, capture_event=True)

    joined, unjoined = _redelivery_ids()
    shapes = tentacle.shapes()
    assert sorted(shapes) == sorted(
        [
            REDELIVERED,
            *(f"{pair}:{part}" for pair in (joined, unjoined) for part in ("request", "response")),
        ]
    )
    assert {_ids(shapes[f"{joined}:{part}"]) for part in ("request", "response")} == {joined_ids}
    assert {_ids(shapes[f"{unjoined}:{part}"]) for part in ("request", "response")} == {
        (None, None, REDELIVERED)
    }
    assert _ids(shapes[REDELIVERED]) == (None, None, REDELIVERED)
    assert shapes[REDELIVERED]["channel"]["name"] == "order.charged"
    assert (
        f"source={TRACES} read={RECORDED_SPANS} built={RECORDED_SPANS} sent={RECORDED_SPANS} "
        "unsent=0 pending=0 rejected=- skipped=- noted=capture_indexed:4 body_duplicate=0 "
        "body_rejected=-"
    ) in caplog.text
    assert (
        f"source={TRANSMISSIONS} read=2 built=4 sent=4 unsent=0 pending=0 rejected=- skipped=- "
        "noted=unjoined:1 body_duplicate=0 body_rejected=-"
    ) in caplog.text
    assert (
        f"source={SAMPLES} read=1 built=1 sent=1 unsent=0 pending=0 rejected=- skipped=- "
        "noted=- body_duplicate=0 body_rejected=-"
    ) in caplog.text

    report, path = await _settled_path(harness.clock)

    assert report["counters"] == _counters(unattached=0)
    business = _node(path, SF, BUSINESS)
    assert business["channels"] == {f"{HELIOS}:request": 1, f"{HELIOS}:response:5xx": 1}
    assert [node for node in path["nodes"] if node["channels"]] == [business]
    assert path["flow_channels"] == {
        ORDER_CHARGED: 1,
        f"{HELIOS}:request": 1,
        f"{HELIOS}:response:5xx": 1,
    }


@pytest.mark.integration
async def test_without_the_capture_event_shapes_wait_and_count_unattached(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    tentacle, _ = await _run_redelivery(harness, capture_event=False)

    shapes = tentacle.shapes()
    # Two shapes from each of the two pairs, and the WorkUnit's.
    assert len(shapes) == 5
    assert {_ids(shape) for shape in shapes.values()} == {(None, None, REDELIVERED)}
    assert (
        f"source={TRACES} read={RECORDED_SPANS} built={RECORDED_SPANS} sent={RECORDED_SPANS} "
        "unsent=0 pending=0 rejected=- skipped=- noted=- body_duplicate=0 body_rejected=-"
    ) in caplog.text
    assert (
        f"source={TRANSMISSIONS} read=2 built=4 sent=4 unsent=0 pending=0 rejected=- skipped=- "
        "noted=unjoined:2 body_duplicate=0 body_rejected=-"
    ) in caplog.text

    report, path = await _settled_path(harness.clock)

    assert report["counters"] == _counters(unattached=5)
    assert _node(path, SF, BUSINESS)["channels"] == {}
    assert [node for node in path["nodes"] if node["channels"]] == []
    assert path["flow_channels"] == {}
