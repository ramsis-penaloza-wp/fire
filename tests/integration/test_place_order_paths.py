"""L1: OMG's recorded place-order flow gives one path, and copies of it with a hop removed, added,
or failing raise path drift.

The recordings are `tests/fixtures/omg/otlp/place_order_1` and `place_order_2` (step 1.4),
mapped to `SpanObservation`s by the tentacle's own `parse_traces` and `SpanPipeline`. Each holds
the place-order flow, the API's trace and the listener's joined by the listener's link, and an
sf-publisher retry of an earlier work unit whose parent never arrives: an orphan. Copies get
fresh ids, so the body sees new flows. The body's clock is moved by hand.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Callable, Iterator
from functools import cache
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient
from spans import Clock

import fire.apps.incident_engine.app as app_module
from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.adapters.sources.otlp import parse_traces
from fire.apps.config import load_system_config
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.core.domain.dossier import Dossier
from fire.core.domain.incident import Incident, Trigger
from fire.core.domain.operation import OperationNamer
from fire.core.services.span_pipeline import SpanPipeline

REPO = Path(__file__).resolve().parents[2]
RECORDINGS = REPO / "tests" / "fixtures" / "omg" / "otlp"
OMG_YML = REPO / "config" / "systems" / "omg.yml"
# The thresholds the real omg.yml declares.
DRIFT = yaml.safe_load(OMG_YML.read_text(encoding="utf-8"))["drift"]
MIN_SAMPLES = DRIFT["min_samples"]
SETTLE = 60
TOKEN = "place-order-paths-token"
HEADERS = {"X-FIRE-Token": TOKEN}
SOURCE = "omg-traces"
API = "order-management-api"
DISTRIBUTOR = "order-management-gateway-work-distributor"
SF = "order-management-gateway-sf-publisher"
ROUTE = "POST /v1/orders/place_order"
LISTENER = "dynamo_listener.distribute_work"
PUBLISH = "SNS.Publish omg-springfield-events-dev"
PROCESS = "sf-publisher.process_work_unit"
BUSINESS = "sf-publisher.business_logic"
WRITE = "DynamoDB.PutItem omg-dev-order_domain"
HELIOS = "POST helios-store.us-east-1.pvt.gateway.stage.warby.io/api/v4/estimate/place"
PAYMENTS = (
    "POST payment-services-payments-api.us-east-1.pvt.gateway.stage.warby.io"
    "/v1/payments/order-payments/create"
)
# Not in any recording: a partner call the processor never made.
NEW_CALL = "POST partner.example.com/v1/orders"
NOTIFY = "[fire] notify incident="
# Per recording: 27 nodes and 26 edges, and 29 hops in the flow's process path.
NODES, EDGES, HOPS = 27, 26, 29
# sf-publisher's nodes in the listener's trace: its root, three steps, and six calls.
SF_NODES = 10

Run = tuple[Incident, Dossier]
Spans = list[dict[str, Any]]


@cache
def _recorded(name: str) -> tuple[dict[str, Any], ...]:
    """One recording's spans as the tentacle sends them, in the order the collector sent them."""
    pipeline = SpanPipeline(
        system="omg",
        source=SOURCE,
        sample_rate=1.0,
        namer=OperationNamer(load_system_config(OMG_YML).operations),
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
    )
    spans: Spans = []
    for path in sorted((RECORDINGS / name).glob("*.json")):
        for raw in parse_traces(json.loads(path.read_text(encoding="utf-8"))):
            outcome = pipeline.process(raw)
            assert outcome.span is not None
            spans.append(outcome.span.model_dump(mode="json"))
    return tuple(spans)


def _copy(number: int, name: str = "place_order_1") -> Spans:
    """The recording with fresh ids: within one copy, each id always maps to the same new id."""

    def fresh(value: str) -> str:
        return hashlib.sha256(f"{number}:{value}".encode()).hexdigest()[: len(value)]

    spans = copy.deepcopy(list(_recorded(name)))
    for item in spans:
        item["trace_id"] = fresh(item["trace_id"])
        item["span_id"] = fresh(item["span_id"])
        if item["parent_span_id"] is not None:
            item["parent_span_id"] = fresh(item["parent_span_id"])
        item["links"] = [
            {"trace_id": fresh(link["trace_id"]), "span_id": fresh(link["span_id"])}
            for link in item["links"]
        ]
    return spans


def _one(spans: Spans, operation: str) -> dict[str, Any]:
    [found] = [item for item in spans if item["operation"] == operation]
    return found


def _without_helios(spans: Spans) -> Spans:
    return [item for item in spans if item["operation"] != HELIOS]


def _without_the_processor(spans: Spans) -> Spans:
    """Without sf-publisher's spans in the listener's trace: the processor never ran."""
    trace = _one(spans, LISTENER)["trace_id"]
    return [item for item in spans if not (item["service"] == SF and item["trace_id"] == trace)]


def _flow_traces(spans: Spans) -> list[str]:
    """The place-order flow's traces, sorted: the API's and the listener's."""
    return sorted({_one(spans, ROUTE)["trace_id"], _one(spans, LISTENER)["trace_id"]})


def _with_a_new_call(spans: Spans) -> Spans:
    """With one more client call under sf-publisher's business logic in the listener's trace."""
    trace = _one(spans, LISTENER)["trace_id"]
    [business] = [
        item for item in spans if item["operation"] == BUSINESS and item["trace_id"] == trace
    ]
    call = {
        **business,
        "span_id": hashlib.sha256(business["span_id"].encode()).hexdigest()[:16],
        "parent_span_id": business["span_id"],
        "kind": "client",
        "operation": NEW_CALL,
        "status_code": 200,
        "error": False,
    }
    return [*spans, call]


def _with_payments_failing(spans: Spans) -> Spans:
    return [
        {**item, "error": True, "status_code": 502} if item["operation"] == PAYMENTS else item
        for item in spans
    ]


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: Clock) -> Iterator[TestClient]:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    (tmp_path / "systems").mkdir()
    system = {
        "system": "omg",
        "display_name": "Order Management Gateway",
        "notify": {"slack": {"channel_id": "C0000000000"}},
        "sources": [{"id": SOURCE, "kind": "otlp", "listen_env": "OMG_OTLP_LISTEN"}],
        "drift": DRIFT,
        "paths": {"settle_s": SETTLE},
    }
    (tmp_path / "systems" / "omg.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    app_module._container = build_container(tmp_path / "fire.yml", clock=clock)
    yield TestClient(app)
    app_module._container = None


@pytest.fixture
def runs(monkeypatch: pytest.MonkeyPatch) -> list[Run]:
    """Incidents and dossiers handed to the background pipeline, in order."""
    captured: list[Run] = []

    async def capture(incident: Incident, dossier: Dossier) -> None:
        captured.append((incident, dossier))

    monkeypatch.setattr(app_module, "_run_pipeline", capture)
    return captured


def _settle(client: TestClient, clock: Clock, spans: Spans) -> dict[str, Any]:
    """Posts `spans`, lets their flows settle, and returns `/paths`."""
    response = client.post("/v0/observations", json={"spans": spans}, headers=HEADERS)
    assert response.status_code == 202
    assert response.json()["spans"] == {"accepted": len(spans), "duplicate": 0, "rejected": []}
    clock.advance(SETTLE)
    paths = client.get("/v0/systems/omg/paths", headers=HEADERS)
    assert paths.status_code == 200
    return paths.json()


def _warm(client: TestClient, clock: Clock) -> None:
    """`MIN_SAMPLES` copies, alternating the two recordings."""
    for number in range(MIN_SAMPLES):
        body = _settle(client, clock, _copy(number, f"place_order_{number % 2 + 1}"))
    [path] = body["paths"]
    assert (path["status"], path["flows"]) == ("warm", MIN_SAMPLES)


def _named(path: dict[str, Any]) -> tuple[set[tuple[str, str]], set[tuple[Any, ...]]]:
    nodes = [(node["service"], node["operation"]) for node in path["nodes"]]
    edges = {(nodes[edge["from"]], nodes[edge["to"]], edge["via"]) for edge in path["edges"]}
    return set(nodes), edges


@pytest.mark.integration
@pytest.mark.parametrize("name", ["place_order_1", "place_order_2"])
def test_a_recorded_place_order_gives_one_path_across_the_listeners_link(
    client: TestClient, clock: Clock, name: str
) -> None:
    body = _settle(client, clock, list(_recorded(name)))

    assert body["open_flows"] == 0
    assert body["counters"]["orphan_flows"] == 1
    assert {key: value for key, value in body["counters"].items() if key != "orphan_flows"} == {
        "evicted_flows": 0,
        "late_spans": 0,
        "late_shapes": 0,
        "unattached_shapes": 0,
        "fan_in_links": 0,
    }
    [path] = body["paths"]
    assert path["entry"] == {"service": API, "operation": ROUTE}
    nodes, edges = _named(path)
    assert (len(nodes), len(edges)) == (NODES, EDGES)
    assert {
        ((API, ROUTE), (API, WRITE), "child"),
        ((API, ROUTE), (DISTRIBUTOR, LISTENER), "link"),
        ((DISTRIBUTOR, LISTENER), (DISTRIBUTOR, PUBLISH), "child"),
        ((DISTRIBUTOR, LISTENER), (SF, PROCESS), "child"),
        ((SF, PROCESS), (SF, BUSINESS), "child"),
        ((SF, BUSINESS), (SF, HELIOS), "child"),
    } <= edges
    assert [edge["via"] for edge in path["edges"]].count("link") == 1


@pytest.mark.integration
def test_two_recordings_give_the_same_nodes_and_edges(client: TestClient, clock: Clock) -> None:
    [first] = _settle(client, clock, list(_recorded("place_order_1")))["paths"]
    [second] = _settle(client, clock, list(_recorded("place_order_2")))["paths"]

    assert _named(second) == _named(first)
    assert {node["flows"] for node in second["nodes"]} == {2}
    assert {edge["flows"] for edge in second["edges"]} == {2}


@pytest.mark.integration
def test_the_recording_without_its_helios_call_notifies_once(
    client: TestClient, clock: Clock, capfd: pytest.CaptureFixture[str]
) -> None:
    _warm(client, clock)
    assert NOTIFY not in capfd.readouterr().out

    _settle(client, clock, _without_helios(_copy(100)))

    out = capfd.readouterr().out
    [header] = [line for line in out.splitlines() if line.startswith(NOTIFY)]
    assert re.fullmatch(rf"{re.escape(NOTIFY)}\S+ system=omg trigger=path_drift", header)
    assert f"hop_missing: {HELIOS} on {ROUTE} _[path_drift]_" in out

    body = _settle(client, clock, _without_helios(_copy(101, "place_order_2")))
    assert NOTIFY not in capfd.readouterr().out
    [path] = body["paths"]
    [signal] = path["drift"]
    assert (signal["rule"], signal["node"], signal["occurrences"]) == (
        "hop_missing",
        {"service": SF, "operation": HELIOS},
        2,
    )
    assert (path["flows"], path["drifted_flows"]) == (MIN_SAMPLES, 2)


@pytest.mark.parametrize(
    ("change", "rule", "nodes"),
    [
        (_without_helios, "hop_missing", [HELIOS]),
        (_with_a_new_call, "hop_added", [NEW_CALL]),
        (_with_payments_failing, "hop_failing", [PAYMENTS]),
    ],
    ids=["helios-removed", "partner-call-added", "payments-failing"],
)
@pytest.mark.integration
def test_one_changed_hop_fires_its_rule_and_opens_one_incident(
    client: TestClient,
    clock: Clock,
    runs: list[Run],
    change: Callable[[Spans], Spans],
    rule: str,
    nodes: list[str],
) -> None:
    _warm(client, clock)
    drifted = change(_copy(100))

    _settle(client, clock, drifted)

    [(incident, dossier)] = runs
    assert (incident.trigger, incident.summary) == (Trigger.PATH_DRIFT, f"Path drift on {ROUTE}")
    assert [(f.data["rule"], f.data["node"]["operation"]) for f in dossier.fragments] == [
        (rule, node) for node in nodes
    ]
    [fragment] = dossier.fragments
    assert fragment.data["path_flows"] == MIN_SAMPLES
    assert fragment.data["trace_ids"] == _flow_traces(drifted)
    hops = [hop.operation for hop in dossier.process_path.hops if hop.ref is not None]
    assert (HELIOS in hops) is (rule != "hop_missing")
    assert (NEW_CALL in hops) is (rule == "hop_added")


@pytest.mark.integration
def test_the_helios_dossier_counts_the_paths_errors_and_the_flows_hops(
    client: TestClient, clock: Clock, runs: list[Run]
) -> None:
    _warm(client, clock)

    _settle(client, clock, _without_helios(_copy(100)))

    [(_incident, dossier)] = runs
    [fragment] = dossier.fragments
    # Staging refuses sf-publisher's Helios call in every recorded flow.
    assert {key: fragment.data[key] for key in ("node_flows", "node_errors")} == {
        "node_flows": MIN_SAMPLES,
        "node_errors": MIN_SAMPLES,
    }
    assert (fragment.data["flow_spans"], fragment.data["flow_errors"]) == (0, 0)
    assert len(dossier.process_path.hops) == HOPS - 1
    link = next(hop for hop in dossier.process_path.hops if hop.via is not None)
    assert (link.from_ref, link.to_ref, link.via, link.operation) == (
        API,
        DISTRIBUTOR,
        "link",
        LISTENER,
    )


@pytest.mark.integration
def test_a_lost_processor_is_one_incident_with_a_signal_per_node_it_took(
    client: TestClient, clock: Clock, runs: list[Run]
) -> None:
    _warm(client, clock)

    body = _settle(client, clock, _without_the_processor(_copy(100)))

    [(_incident, dossier)] = runs
    assert len(dossier.fragments) == SF_NODES
    assert {(f.data["rule"], f.data["node"]["service"]) for f in dossier.fragments} == {
        ("hop_missing", SF)
    }
    assert f"hop_missing: {PROCESS} on {ROUTE}" in [f.summary for f in dossier.fragments]
    [path] = body["paths"]
    assert len(path["drift"]) == SF_NODES


@pytest.mark.integration
def test_incomplete_and_orphan_flows_raise_nothing(
    client: TestClient, clock: Clock, runs: list[Run]
) -> None:
    _warm(client, clock)
    drifted = _without_helios(_copy(100))
    response = client.post("/v0/observations", json={"spans": drifted}, headers=HEADERS)
    assert response.status_code == 202

    clock.advance(SETTLE - 1)
    body = client.get("/v0/systems/omg/paths", headers=HEADERS).json()

    assert runs == []
    assert body["open_flows"] == 2
    assert body["counters"]["orphan_flows"] == MIN_SAMPLES
