"""POST /v0/diagnose accepts an Origin and assembles the dossier."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.core.domain.dossier import Dossier, EvidenceQuality, Shape
from fire.core.domain.incident import Incident

HEADERS = {"X-FIRE-Token": "test-fire-token"}


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FIRE_API_TOKEN", "test-fire-token")
    app_module._container = build_container()
    with TestClient(app) as test_client:
        yield test_client
    app_module._container = None


@pytest.fixture
def dossiers(monkeypatch: pytest.MonkeyPatch) -> list[Dossier]:
    """Dossiers handed to the background pipeline, in request order."""
    captured: list[Dossier] = []

    async def capture(_incident: Incident, dossier: Dossier) -> None:
        captured.append(dossier)

    monkeypatch.setattr(app_module, "_run_pipeline", capture)
    return captured


@pytest.mark.unit
def test_diagnose_with_origin_shape_and_fingerprint(
    client: TestClient, dossiers: list[Dossier]
) -> None:
    response = client.post(
        "/v0/diagnose",
        json={
            "project_id": "proj-a",
            "service": "checkout",
            "origin": {
                "origin_id": "orig-orders",
                "kind": "http",
                "endpoint_ref": "orders.create",
            },
            "shape": {
                "fields": [{"path": "order.id", "value_type": "string", "cardinality": "1"}]
            },
            "fingerprint": {"schema_hash": "abc", "normalized_paths": ["order.id"]},
            "process_hops": [
                {"from_ref": "orig-orders", "to_ref": "orig-payments", "operation": "POST /pay"}
            ],
        },
        headers=HEADERS,
    )
    assert response.status_code == 202
    body = response.json()
    assert body["origin_id"] == "orig-orders"
    assert body["project_id"] == "proj-a"
    [dossier] = dossiers
    assert dossier.origin is not None
    assert dossier.origin.origin_id == "orig-orders"
    assert dossier.shape is not None
    assert [field.path for field in dossier.shape.fields] == ["order.id"]
    assert dossier.fingerprint is not None
    assert dossier.fingerprint.schema_hash == "abc"


@pytest.mark.unit
def test_diagnose_declares_missing_evidence_as_fragments(
    client: TestClient, dossiers: list[Dossier]
) -> None:
    response = client.post("/v0/diagnose", json={"project_id": "proj-a"}, headers=HEADERS)
    assert response.status_code == 202
    [dossier] = dossiers
    assert [(f.collector, f.quality) for f in dossier.fragments] == [
        ("origin", EvidenceQuality.MISSING),
        ("shape", EvidenceQuality.MISSING),
    ]
    assert dossier.missing == dossier.fragments


@pytest.mark.unit
def test_diagnose_shape_without_fingerprint_computes_it(
    client: TestClient, dossiers: list[Dossier]
) -> None:
    shape = {"fields": [{"path": "order.id", "value_type": "string"}]}
    response = client.post(
        "/v0/diagnose",
        json={
            "project_id": "proj-a",
            "origin": {"origin_id": "orig-orders", "endpoint_ref": "orders.create"},
            "shape": shape,
        },
        headers=HEADERS,
    )
    assert response.status_code == 202
    [dossier] = dossiers
    assert dossier.fingerprint == Sha256ShapeFingerprinter().fingerprint(
        Shape.model_validate(shape)
    )
    assert dossier.missing == []


@pytest.mark.unit
def test_diagnose_accepts_node_and_edge_hops(client: TestClient, dossiers: list[Dossier]) -> None:
    response = client.post(
        "/v0/diagnose",
        json={
            "project_id": "proj-a",
            "fingerprint": {"schema_hash": "abc"},
            "process_hops": [
                {"ref": "checkout", "operation": "POST /orders"},
                {"from_ref": "orig-orders", "to_ref": "orig-payments"},
            ],
        },
        headers=HEADERS,
    )
    assert response.status_code == 202
    [dossier] = dossiers
    assert [hop.display_label() for hop in dossier.process_path.hops] == [
        "checkout/POST /orders",
        "orig-orders->orig-payments",
    ]


@pytest.mark.unit
def test_diagnose_rejects_hop_that_is_node_and_edge(
    client: TestClient, dossiers: list[Dossier]
) -> None:
    response = client.post(
        "/v0/diagnose",
        json={
            "project_id": "proj-a",
            "process_hops": [{"ref": "checkout", "from_ref": "orig-orders"}],
        },
        headers=HEADERS,
    )
    assert response.status_code == 422
    assert dossiers == []
