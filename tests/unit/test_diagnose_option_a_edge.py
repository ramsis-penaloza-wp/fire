"""Edge accepts Origin + ShapeRecord assembly on diagnose."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FIRE_API_TOKEN", "test-fire-token")
    app_module._container = build_container()
    with TestClient(app) as test_client:
        yield test_client
    app_module._container = None


@pytest.mark.unit
def test_diagnose_with_origin_and_shape_record(client: TestClient) -> None:
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
                "fields": [
                    {"path": "order.id", "value_type": "string", "cardinality": "1"}
                ]
            },
            "fingerprint": {"schema_hash": "abc", "normalized_paths": ["order.id"]},
            "process_hops": [
                {"from_ref": "orig-orders", "to_ref": "orig-payments", "operation": "POST /pay"}
            ],
        },
        headers={"X-FIRE-Token": "test-fire-token"},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["origin_id"] == "orig-orders"
    assert body["project_id"] == "proj-a"
