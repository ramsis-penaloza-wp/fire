"""Generic Shape / Incident / DiagnoseRequest contracts."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from fire.apps.incident_engine.app import DiagnoseRequest, app
import fire.apps.incident_engine.app as app_module
from fire.apps.incident_engine.wiring import build_container
from fire.core.domain.dossier import Cardinality, Shape, ShapeField, ValueType
from fire.core.domain.incident import Incident


@pytest.mark.unit
def test_shape_has_no_value_slot() -> None:
    field = ShapeField(path="a.b", value_type=ValueType.NUMBER, cardinality=Cardinality.ONE)
    assert not hasattr(field, "value")
    shape = Shape(fields=[field])
    assert shape.model_dump() == {
        "fields": [
            {"path": "a.b", "value_type": "number", "cardinality": "one"},
        ]
    }


@pytest.mark.unit
def test_incident_uses_project_id() -> None:
    incident = Incident(id="i1", project_id="proj-a", service="payments")
    dumped = incident.model_dump()
    assert "system" not in dumped
    assert dumped["project_id"] == "proj-a"
    assert dumped["service"] == "payments"


@pytest.mark.unit
def test_diagnose_request_nests_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIRE_API_TOKEN", "test-fire-token")
    app_module._container = build_container()
    body = DiagnoseRequest(
        project_id="proj-a",
        shape=Shape(fields=[ShapeField(path="x", value_type=ValueType.STRING)]),
    )
    assert body.fingerprint is None
    with TestClient(app) as client:
        response = client.post(
            "/v0/diagnose",
            json=body.model_dump(),
            headers={"X-FIRE-Token": "test-fire-token"},
        )
    app_module._container = None
    assert response.status_code == 202
