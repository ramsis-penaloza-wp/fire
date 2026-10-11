"""Generic Shape / Incident / DiagnoseRequest contracts."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel, ValidationError

import fire.apps.incident_engine.app as app_module
from fire.apps.incident_engine.app import DiagnoseRequest, app
from fire.apps.incident_engine.wiring import build_container
from fire.core.domain.dossier import Shape, ShapeField, ShapeFingerprint, ValueType
from fire.core.domain.incident import Incident


@pytest.mark.unit
def test_shape_has_no_value_slot() -> None:
    field = ShapeField(path="a.b", value_type=ValueType.NUMBER, cardinality="1")
    assert not hasattr(field, "value")
    shape = Shape(fields=[field])
    assert shape.model_dump() == {
        "fields": [
            {"path": "a.b", "value_type": "number", "cardinality": "1", "optional": False},
        ]
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    ("model", "data"),
    [
        (ShapeField, {"path": "a", "value": "planted-value"}),
        (Shape, {"fields": [], "payload": {"a": 1}}),
        (ShapeFingerprint, {"schema_hash": "abc", "body": "planted-value"}),
    ],
    ids=["shape-field", "shape", "fingerprint"],
)
def test_shape_models_reject_unknown_fields(model: type[BaseModel], data: dict[str, Any]) -> None:
    with pytest.raises(ValidationError) as caught:
        model.model_validate(data)
    assert [error["type"] for error in caught.value.errors()] == ["extra_forbidden"]


@pytest.mark.unit
def test_diagnose_rejects_a_value_inside_the_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIRE_API_TOKEN", "test-fire-token")
    app_module._container = build_container()
    body = {
        "project_id": "proj-a",
        "shape": {"fields": [{"path": "x", "value_type": "string", "value": "planted-value"}]},
    }
    with TestClient(app) as client:
        response = client.post(
            "/v0/diagnose", json=body, headers={"X-FIRE-Token": "test-fire-token"}
        )
    app_module._container = None
    assert response.status_code == 422
    [error] = response.json()["detail"]
    assert error["loc"] == ["body", "shape", "fields", 0, "value"]
    assert error["type"] == "extra_forbidden"


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
