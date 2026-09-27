"""HTTP auth fail-closed on POST /v0/diagnose."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
import fire.apps.incident_engine.app as app_module


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FIRE_API_TOKEN", "test-fire-token")
    app_module._container = build_container()
    with TestClient(app) as test_client:
        yield test_client
    app_module._container = None


@pytest.mark.unit
def test_diagnose_missing_token_401(client: TestClient) -> None:
    response = client.post("/v0/diagnose", json={"project_id": "example"})
    assert response.status_code == 401


@pytest.mark.unit
def test_diagnose_bad_token_401(client: TestClient) -> None:
    response = client.post(
        "/v0/diagnose",
        json={"project_id": "example"},
        headers={"X-FIRE-Token": "wrong"},
    )
    assert response.status_code == 401


@pytest.mark.unit
def test_diagnose_unset_token_env_503(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FIRE_API_TOKEN", raising=False)
    app_module._container = build_container()
    with TestClient(app) as test_client:
        response = test_client.post(
            "/v0/diagnose",
            json={"project_id": "example"},
            headers={"X-FIRE-Token": "anything"},
        )
    app_module._container = None
    assert response.status_code == 503


@pytest.mark.unit
def test_diagnose_ok_202(client: TestClient) -> None:
    response = client.post(
        "/v0/diagnose",
        json={
            "project_id": "example",
            "service": "api",
            "fingerprint": {"schema_hash": "abc"},
            "process_hops": [{"service": "api", "operation": "POST /x"}],
        },
        headers={"X-FIRE-Token": "test-fire-token"},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["project_id"] == "example"
    assert body["service"] == "api"
