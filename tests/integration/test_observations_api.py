"""POST /v0/observations and GET /v0/systems/{system}/streams through the FastAPI app."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.core.domain.dossier import Shape

TOKEN = "test-fire-token"
HEADERS = {"X-FIRE-Token": TOKEN}
CANARY = "canary-7f3a9c1e5b2d4e8fa6c0d9b1e2f3a4b5"
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID = "00f067aa0ba902b7"

SHAPE = {
    "fields": [{"path": "order_id", "value_type": "string", "cardinality": "1", "optional": False}]
}
FINGERPRINT = Sha256ShapeFingerprinter().fingerprint(Shape.model_validate(SHAPE)).model_dump()
EVENT_CHANNEL = {
    "kind": "event",
    "direction": "internal",
    "name": "order.created",
    "part": "payload",
}


def _shape(**overrides: Any) -> dict[str, Any]:
    item: dict[str, Any] = {
        "system": "shop",
        "source": "order-events",
        "channel": dict(EVENT_CHANNEL),
        "observation_id": "01J9Z3K4N8Q2R5T7V9X1Y3Z5A7",
        "occurred_at": "2026-10-11T16:30:00Z",
        "observed_at": "2026-10-11T16:30:01Z",
        "shape": SHAPE,
        "fingerprint": FINGERPRINT,
    }
    item.update(overrides)
    return item


def _span(**overrides: Any) -> dict[str, Any]:
    item: dict[str, Any] = {
        "system": "shop",
        "source": "traces",
        "trace_id": TRACE_ID,
        "span_id": SPAN_ID,
        "service": "orders-api",
        "kind": "server",
        "operation": "POST /v1/orders",
        "status_code": 201,
        "error": False,
        "start": "2026-10-11T16:30:00Z",
        "duration_ms": 41.5,
    }
    item.update(overrides)
    return item


def _write_system(directory: Path, system: str, source_ids: list[str]) -> None:
    raw = {
        "system": system,
        "display_name": system.title(),
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [
            {"id": source_id, "kind": "kinesis", "stream_env": "S", "regions": ["us-east-1"]}
            for source_id in source_ids
        ],
    }
    (directory / f"{system}.yml").write_text(yaml.safe_dump(raw), encoding="utf-8")


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    systems = tmp_path / "systems"
    systems.mkdir()
    _write_system(systems, "shop", ["order-events", "order-table", "traces"])
    _write_system(systems, "billing", [])
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    app_module._container = build_container(tmp_path / "fire.yml")
    # Not entered as a context manager: the lifespan would replace this container with one
    # built from the repo's config.
    yield TestClient(app)
    app_module._container = None


def _post(client: TestClient, body: Any, headers: dict[str, str] = HEADERS) -> Any:
    content = body if isinstance(body, bytes) else json.dumps(body).encode()
    return client.post(
        "/v0/observations",
        content=content,
        headers={**headers, "Content-Type": "application/json"},
    )


def _counts(accepted: int = 0, duplicate: int = 0, rejected: list[Any] | None = None) -> Any:
    return {"accepted": accepted, "duplicate": duplicate, "rejected": rejected or []}


INVALID_BODIES = [
    b"{not json",
    b"",
    json.dumps([]).encode(),
    json.dumps("shapes").encode(),
    json.dumps({"shapes": {}}).encode(),
    json.dumps({"shapes": None}).encode(),
    json.dumps({"spans": "x"}).encode(),
    json.dumps({"shapes": [], "observations": []}).encode(),
]
INVALID_BODY_IDS = [
    "malformed-json",
    "empty",
    "list",
    "string",
    "shapes-object",
    "shapes-null",
    "spans-string",
    "unknown-key",
]


@pytest.mark.integration
@pytest.mark.parametrize("body", INVALID_BODIES, ids=INVALID_BODY_IDS)
@pytest.mark.parametrize("headers", [{}, {"X-FIRE-Token": "wrong"}], ids=["missing", "wrong"])
def test_bad_token_is_401_before_the_body(
    client: TestClient, body: bytes, headers: dict[str, str]
) -> None:
    assert _post(client, body, headers).status_code == 401


@pytest.mark.integration
@pytest.mark.parametrize("body", INVALID_BODIES, ids=INVALID_BODY_IDS)
def test_unset_token_env_is_503_before_the_body(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, body: bytes
) -> None:
    monkeypatch.delenv("FIRE_API_TOKEN")
    assert _post(client, body).status_code == 503


@pytest.mark.integration
@pytest.mark.parametrize("body", INVALID_BODIES, ids=INVALID_BODY_IDS)
def test_body_that_is_not_a_batch_is_422_with_locations_only(
    client: TestClient, body: bytes
) -> None:
    response = _post(client, body)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail
    for error in detail:
        assert set(error) == {"loc", "type"}
        assert error["loc"][0] == "body"


@pytest.mark.integration
@pytest.mark.parametrize("body", [{}, {"shapes": []}, {"spans": []}, {"shapes": [], "spans": []}])
def test_empty_or_absent_lists_are_202(client: TestClient, body: dict[str, Any]) -> None:
    response = _post(client, body)
    assert response.status_code == 202
    assert response.json() == {"shapes": _counts(), "spans": _counts()}


MIXED_SHAPES = [
    _shape(),
    _shape(channel={**EVENT_CHANNEL, "kind": "http", "part": "response"}),
    _shape(system="unknown"),
    _shape(source="unknown"),
    _shape(fingerprint={**FINGERPRINT, "schema_hash": "0" * 64}),
    _shape(fingerprint={**FINGERPRINT, "notes": "extra"}),
    _shape(),
    _shape(source="order-table"),
]
MIXED_SPANS = [
    _span(),
    _span(source="order-events"),
    _span(source="unknown"),
    _span(trace_id=TRACE_ID.upper()),
]


@pytest.mark.integration
def test_mixed_batch_returns_each_reason_at_its_index(client: TestClient) -> None:
    response = _post(client, {"shapes": MIXED_SHAPES, "spans": MIXED_SPANS})

    assert response.status_code == 202
    assert response.json() == {
        "shapes": _counts(
            accepted=2,
            duplicate=1,
            rejected=[
                {"index": 1, "reason": "invalid", "fields": ["channel.status_class"]},
                {"index": 2, "reason": "unknown_system"},
                {"index": 3, "reason": "unknown_source"},
                {"index": 4, "reason": "fingerprint_mismatch"},
                {"index": 5, "reason": "fingerprint_mismatch"},
            ],
        ),
        "spans": _counts(
            accepted=1,
            duplicate=1,
            rejected=[
                {"index": 2, "reason": "unknown_source"},
                {"index": 3, "reason": "invalid", "fields": ["trace_id"]},
            ],
        ),
    }


@pytest.mark.integration
def test_resent_batch_is_all_duplicate(client: TestClient) -> None:
    batch = {
        "shapes": [_shape(), _shape(observation_id="obs-2"), _shape(source="order-table")],
        "spans": [_span(), _span(span_id="a3ce929d0e0e4736")],
    }
    first = _post(client, batch).json()
    assert first == {"shapes": _counts(accepted=3), "spans": _counts(accepted=2)}

    second = _post(client, batch).json()
    assert second == {"shapes": _counts(duplicate=3), "spans": _counts(duplicate=2)}


@pytest.mark.integration
def test_streams_counts_accepted_items_only(client: TestClient) -> None:
    _post(client, {"shapes": MIXED_SHAPES, "spans": MIXED_SPANS})

    response = client.get("/v0/systems/shop/streams", headers=HEADERS)

    assert response.status_code == 200
    fingerprint = {
        "schema_hash": FINGERPRINT["schema_hash"],
        "observations": 1,
        "first_observed_at": "2026-10-11T16:30:01Z",
        "last_observed_at": "2026-10-11T16:30:01Z",
    }
    assert response.json() == {
        "system": "shop",
        "streams": [
            {
                "source": source,
                "key": "event:internal:order.created:payload",
                "observations": 1,
                "fingerprints": [fingerprint],
            }
            for source in ("order-events", "order-table")
        ],
    }


@pytest.mark.integration
def test_streams_of_a_known_system_without_observations_is_empty(client: TestClient) -> None:
    _post(client, {"shapes": [_shape()]})
    response = client.get("/v0/systems/billing/streams", headers=HEADERS)
    assert response.status_code == 200
    assert response.json() == {"system": "billing", "streams": []}


@pytest.mark.integration
def test_streams_of_an_unknown_system_is_404(client: TestClient) -> None:
    assert client.get("/v0/systems/unknown/streams", headers=HEADERS).status_code == 404


@pytest.mark.integration
@pytest.mark.parametrize("headers", [{}, {"X-FIRE-Token": "wrong"}], ids=["missing", "wrong"])
@pytest.mark.parametrize("system", ["shop", "unknown"])
def test_streams_bad_token_is_401(
    client: TestClient, headers: dict[str, str], system: str
) -> None:
    assert client.get(f"/v0/systems/{system}/streams", headers=headers).status_code == 401


@pytest.mark.integration
def test_streams_unset_token_env_is_503(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FIRE_API_TOKEN")
    assert client.get("/v0/systems/shop/streams", headers=HEADERS).status_code == 503


@pytest.mark.integration
def test_canary_in_rejected_items_reaches_neither_responses_nor_logs(
    client: TestClient, caplog: pytest.LogCaptureFixture, capfd: pytest.CaptureFixture[str]
) -> None:
    caplog.set_level(logging.DEBUG)
    planted_field = {"path": "order_id", "value_type": "string", "value": CANARY}
    link = {"trace_id": TRACE_ID, "span_id": "a3ce929d0e0e4736", "payload": CANARY}
    batch = {
        "shapes": [
            _shape(),
            _shape(payload=CANARY),
            _shape(channel={**EVENT_CHANNEL, "body": CANARY}),
            _shape(shape={"fields": [planted_field]}),
            _shape(fingerprint={**FINGERPRINT, "attributes": CANARY}),
            _shape(region=CANARY * 10),
            _shape(system=CANARY),
            _shape(source=CANARY),
            _shape(fingerprint={**FINGERPRINT, "notes": CANARY}),
        ],
        "spans": [
            _span(attributes={"http.request.body": CANARY}),
            _span(links=[link]),
            _span(operation=CANARY * 10),
        ],
    }

    responses = [
        _post(client, batch),
        _post(client, {"shapes": [], "payload": CANARY}),
        _post(client, {"shapes": CANARY}),
        client.post(
            "/v0/diagnose",
            json={"project_id": "proj-a", "shape": {"fields": [planted_field]}},
            headers=HEADERS,
        ),
        client.get("/v0/systems/shop/streams", headers=HEADERS),
    ]

    assert [response.status_code for response in responses] == [202, 422, 422, 422, 200]
    assert responses[0].json()["shapes"]["accepted"] == 1
    assert len(responses[0].json()["shapes"]["rejected"]) == 8
    assert len(responses[0].json()["spans"]["rejected"]) == 3
    for response in responses:
        assert CANARY not in response.text
    assert "shapes accepted=1 duplicate=0 rejected=8" in caplog.text
    captured = capfd.readouterr()
    assert CANARY not in caplog.text
    assert CANARY not in captured.out
    assert CANARY not in captured.err
