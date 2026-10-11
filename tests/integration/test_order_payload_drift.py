"""OMG's place-order request body as a baseline through the API, then drifted copies of it.

The fixture is OMG's scripts/test_payloads/test_order_payload.json, the request body of
`POST /v1/orders/place_order`, copied with placeholder values and its shape unchanged;
test_fixture_has_the_shape_of_omgs_payload says when it drifts from OMG's file. Each test builds
its observations from variants of the fixture, as dev/omg/README.md section e sends them to
OMG's stack, and posts them to /v0/observations.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container

REPO = Path(__file__).resolve().parents[2]
FIXTURE = REPO / "tests" / "fixtures" / "omg" / "api_requests" / "test_order_payload.json"
TOKEN = "order-payload-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
SOURCE = "api-proxy"
CHANNEL = {
    "kind": "http",
    "direction": "inbound",
    "name": "POST /v1/orders/place_order",
    "part": "request",
}
STREAM = "http:inbound:POST /v1/orders/place_order:request"
OMG_YML = REPO / "config" / "systems" / "omg.yml"
# The thresholds the real omg.yml declares.
DRIFT = yaml.safe_load(OMG_YML.read_text(encoding="utf-8"))["drift"]
MIN_SAMPLES = DRIFT["min_samples"]
# PlaceOrderRequest in OMG declares these optional.
OPTIONAL = ("adminId", "adminName", "partyId")
# The fixture's placeholder payment id; every other planted string contains "fixture".
PAYMENT_ID = "00000000-0000-4000-8000-000000000001"
START = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
GENERALIZER = FlatStarShapeGeneralizer()
FINGERPRINTER = Sha256ShapeFingerprinter()


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """A body with one system, omg, whose inbound source reads OMG's API requests."""
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    (tmp_path / "systems").mkdir()
    system = {
        "system": "omg",
        "display_name": "Order Management Gateway",
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [
            {
                "id": SOURCE,
                "kind": "replay",
                "direction": "inbound",
                "path_env": "OMG_API_REPLAY_PATH",
                "envelope": {
                    "event_id": "id",
                    "event_type": "type",
                    "occurred_at": "at",
                    "payload": "body",
                },
            }
        ],
        "drift": DRIFT,
    }
    (tmp_path / "systems" / "omg.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    app_module._container = build_container(tmp_path / "fire.yml")
    # Not entered as a context manager: the lifespan would replace this container with one
    # built from the repo's config.
    yield TestClient(app)
    app_module._container = None


def _fixture() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _without(payload: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in keys}


def _warm_up_variant(index: int) -> dict[str, Any]:
    """Toggles the optional fields, never all three at once: that form first comes when warm."""
    payload = _fixture()
    if index % 2 == 0:
        return _without(payload, "adminId", "adminName")
    if index % 3 == 0:
        return _without(payload, "partyId")
    return payload


def _drifted() -> dict[str, Any]:
    """dev/omg/README.md section e: `timezone` removed and `referenceOrderId` an integer."""
    payload = _without(_fixture(), "timezone")
    payload["referenceOrderId"] = 12345
    return payload


def _at(index: int) -> str:
    return (START + timedelta(seconds=index)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _schema_hash(payload: dict[str, Any]) -> str:
    return FINGERPRINTER.fingerprint(GENERALIZER.generalize(payload)).schema_hash


def _observation(payload: dict[str, Any], index: int) -> dict[str, Any]:
    """`payload` seen on the place-order request channel: its shape and fingerprint only."""
    shape = GENERALIZER.generalize(payload)
    return {
        "system": "omg",
        "source": SOURCE,
        "channel": dict(CHANNEL),
        "observation_id": f"place-order-{index:04d}",
        "occurred_at": _at(index),
        "observed_at": _at(index),
        "shape": shape.model_dump(mode="json"),
        "fingerprint": FINGERPRINTER.fingerprint(shape).model_dump(mode="json"),
    }


def _post(client: TestClient, payloads: list[dict[str, Any]], first: int) -> httpx.Response:
    """Posts `payloads` as observations numbered from `first`; every one must be accepted."""
    shapes = [_observation(payload, first + offset) for offset, payload in enumerate(payloads)]
    response = client.post("/v0/observations", json={"shapes": shapes}, headers=HEADERS)
    assert response.status_code == 202
    assert response.json()["shapes"] == {"accepted": len(shapes), "duplicate": 0, "rejected": []}
    return response


def _warm(client: TestClient) -> list[httpx.Response]:
    """The warm-up variants, then the form with every optional field absent."""
    return [
        _post(client, [_warm_up_variant(index) for index in range(MIN_SAMPLES)], first=0),
        _post(client, [_without(_fixture(), *OPTIONAL)], first=MIN_SAMPLES),
    ]


def _get_streams(client: TestClient) -> httpx.Response:
    response = client.get("/v0/systems/omg/streams", headers=HEADERS)
    assert response.status_code == 200
    return response


def _stream(client: TestClient) -> dict[str, Any]:
    [stream] = _get_streams(client).json()["streams"]
    assert (stream["source"], stream["key"]) == (SOURCE, STREAM)
    return stream


def _drift_lines(caplog: pytest.LogCaptureFixture) -> list[tuple[str, int, str]]:
    return [record for record in caplog.record_tuples if "shape drift raised" in record[2]]


def _drift_line(schema_hash: str, added: int, type_changed: int, removed: int) -> str:
    return (
        f"shape drift raised: system=omg source={SOURCE} stream={STREAM} "
        f"fingerprint={schema_hash} added={added} type_changed={type_changed} removed={removed}"
    )


def _strings(value: Any) -> Iterator[str]:
    if isinstance(value, dict):
        for item in value.values():
            yield from _strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _strings(item)
    elif isinstance(value, str):
        yield value


def _planted() -> list[str]:
    """The fixture's placeholder strings: the payment id and every value holding "fixture"."""
    strings = set(_strings(_fixture()))
    assert PAYMENT_ID in strings
    return sorted({value for value in strings if "fixture" in value} | {PAYMENT_ID})


@pytest.mark.integration
def test_optional_fields_toggled_warm_the_stream_and_fold_silently(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    all_absent = _without(_fixture(), *OPTIONAL)

    _post(client, [_warm_up_variant(index) for index in range(MIN_SAMPLES)], first=0)

    stream = _stream(client)
    assert (stream["status"], stream["samples"], stream["drift"]) == ("warm", MIN_SAMPLES, [])
    known = {fingerprint["schema_hash"] for fingerprint in stream["fingerprints"]}
    assert _schema_hash(all_absent) not in known

    _post(client, [all_absent], first=MIN_SAMPLES)

    stream = _stream(client)
    assert (stream["status"], stream["samples"], stream["drift"]) == ("warm", MIN_SAMPLES + 1, [])
    assert _drift_lines(caplog) == []


@pytest.mark.integration
def test_drifted_copy_is_one_signal_that_counts_100_occurrences(
    client: TestClient, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    _warm(client)
    drifted = _drifted()
    schema_hash = _schema_hash(drifted)
    first = MIN_SAMPLES + 1

    _post(client, [drifted], first=first)

    signal = {
        "schema_hash": schema_hash,
        "diff": {
            "added": [],
            "type_changed": [{"path": "referenceOrderId", "old": ["string"], "new": ["integer"]}],
            "removed": [{"path": "timezone", "value_type": "string"}],
        },
        "occurrences": 1,
        "first_seen": _at(first),
        "last_seen": _at(first),
    }
    stream = _stream(client)
    assert (stream["samples"], stream["observations"]) == (MIN_SAMPLES + 1, MIN_SAMPLES + 2)
    assert stream["drift"] == [signal]
    assert _drift_lines(caplog) == [
        ("fire.apps.incident_engine.wiring", logging.INFO, _drift_line(schema_hash, 0, 1, 1))
    ]

    _post(client, [drifted] * 99, first=first + 1)

    stream = _stream(client)
    assert (stream["samples"], stream["observations"]) == (MIN_SAMPLES + 1, MIN_SAMPLES + 101)
    assert stream["drift"] == [{**signal, "occurrences": 100, "last_seen": _at(first + 99)}]
    assert len(_drift_lines(caplog)) == 1


@pytest.mark.integration
@pytest.mark.parametrize(
    ("field", "value_type", "children"), [("checkoutId", "string", 0), ("totals", "object", 15)]
)
def test_required_field_removed_is_one_signal_with_only_that_removal(
    client: TestClient,
    caplog: pytest.LogCaptureFixture,
    field: str,
    value_type: str,
    children: int,
) -> None:
    caplog.set_level(logging.INFO)
    _warm(client)
    drifted = _without(_fixture(), field)

    _post(client, [drifted], first=MIN_SAMPLES + 1)

    removed = [
        {"path": shape_field.path, "value_type": shape_field.value_type.value}
        for shape_field in GENERALIZER.generalize(_fixture()).fields
        if shape_field.path == field or shape_field.path.startswith(f"{field}.")
    ]
    assert removed[0] == {"path": field, "value_type": value_type}
    assert len(removed) == 1 + children
    stream = _stream(client)
    assert stream["samples"] == MIN_SAMPLES + 1
    [signal] = stream["drift"]
    assert signal["schema_hash"] == _schema_hash(drifted)
    assert signal["diff"] == {"added": [], "type_changed": [], "removed": removed}
    assert signal["occurrences"] == 1
    assert [line for _name, _level, line in _drift_lines(caplog)] == [
        _drift_line(_schema_hash(drifted), 0, 0, len(removed))
    ]


@pytest.mark.integration
def test_no_placeholder_reaches_streams_logs_or_the_catalog(
    client: TestClient, caplog: pytest.LogCaptureFixture, capfd: pytest.CaptureFixture[str]
) -> None:
    caplog.set_level(logging.DEBUG)
    responses = _warm(client)
    responses.append(_get_streams(client))
    responses.append(_post(client, [_drifted()] * 2, first=MIN_SAMPLES + 1))
    responses.append(_get_streams(client))
    responses.append(_post(client, [_without(_fixture(), "totals")], first=MIN_SAMPLES + 3))
    responses.append(_get_streams(client))

    assert len(_stream(client)["drift"]) == 2
    catalog_state = repr(vars(app_module.get_container().catalog))
    captured = capfd.readouterr()
    outputs = [response.text for response in responses]
    outputs += [catalog_state, caplog.text, captured.out, captured.err]
    for value in _planted():
        for output in outputs:
            assert value not in output


@pytest.mark.integration
@pytest.mark.omg_local
def test_fixture_has_the_shape_of_omgs_payload() -> None:
    path = Path(os.environ["OMG_REPO"]) / "scripts" / "test_payloads" / FIXTURE.name
    omg_shape = GENERALIZER.generalize(json.loads(path.read_text(encoding="utf-8")))
    assert omg_shape == GENERALIZER.generalize(_fixture())
