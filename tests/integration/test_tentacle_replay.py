"""OMG's five WorkUnit samples, replayed through omg.yml's replay source into the body."""

from __future__ import annotations

import logging
import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.apps.tentacle.runner import EXIT_OK, run_tentacle

REPO = Path(__file__).resolve().parents[2]
# Copied from order-management-gateway scripts/test_payloads/*_work_unit.json (OMG's synthetic
# samples); test_omg_repo_samples_match_the_fixtures says when they drift.
FIXTURES = REPO / "tests" / "fixtures" / "omg" / "test_payloads"
TOKEN = "replay-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
EVENT_TYPES = [
    "fulfillment.allocated",
    "fulfillment.receive",
    "order.charged",
    "payments.ar_refund_ack",
    "payments.oracle_refund_initiated",
]


@pytest.fixture
def fire_yml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """The repo's omg.yml next to an empty fire.yml, and a body built from them."""
    path = tmp_path / "fire.yml"
    path.write_text("{}", encoding="utf-8")
    (tmp_path / "systems").mkdir()
    shutil.copy(REPO / "config" / "systems" / "omg.yml", tmp_path / "systems" / "omg.yml")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    monkeypatch.delenv("OMG_MANIFEST_PATH", raising=False)
    app_module._container = build_container(path)
    yield path
    app_module._container = None


def _replay(fire_yml: Path, replay_path: Path, **kwargs: Any) -> int:
    environ = {
        "FIRE_API_TOKEN": TOKEN,
        "FIRE_BODY_URL": "http://body",
        "OMG_REPLAY_PATH": str(replay_path),
    }
    transport = httpx.ASGITransport(app=app)
    return run_tentacle(
        "omg", config_path=fire_yml, environ=environ, transport=transport, **kwargs
    )


def _streams() -> list[dict[str, Any]]:
    response = TestClient(app).get("/v0/systems/omg/streams", headers=HEADERS)
    assert response.status_code == 200
    return response.json()["streams"]


@pytest.mark.integration
def test_work_unit_samples_give_five_event_channels_of_one_sample(fire_yml: Path) -> None:
    assert _replay(fire_yml, FIXTURES) == EXIT_OK

    streams = _streams()
    assert [(stream["source"], stream["key"], stream["observations"]) for stream in streams] == [
        ("work-unit-samples", f"event:internal:{event_type}:payload", 1)
        for event_type in EVENT_TYPES
    ]
    assert all(len(stream["fingerprints"]) == 1 for stream in streams)


@pytest.mark.integration
def test_second_replay_adds_only_duplicates(
    fire_yml: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    assert _replay(fire_yml, FIXTURES) == EXIT_OK
    first = _streams()

    assert _replay(fire_yml, FIXTURES, source="work-unit-samples") == EXIT_OK

    assert _streams() == first
    assert (
        "source=work-unit-samples read=5 built=5 sent=5 unsent=0 pending=0 rejected=- "
        "skipped=- noted=- body_duplicate=5 body_rejected=-"
    ) in caplog.text


def _fingerprints() -> list[tuple[str, list[str]]]:
    return [
        (stream["key"], [fingerprint["schema_hash"] for fingerprint in stream["fingerprints"]])
        for stream in _streams()
    ]


@pytest.mark.integration
@pytest.mark.omg_local
def test_omg_repo_samples_match_the_fixtures(
    fire_yml: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    payloads = Path(os.environ["OMG_REPO"]) / "scripts" / "test_payloads"
    samples = sorted(path.name for path in payloads.glob("*_work_unit.json"))
    assert samples == sorted(path.name for path in FIXTURES.glob("*.json"))

    # The whole directory, as dev/omg/README.md runs it: its API bodies count as rejected.
    assert _replay(fire_yml, payloads) == EXIT_OK
    assert "source=work-unit-samples" in caplog.text
    assert "built=5 sent=5 unsent=0 pending=0 rejected=missing_event_id:" in caplog.text
    from_omg = _fingerprints()

    app_module._container = build_container(fire_yml)
    assert _replay(fire_yml, FIXTURES) == EXIT_OK
    assert _fingerprints() == from_omg
