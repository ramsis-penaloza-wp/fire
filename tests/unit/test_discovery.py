"""Discovery at startup: inventory from the declared manifest, or a declared reason why not."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from fire.apps.config import SystemConfig
from fire.apps.discovery import check_manifest_kinds, discover
from fire.apps.incident_engine.wiring import build_container, manifest_parsers

MANIFEST_ENV = "EXAMPLE_MANIFEST_PATH"
GRAPH = {"nodes": [{"id": "order.created", "kind": "event"}]}


def _config(manifest: dict[str, Any] | None = None) -> SystemConfig:
    raw: dict[str, Any] = {
        "system": "example",
        "display_name": "Example",
        "notify": {"slack": {"channel_id": "C0123456789"}},
    }
    if manifest is not None:
        raw["discovery"] = {"manifest": manifest}
    return SystemConfig.model_validate(raw)


def _with_manifest(kind: str = "event_chains_graph") -> SystemConfig:
    return _config({"kind": kind, "path_env": MANIFEST_ENV})


def _write_json(tmp_path: Path, content: str) -> dict[str, str]:
    path = tmp_path / "graph.json"
    path.write_text(content, encoding="utf-8")
    return {MANIFEST_ENV: str(path)}


@pytest.mark.unit
def test_no_manifest_is_observation_only() -> None:
    result = discover(_config(), manifest_parsers(), environ={})
    assert result.inventory is None
    assert result.missing_reason == "No manifest declared (observation only)"


@pytest.mark.unit
def test_unset_env_var_is_declared() -> None:
    result = discover(_with_manifest(), manifest_parsers(), environ={})
    assert result.inventory is None
    assert result.missing_reason == f"{MANIFEST_ENV} is not set"


@pytest.mark.unit
def test_missing_file_is_declared(tmp_path: Path) -> None:
    environ = {MANIFEST_ENV: str(tmp_path / "absent.json")}
    result = discover(_with_manifest(), manifest_parsers(), environ=environ)
    assert result.inventory is None
    assert result.missing_reason is not None
    assert result.missing_reason.startswith("Manifest unreadable")


@pytest.mark.unit
def test_invalid_json_is_declared(tmp_path: Path) -> None:
    environ = _write_json(tmp_path, "{not json")
    result = discover(_with_manifest(), manifest_parsers(), environ=environ)
    assert result.missing_reason is not None
    assert result.missing_reason.startswith("Manifest is not JSON")


@pytest.mark.unit
def test_rejected_document_is_declared(tmp_path: Path) -> None:
    environ = _write_json(tmp_path, json.dumps({"nodes": {}}))
    result = discover(_with_manifest(), manifest_parsers(), environ=environ)
    assert result.missing_reason is not None
    assert result.missing_reason.startswith("Manifest rejected")


@pytest.mark.unit
def test_valid_manifest_gives_inventory(tmp_path: Path) -> None:
    environ = _write_json(tmp_path, json.dumps(GRAPH))
    result = discover(_with_manifest(), manifest_parsers(), environ=environ)
    assert result.missing_reason is None
    assert result.inventory is not None
    assert result.inventory.event_names() == {"order.created"}


@pytest.mark.unit
def test_unknown_manifest_kind_fails_fast() -> None:
    with pytest.raises(ValueError, match="no parser"):
        check_manifest_kinds({"example": _with_manifest("unknown")}, manifest_parsers())


@pytest.mark.unit
def test_build_container_loads_systems_and_discoveries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    systems = tmp_path / "systems"
    systems.mkdir()
    raw = _with_manifest().model_dump(exclude_none=True)
    (systems / "example.yml").write_text(yaml.safe_dump(raw), encoding="utf-8")
    graph = tmp_path / "graph.json"
    graph.write_text(json.dumps(GRAPH), encoding="utf-8")
    monkeypatch.setenv(MANIFEST_ENV, str(graph))

    container = build_container(tmp_path / "fire.yml")

    assert list(container.systems) == ["example"]
    inventory = container.discoveries["example"].inventory
    assert inventory is not None
    assert inventory.event_names() == {"order.created"}


@pytest.mark.unit
def test_build_container_rejects_unknown_manifest_kind(tmp_path: Path) -> None:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    systems = tmp_path / "systems"
    systems.mkdir()
    raw = _with_manifest("unknown").model_dump(exclude_none=True)
    (systems / "example.yml").write_text(yaml.safe_dump(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="no parser"):
        build_container(tmp_path / "fire.yml")
