"""SystemConfig: per-system YAML holds decisions, validated strictly."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from fire.apps.config import SystemConfig, load_system_config, load_system_configs

SYSTEMS = Path(__file__).resolve().parents[2] / "config" / "systems"
TEMPLATE = SYSTEMS / "_template.yml"


def _minimal(**overrides: Any) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "system": "example",
        "display_name": "Example",
        "notify": {"slack": {"channel_id": "C0123456789"}},
    }
    raw.update(overrides)
    return raw


def _write(directory: Path, name: str, raw: dict[str, Any]) -> Path:
    path = directory / name
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    return path


@pytest.mark.unit
def test_template_is_a_valid_system_config() -> None:
    config = SystemConfig.model_validate(yaml.safe_load(TEMPLATE.read_text()))
    assert config.notify.slack.channel_id == "C0123456789"
    assert config.discovery.manifest is not None
    assert config.sources[0].dedupe_key == "work_unit_id"


@pytest.mark.unit
def test_repo_system_configs_are_valid() -> None:
    assert "omg" in load_system_configs(SYSTEMS)


@pytest.mark.unit
def test_minimal_config_defaults() -> None:
    config = SystemConfig.model_validate(_minimal())
    assert config.notify.slack.channel_name is None
    assert config.discovery.manifest is None
    assert config.sources == []
    assert config.policy.critical == []
    assert config.fingerprinting.sample_rate == 1.0


@pytest.mark.unit
def test_slack_channel_id_is_required() -> None:
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(notify={"slack": {"channel_name": "#x"}}))


@pytest.mark.unit
@pytest.mark.parametrize("channel_id", ["C0123456789", "G0ABCDEFGH", "C12345678"])
def test_slack_channel_id_accepts_channel_ids(channel_id: str) -> None:
    config = SystemConfig.model_validate(_minimal(notify={"slack": {"channel_id": channel_id}}))
    assert config.notify.slack.channel_id == channel_id


@pytest.mark.unit
@pytest.mark.parametrize(
    "channel_id",
    ["#example-incidents", "c0123456789", "D0123456789", "U0123456789", "C1234", ""],
    ids=["name", "lowercase", "dm", "user", "short", "empty"],
)
def test_slack_channel_id_rejects_non_channel_ids(channel_id: str) -> None:
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(notify={"slack": {"channel_id": channel_id}}))


@pytest.mark.unit
@pytest.mark.parametrize("channel_name", ["example-incidents", "#Upper", "#has space"])
def test_slack_channel_name_must_look_like_a_channel(channel_name: str) -> None:
    slack = {"channel_id": "C0123456789", "channel_name": channel_name}
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(notify={"slack": slack}))


@pytest.mark.unit
def test_unknown_keys_are_rejected() -> None:
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(team_channel="#old"))


@pytest.mark.unit
def test_critical_and_ignore_must_be_disjoint() -> None:
    with pytest.raises(ValidationError, match="order.created"):
        SystemConfig.model_validate(
            _minimal(policy={"critical": ["order.created"], "ignore": ["order.created"]})
        )


@pytest.mark.unit
@pytest.mark.parametrize("sample_rate", [0, -0.1, 1.5])
def test_sample_rate_is_in_zero_one(sample_rate: float) -> None:
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(fingerprinting={"sample_rate": sample_rate}))


@pytest.mark.unit
def test_kinesis_source_needs_a_region() -> None:
    source = {"kind": "kinesis", "stream_env": "S", "regions": []}
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(sources=[source]))


@pytest.mark.unit
def test_file_name_must_match_system(tmp_path: Path) -> None:
    path = _write(tmp_path, "other.yml", _minimal())
    with pytest.raises(ValueError, match="must match the file name"):
        load_system_config(path)


@pytest.mark.unit
def test_load_system_configs_skips_underscore_files(tmp_path: Path) -> None:
    _write(tmp_path, "example.yml", _minimal())
    _write(tmp_path, "_template.yml", _minimal(system="template"))
    (tmp_path / "notes.txt").write_text("ignored", encoding="utf-8")

    assert list(load_system_configs(tmp_path)) == ["example"]


@pytest.mark.unit
def test_load_system_configs_missing_directory(tmp_path: Path) -> None:
    assert load_system_configs(tmp_path / "absent") == {}
