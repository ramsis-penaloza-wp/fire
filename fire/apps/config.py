"""Load YAML config. Credential values stay as env var *names* until wiring."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class ApiConfig(BaseModel):
    token_env: str = "FIRE_API_TOKEN"


class ReasonerConfig(BaseModel):
    provider: str = "echo"
    base_url_env: str | None = None
    api_key_env: str | None = None


class NarratorConfig(BaseModel):
    provider: str = "template"
    base_url_env: str = "FIRE_NARRATOR_BASE_URL"
    api_key_env: str = "FIRE_NARRATOR_API_KEY"
    model_env: str = "FIRE_NARRATOR_MODEL"


class NotifierConfig(BaseModel):
    provider: str = "console"


class BudgetsConfig(BaseModel):
    tier0_ms: int = 300
    tier1_ms: int = 3000
    tier2_ms: int = 1500


class FireConfig(BaseModel):
    api: ApiConfig = Field(default_factory=ApiConfig)
    reasoner: ReasonerConfig = Field(default_factory=ReasonerConfig)
    narrator: NarratorConfig = Field(default_factory=NarratorConfig)
    notifier: NotifierConfig = Field(default_factory=NotifierConfig)
    budgets: BudgetsConfig = Field(default_factory=BudgetsConfig)


def load_fire_config(path: Path) -> FireConfig:
    raw: dict[str, Any] = yaml.safe_load(path.read_text()) or {}
    return FireConfig.model_validate(raw)


# Per-system config: what a person decides. What the system contains is discovered, never
# listed here. Contract: docs/specs/system-config-and-discovery.md.


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SlackTarget(_Strict):
    # Public channel ids start with C, private ones with G. The id survives channel renames.
    channel_id: str = Field(pattern=r"^[CG][A-Z0-9]{8,}$")
    channel_name: str | None = Field(default=None, pattern=r"^#[a-z0-9_-]{1,80}$")


class NotifyConfig(_Strict):
    slack: SlackTarget


class ManifestConfig(_Strict):
    kind: str = Field(min_length=1)
    path_env: str = Field(min_length=1)


class DiscoveryConfig(_Strict):
    manifest: ManifestConfig | None = None


class KinesisSourceConfig(_Strict):
    # The `source` of every observation read from it; unique within the system.
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    kind: Literal["kinesis"]
    stream_env: str = Field(min_length=1)
    regions: list[str] = Field(min_length=1)
    dedupe_key: str | None = None


class PolicyConfig(_Strict):
    critical: list[str] = Field(default_factory=list)
    ignore: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _critical_and_ignore_are_disjoint(self) -> PolicyConfig:
        both = sorted(set(self.critical) & set(self.ignore))
        if both:
            raise ValueError(f"names both critical and ignored: {', '.join(both)}")
        return self


class FingerprintingConfig(_Strict):
    sample_rate: float = Field(default=1.0, gt=0, le=1)


class SystemConfig(_Strict):
    system: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    display_name: str
    notify: NotifyConfig
    discovery: DiscoveryConfig = Field(default_factory=DiscoveryConfig)
    sources: list[KinesisSourceConfig] = Field(default_factory=list)
    policy: PolicyConfig = Field(default_factory=PolicyConfig)
    fingerprinting: FingerprintingConfig = Field(default_factory=FingerprintingConfig)

    @model_validator(mode="after")
    def _source_ids_are_unique(self) -> SystemConfig:
        ids = [source.id for source in self.sources]
        duplicates = sorted({source_id for source_id in ids if ids.count(source_id) > 1})
        if duplicates:
            raise ValueError(f"duplicate source ids: {', '.join(duplicates)}")
        return self

    def source_ids(self) -> frozenset[str]:
        return frozenset(source.id for source in self.sources)


def load_system_config(path: Path) -> SystemConfig:
    raw: dict[str, Any] = yaml.safe_load(path.read_text()) or {}
    config = SystemConfig.model_validate(raw)
    if config.system != path.stem:
        raise ValueError(f"{path.name}: system {config.system!r} must match the file name")
    return config


def load_system_configs(directory: Path) -> dict[str, SystemConfig]:
    """Every `<system>.yml` in `directory`; files starting with `_` are skipped."""
    if not directory.is_dir():
        return {}
    configs = [
        load_system_config(path)
        for path in sorted(directory.glob("*.yml"))
        if not path.name.startswith("_")
    ]
    return {config.system: config for config in configs}
