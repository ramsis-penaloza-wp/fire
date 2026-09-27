"""Load YAML config. Credential values stay as env var *names* until wiring."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


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
