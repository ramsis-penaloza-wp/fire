"""FireConfig: the global config, and the tentacle block it gained in step 1.3."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from fire.apps.config import FireConfig, TentacleConfig, load_fire_config

REPO_CONFIG = Path(__file__).resolve().parents[2] / "config" / "fire.yml"


@pytest.mark.unit
def test_tentacle_defaults_match_the_spec() -> None:
    tentacle = FireConfig().tentacle
    assert tentacle.body_url_env == "FIRE_BODY_URL"
    assert (tentacle.batch_size, tentacle.flush_ms, tentacle.buffer_size) == (200, 1000, 10000)
    assert (tentacle.retry.max_attempts, tentacle.retry.backoff_ms) == (6, 500)
    assert tentacle.shutdown_s == 10


@pytest.mark.unit
def test_repo_fire_yml_sets_the_defaults() -> None:
    assert load_fire_config(REPO_CONFIG).tentacle == TentacleConfig()


@pytest.mark.unit
@pytest.mark.parametrize(
    "tentacle",
    [
        {"batchsize": 10},
        {"retry": {"attempts": 3}},
        {"batch_size": 0},
        {"flush_ms": 0},
        {"buffer_size": 0},
        {"retry": {"max_attempts": 0}},
        {"retry": {"backoff_ms": -1}},
        {"shutdown_s": 0},
        {"body_url_env": ""},
    ],
    ids=[
        "unknown-key",
        "unknown-retry-key",
        "batch-size",
        "flush-ms",
        "buffer-size",
        "max-attempts",
        "backoff-ms",
        "shutdown-s",
        "body-url-env",
    ],
)
def test_tentacle_rejects_bad_values(tentacle: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        FireConfig.model_validate({"tentacle": tentacle})
