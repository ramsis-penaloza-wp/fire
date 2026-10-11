"""Baselines: per stream key, what the observations folded so far carried.

Contract: docs/specs/ingest-drift-and-feedback.md#baseline.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from fire.core.domain.dossier import Shape, ValueType


class DriftPolicy(BaseModel):
    """When a stream's baseline is warm, and which fields it requires. Set per system."""

    model_config = ConfigDict(extra="forbid")

    # Observations folded before a stream is warm and may raise drift.
    min_samples: int = Field(default=20, ge=1)
    # A `(path, value_type)` carried by at least this share of the samples is required.
    required_ratio: float = Field(default=1.0, gt=0, le=1, allow_inf_nan=False)


class BaselineStatus(StrEnum):
    REBUILDING = "baseline rebuilding"
    WARM = "warm"


@dataclass
class Baseline:
    """One stream key's baseline: how many observations it folded, and what they carried.

    `presence` counts the observations that carried each `(path, value_type)`; a shape listing
    one pair twice counts once. `fingerprints` holds the schema hashes folded so far.
    """

    samples: int = 0
    presence: Counter[tuple[str, ValueType]] = field(default_factory=Counter)
    fingerprints: set[str] = field(default_factory=set)

    def status(self, policy: DriftPolicy) -> BaselineStatus:
        if self.samples >= policy.min_samples:
            return BaselineStatus.WARM
        return BaselineStatus.REBUILDING

    def compares(self, schema_hash: str, policy: DriftPolicy) -> bool:
        """Whether an observation with this fingerprint is compared before it folds.

        Only on a warm baseline, and only for a fingerprint it has not folded before.
        """
        return self.status(policy) is BaselineStatus.WARM and schema_hash not in self.fingerprints

    def fold(self, schema_hash: str, shape: Shape) -> None:
        self.samples += 1
        self.presence.update({(f.path, f.value_type) for f in shape.fields})
        self.fingerprints.add(schema_hash)
