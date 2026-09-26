"""Incident dossier: shape + process path evidence (never raw payload values)."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class EvidenceQuality(StrEnum):
    PRESENT = "present"
    MISSING = "missing"


class ShapeFingerprint(BaseModel):
    """Structural fingerprint of a payload — keys/types/cardinality, not values."""

    schema_hash: str
    field_paths: list[str] = Field(default_factory=list)
    notes: str | None = None


class ProcessHop(BaseModel):
    """One hop on the request path (service/span/change), shape-safe."""

    service: str
    operation: str | None = None
    span_kind: str | None = None
    status: str | None = None


class ProcessPath(BaseModel):
    hops: list[ProcessHop] = Field(default_factory=list)


class EvidenceFragment(BaseModel):
    collector: str
    quality: EvidenceQuality
    summary: str
    data: dict[str, Any] = Field(
        default_factory=dict,
        description="Shape-safe metadata only — never raw payload field values.",
    )


class Dossier(BaseModel):
    incident_id: str
    system: str
    shape: ShapeFingerprint | None = None
    process_path: ProcessPath = Field(default_factory=ProcessPath)
    fragments: list[EvidenceFragment] = Field(default_factory=list)
    missing: list[EvidenceFragment] = Field(default_factory=list)
