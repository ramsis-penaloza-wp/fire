"""Incident dossier: shape + process path evidence (never raw payload values)."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class EvidenceQuality(StrEnum):
    PRESENT = "present"
    MISSING = "missing"


class ValueType(StrEnum):
    """JSON-ish type labels for shape fields — not runtime values."""

    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    OBJECT = "object"
    ARRAY = "array"
    NULL = "null"
    UNKNOWN = "unknown"


class Cardinality(StrEnum):
    ONE = "one"
    MANY = "many"
    OPTIONAL = "optional"


class ShapeField(BaseModel):
    """One structural field: path + type + cardinality. Never a value."""

    path: str
    value_type: ValueType = ValueType.UNKNOWN
    cardinality: Cardinality = Cardinality.ONE


class Shape(BaseModel):
    """Reusable structure description — keys/types/cardinality only.

    Not a generalizer: assembling Shape from raw payloads is a future port.
    """

    fields: list[ShapeField] = Field(default_factory=list)


class ShapeFingerprint(BaseModel):
    """Stable id for a Shape (schema_hash ± normalized paths). Not a generalizer."""

    schema_hash: str
    normalized_paths: list[str] = Field(default_factory=list)
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
    project_id: str
    service: str | None = None
    shape: Shape | None = None
    fingerprint: ShapeFingerprint | None = None
    process_path: ProcessPath = Field(default_factory=ProcessPath)
    fragments: list[EvidenceFragment] = Field(default_factory=list)
    missing: list[EvidenceFragment] = Field(default_factory=list)
