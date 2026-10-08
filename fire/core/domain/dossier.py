"""Incident dossier: shape + process path evidence (never raw payload values)."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, model_validator

from fire.core.domain.origin import Origin


class EvidenceQuality(StrEnum):
    PRESENT = "present"
    MISSING = "missing"


class ValueType(StrEnum):
    """Formal leaf/structure types for ShapeField — never runtime values.

    Covers structure, the integer/number split, and string refinements inferred
    by form only. Media MIME types (image/audio/video) are not covered yet.
    """

    OBJECT = "object"
    ARRAY = "array"
    NULL = "null"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    NUMBER = "number"  # float / non-int numeric
    STRING = "string"
    DATETIME = "datetime"
    DATE = "date"
    UUID = "uuid"
    URI = "uri"
    EMAIL = "email"
    BINARY = "binary"  # stdlib base64 decode (validate=True); not MIME media
    UNKNOWN = "unknown"


# Shape cardinality: null | "void" | digit string "0"…"N" | "n" (unbounded).
# Never a JSON int. No legacy one/many/optional labels.
CardinalityValue = str | None  # null | "void" | "0"…"N" | "n"


class ShapeField(BaseModel):
    """One structural field: path + type + cardinality. Never a value.

    cardinality is null | "void" | "0"…"N" | "n" (unbounded). Arrays use
    observed len as a digit string (e.g. len 2 → "2"). Never a typed int.
    """

    path: str
    value_type: ValueType = ValueType.UNKNOWN
    cardinality: CardinalityValue = "1"


class Shape(BaseModel):
    """Reusable structure description — keys/types/cardinality only.

    Produced by ShapeGeneralizer (flat paths with [*] / #variant). Not a fingerprint.
    """

    fields: list[ShapeField] = Field(default_factory=list)


class ShapeFingerprint(BaseModel):
    """Stable id for a Shape (schema_hash ± normalized paths). Not a generalizer."""

    schema_hash: str
    normalized_paths: list[str] = Field(default_factory=list)
    notes: str | None = None


class ShapeRecord(BaseModel):
    """Domain association of a Shape to a project Origin — not a DB model."""

    project_id: str
    origin_id: str
    shape: Shape
    fingerprint: ShapeFingerprint


class ProcessHop(BaseModel):
    """One hop on the path by reference — shape-safe; never embeds Shape.

    A hop is either a node (`ref`) or an edge (`from_ref` and/or `to_ref`). Every ref
    is an origin_id or a service name.
    """

    ref: str | None = Field(default=None, min_length=1, description="Node: origin_id or service")
    from_ref: str | None = Field(
        default=None, min_length=1, description="Edge start: origin_id or service"
    )
    to_ref: str | None = Field(
        default=None, min_length=1, description="Edge end: origin_id or service"
    )
    operation: str | None = None
    span_kind: str | None = None
    status: str | None = None

    @model_validator(mode="after")
    def _require_one_identity_form(self) -> ProcessHop:
        is_edge = self.from_ref is not None or self.to_ref is not None
        if self.ref is not None and is_edge:
            raise ValueError("ProcessHop is either a node (ref) or an edge (from_ref/to_ref)")
        if self.ref is None and not is_edge:
            raise ValueError("ProcessHop needs ref, from_ref, or to_ref")
        return self

    def display_label(self) -> str:
        """Human-readable hop label without inventing probabilities."""
        if self.ref is not None:
            base = self.ref
        elif self.from_ref is not None and self.to_ref is not None:
            base = f"{self.from_ref}->{self.to_ref}"
        else:
            base = self.from_ref or self.to_ref
        if self.operation:
            return f"{base}/{self.operation}"
        return base


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
    origin: Origin | None = None
    shape: Shape | None = None
    fingerprint: ShapeFingerprint | None = None
    shape_record: ShapeRecord | None = None
    process_path: ProcessPath = Field(default_factory=ProcessPath)
    fragments: list[EvidenceFragment] = Field(default_factory=list)
    missing: list[EvidenceFragment] = Field(default_factory=list)
