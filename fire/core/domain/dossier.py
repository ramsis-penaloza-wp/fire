"""Incident dossier: shape + process path evidence (never raw payload values)."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, Field, model_validator

from fire.core.domain.origin import Origin


class EvidenceQuality(StrEnum):
    PRESENT = "present"
    MISSING = "missing"


class ValueType(StrEnum):
    """Formal leaf/structure types for ShapeField — never runtime values.

    Covers structure, the integer/number split, and string refinements recognized by one
    exact form each: canonical hyphenated uuid, plain email address, `scheme://` URI,
    ISO 8601 `YYYY-MM-DD` date, and RFC 3339-style datetime. Every other string is
    `string`; nothing is guessed from content (no binary or media detection).
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
    UNKNOWN = "unknown"


CardinalityValue = Annotated[str, Field(pattern=r"^(void|n|0|[1-9][0-9]*)$")] | None


class ShapeField(BaseModel):
    """One structural field: path + type + cardinality + optional. Never a value.

    cardinality is null, "void", "n" (any number), or a non-negative integer as a digit
    string without leading zeros ("0", "1", "42"); never a JSON number. The generalizer
    emits null for null, "void" for an object that is always empty and "1" otherwise, "0"
    for an array that is always empty and "n" otherwise, and "1" for every other leaf.

    optional is true when the key may be absent: some object at the parent path lacks it.
    It is relative to the parent, so fields below an optional key judge their own keys only.
    """

    path: str
    value_type: ValueType = ValueType.UNKNOWN
    cardinality: CardinalityValue = "1"
    optional: bool = False


class Shape(BaseModel):
    """Reusable structure description — keys/types/cardinality only.

    Produced by ShapeGeneralizer: flat paths with [*] for array elements, one field per
    (path, value_type), sorted by that pair. Not a fingerprint.
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

    Prefer `from_ref` / `to_ref` (origin_id or service name). Legacy `service`
    remains for older callers; at least one identity must be present.
    """

    from_ref: str | None = Field(default=None, description="origin_id or service at hop start")
    to_ref: str | None = Field(default=None, description="origin_id or service at hop end")
    origin_id: str | None = Field(
        default=None, description="Primary origin_id for this hop when single-node"
    )
    service: str | None = Field(
        default=None, description="Legacy service label kept for older callers"
    )
    operation: str | None = None
    span_kind: str | None = None
    status: str | None = None

    @model_validator(mode="after")
    def _require_identity(self) -> ProcessHop:
        if not any((self.from_ref, self.to_ref, self.origin_id, self.service)):
            raise ValueError("ProcessHop needs from_ref, to_ref, origin_id, or service")
        return self

    def display_label(self) -> str:
        """Human-readable hop label without inventing probabilities."""
        if self.from_ref and self.to_ref:
            base = f"{self.from_ref}->{self.to_ref}"
        elif self.origin_id:
            base = self.origin_id
        elif self.service:
            base = self.service
        elif self.to_ref:
            base = self.to_ref
        else:
            base = self.from_ref or "unknown"
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
