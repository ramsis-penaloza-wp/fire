"""Legacy string cardinality ↔ Shape cardinality (null | void | int).

Strings (`optional` / `one` / `many`) are never stored on ShapeField — only this
resolver maps them. For `many`, pass observed N when converting to Shape.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from fire.core.domain.dossier import CardinalityValue


class LegacyCardinality(StrEnum):
    """Wire/legacy labels — resolver only, not Shape serialization."""

    OPTIONAL = "optional"
    ONE = "one"
    MANY = "many"
    VOID = "void"


def to_shape_cardinality(
    legacy: LegacyCardinality | str, *, n: int | None = None
) -> CardinalityValue:
    """Map legacy label → Shape cardinality.

    many requires n (observed array length, including 0).
    """
    label = LegacyCardinality(legacy)
    if label is LegacyCardinality.OPTIONAL:
        return None
    if label is LegacyCardinality.ONE:
        return 1
    if label is LegacyCardinality.VOID:
        return "void"
    if label is LegacyCardinality.MANY:
        if n is None:
            raise ValueError("many requires n (observed array length)")
        if n < 0:
            raise ValueError("n must be >= 0")
        return n
    raise ValueError(f"unsupported legacy cardinality: {legacy!r}")


def from_shape_cardinality(value: CardinalityValue) -> LegacyCardinality:
    """Best-effort reverse map for display / legacy callers."""
    if value is None:
        return LegacyCardinality.OPTIONAL
    if value == "void":
        return LegacyCardinality.VOID
    if isinstance(value, int):
        if value == 1:
            return LegacyCardinality.ONE
        return LegacyCardinality.MANY
    raise TypeError(f"invalid shape cardinality: {value!r}")
