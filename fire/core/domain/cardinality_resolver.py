"""Legacy string cardinality ↔ Shape cardinality (digit strings).

Shape stores null | "void" | "0"…"N" | "n" — never typed ints, never one/many.
Legacy labels map only here.
"""

from __future__ import annotations

from enum import StrEnum

from fire.core.domain.dossier import CardinalityValue


class LegacyCardinality(StrEnum):
    """Wire/legacy labels — resolver only, not Shape serialization."""

    OPTIONAL = "optional"
    ONE = "one"
    MANY = "many"
    VOID = "void"


def to_shape_cardinality(legacy: LegacyCardinality | str) -> CardinalityValue:
    """Map legacy label → Shape cardinality.

    many → "n" (unbounded). Observed array lengths use format_count(n).
    """
    label = LegacyCardinality(legacy)
    if label is LegacyCardinality.OPTIONAL:
        return "0"
    if label is LegacyCardinality.ONE:
        return "1"
    if label is LegacyCardinality.VOID:
        return "void"
    if label is LegacyCardinality.MANY:
        return "n"
    raise ValueError(f"unsupported legacy cardinality: {legacy!r}")


def format_count(n: int) -> str:
    """Observed countable N → digit string (arrays). n must be >= 0."""
    if n < 0:
        raise ValueError("n must be >= 0")
    return str(n)


def from_shape_cardinality(value: CardinalityValue) -> LegacyCardinality:
    """Best-effort reverse map for display / legacy callers."""
    if value is None:
        return LegacyCardinality.OPTIONAL
    if value == "void":
        return LegacyCardinality.VOID
    if value == "0":
        return LegacyCardinality.OPTIONAL
    if value == "1":
        return LegacyCardinality.ONE
    if value == "n":
        return LegacyCardinality.MANY
    if isinstance(value, str) and value.isdigit():
        return LegacyCardinality.MANY  # finite N > 1 (or 0 already handled)
    raise TypeError(f"invalid shape cardinality: {value!r}")
