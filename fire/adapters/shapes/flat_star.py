"""Flat-star ShapeGeneralizer: JSON → ShapeField paths with [*] / #variant.

Homogeneous arrays share one template under `path[*].…`; mixed arrays use
`path[*]#k.…`. Never emit `path[0]`…`path[N]` index paths, values, or fingerprints.
Array cardinality is observed len as digit string ("2"); null | "void" | "n" aside.

A string gets a refined type only in one exact form: a structural check (length, separator
positions) followed by a stdlib or Pydantic parser. Any other string is `string`, so a field
keeps its type whatever its value. Contract: docs/specs/shape-generalizer.md.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import date, datetime
from typing import Any
from uuid import UUID

from pydantic import AnyUrl, EmailStr, TypeAdapter

from fire.core.domain.dossier import (
    Shape,
    ShapeField,
    ValueType,
)

_EMAIL = TypeAdapter(EmailStr)
_URL = TypeAdapter(AnyUrl)


def _parses(parse: Callable[[str], object], value: str) -> bool:
    try:
        parse(value)
    except ValueError:
        return False
    return True


def _has_whitespace(value: str) -> bool:
    return any(char.isspace() for char in value)


def _is_uuid(value: str) -> bool:
    """Canonical 8-4-4-4-12 hex in either case.

    `UUID` alone also takes braces, `urn:uuid:`, bare hex, misplaced hyphens, and `+` or `_`.
    """
    try:
        return str(UUID(value)) == value.lower()
    except ValueError:
        return False


def _is_email(value: str) -> bool:
    """A plain address. `EmailStr` alone also takes `Name <addr>` and strips padding."""
    if "<" in value or _has_whitespace(value):
        return False
    return _parses(_EMAIL.validate_python, value)


def _is_uri(value: str) -> bool:
    """`scheme://…`. `AnyUrl` alone also takes `scheme:path` and drops or encodes whitespace."""
    if "://" not in value or _has_whitespace(value):
        return False
    return _parses(_URL.validate_python, value)


def _is_date(value: str) -> bool:
    """`YYYY-MM-DD`. `date.fromisoformat` alone also takes `YYYYMMDD` and week dates."""
    return len(value) == 10 and value[4] == value[7] == "-" and _parses(date.fromisoformat, value)


def _is_datetime(value: str) -> bool:
    """`YYYY-MM-DD`, `T` or a space, a clock, then an optional `Z`, `+HH:MM`, or `-HH:MM`.

    `datetime.fromisoformat` alone also takes any separator, hour-only and basic (`HHMMSS`,
    `+HHMM`) forms, and comma fractions.
    """
    if len(value) < 16 or value[10] not in "T " or not _is_date(value[:10]):
        return False
    return _is_clock(_without_offset(value[11:])) and _parses(datetime.fromisoformat, value)


def _without_offset(time: str) -> str:
    if time.endswith("Z"):
        return time[:-1]
    if len(time) > 6 and time[-6] in "+-" and time[-3] == ":":
        return time[:-6]
    return time


def _is_clock(clock: str) -> bool:
    """`HH:MM`, `HH:MM:SS`, or `HH:MM:SS.` plus digits; the parser checks the ranges."""
    if len(clock) < 5 or clock[2] != ":":
        return False
    if len(clock) == 5:
        return True
    if len(clock) < 8 or clock[5] != ":":
        return False
    return len(clock) == 8 or (clock[8] == "." and clock[9:].isdigit())


def _classify_string(value: str) -> ValueType:
    if _is_uuid(value):
        return ValueType.UUID
    if _is_email(value):
        return ValueType.EMAIL
    if _is_uri(value):
        return ValueType.URI
    if _is_date(value):
        return ValueType.DATE
    if _is_datetime(value):
        return ValueType.DATETIME
    return ValueType.STRING


def _classify_leaf(value: Any) -> ValueType:
    if value is None:
        return ValueType.NULL
    if isinstance(value, bool):
        return ValueType.BOOLEAN
    if isinstance(value, int):
        return ValueType.INTEGER
    if isinstance(value, float):
        return ValueType.NUMBER
    if isinstance(value, str):
        return _classify_string(value)
    return ValueType.UNKNOWN


def _structural_signature(value: Any) -> tuple[Any, ...]:
    """Canonical structural id for equality (keys sorted; no values).

    Arrays are length-agnostic: homogeneous → ("array", element_sig);
    mixed → ("array", ("mixed", unique_element_sigs…)). Never encode length.
    Leaf sig uses ValueType so integer ≠ number and uuid ≠ string.
    """
    if isinstance(value, dict):
        return (
            "object",
            tuple(
                (key, _structural_signature(child))
                for key, child in sorted(value.items(), key=lambda kv: kv[0])
            ),
        )
    if isinstance(value, list):
        if not value:
            return ("array", ("empty",))
        unique: list[tuple[Any, ...]] = []
        seen: set[tuple[Any, ...]] = set()
        for item in value:
            sig = _structural_signature(item)
            if sig not in seen:
                seen.add(sig)
                unique.append(sig)
        if len(unique) == 1:
            return ("array", unique[0])
        return ("array", ("mixed", tuple(unique)))
    return (_classify_leaf(value).value,)


def _child_path(prefix: str, key: str) -> str:
    return f"{prefix}.{key}" if prefix else key


def _star_path(prefix: str) -> str:
    return f"{prefix}[*]" if prefix else "[*]"


def _variant_path(prefix: str, index: int) -> str:
    return f"{prefix}[*]#{index}" if prefix else f"[*]#{index}"


def _is_array_element_slot(path: str) -> bool:
    """True for paths ending in `[*]` or `[*]#k`."""
    if not path:
        return False
    return bool(re.search(r"\[\*\](#\d+)?$", path))


class FlatStarShapeGeneralizer:
    """Pure adapter: walk JSON, emit deduped flat Shape fields."""

    def generalize(self, payload: Any) -> Shape:
        fields: list[ShapeField] = []
        self._walk(payload, "", fields)
        fields.sort(key=lambda field: field.path)
        return Shape(fields=fields)

    def _walk(self, value: Any, path: str, fields: list[ShapeField]) -> None:
        if isinstance(value, dict):
            if not value:
                if path:
                    fields.append(
                        ShapeField(
                            path=path,
                            value_type=ValueType.OBJECT,
                            cardinality="void",
                        )
                    )
                return
            if path and not _is_array_element_slot(path):
                fields.append(
                    ShapeField(
                        path=path,
                        value_type=ValueType.OBJECT,
                        cardinality="1",
                    )
                )
            for key, child in sorted(value.items(), key=lambda kv: kv[0]):
                self._walk(child, _child_path(path, key), fields)
            return
        if isinstance(value, list):
            fields.append(
                ShapeField(
                    path=path,
                    value_type=ValueType.ARRAY,
                    cardinality=str(len(value)),
                )
            )
            if not value:
                return
            self._walk_array_elements(value, path, fields)
            return

        leaf = _classify_leaf(value)
        cardinality: str | None = None if leaf is ValueType.NULL else "1"
        fields.append(ShapeField(path=path, value_type=leaf, cardinality=cardinality))

    def _walk_array_elements(self, items: list[Any], path: str, fields: list[ShapeField]) -> None:
        order: list[tuple[Any, ...]] = []
        representatives: dict[tuple[Any, ...], Any] = {}
        for item in items:
            sig = _structural_signature(item)
            if sig not in representatives:
                representatives[sig] = item
                order.append(sig)

        if len(order) == 1:
            self._walk(representatives[order[0]], _star_path(path), fields)
            return

        for index, sig in enumerate(order):
            self._walk(representatives[sig], _variant_path(path, index), fields)
