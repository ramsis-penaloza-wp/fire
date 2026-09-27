"""Flat-star ShapeGeneralizer: JSON → ShapeField paths with [*] / #variant.

Locked model (Project Ops 2026-09-27): homogeneous arrays share one template under
`path[*].…`; mixed arrays use `path[*]#k.…`. Never emit `path[0]`…`path[N]`,
element counts, values, or fingerprints.

ValueType is a single domain StrEnum; leaves are inferred by form only.
"""

from __future__ import annotations

import re
from typing import Any

from fire.core.domain.dossier import (
    Cardinality,
    Shape,
    ShapeField,
    ValueType,
)

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$"
)
_URI_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://\S+$")
_BASE64_RE = re.compile(r"^[A-Za-z0-9+/]+={0,2}$")


def _classify_string(value: str) -> ValueType:
    """Form-only string refinements; never store the value on Shape."""
    if _UUID_RE.match(value):
        return ValueType.UUID
    if _EMAIL_RE.match(value):
        return ValueType.EMAIL
    if _URI_RE.match(value):
        return ValueType.URI
    if _DATETIME_RE.match(value):
        return ValueType.DATETIME
    if _DATE_RE.match(value):
        return ValueType.DATE
    # base64-ish: long enough, charset + optional padding — not MIME media
    if len(value) >= 16 and len(value) % 4 == 0 and _BASE64_RE.match(value):
        return ValueType.BINARY
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
                            cardinality=Cardinality.ONE,
                        )
                    )
                return
            if path and not _is_array_element_slot(path):
                fields.append(
                    ShapeField(
                        path=path,
                        value_type=ValueType.OBJECT,
                        cardinality=Cardinality.ONE,
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
                    cardinality=Cardinality.MANY,
                )
            )
            if not value:
                return
            self._walk_array_elements(value, path, fields)
            return

        leaf = _classify_leaf(value)
        cardinality = (
            Cardinality.OPTIONAL if leaf is ValueType.NULL else Cardinality.ONE
        )
        fields.append(
            ShapeField(path=path, value_type=leaf, cardinality=cardinality)
        )

    def _walk_array_elements(
        self, items: list[Any], path: str, fields: list[ShapeField]
    ) -> None:
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
