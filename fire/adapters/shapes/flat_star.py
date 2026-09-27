"""Flat-star ShapeGeneralizer: JSON → ShapeField paths with [*] / #variant.

Locked model (Project Ops 2026-09-27): homogeneous arrays share one template under
`path[*].…`; mixed arrays use `path[*]#k.…`. Never emit `path[0]`…`path[N]` index paths, values, or fingerprints.
Array cardinality is observed len as digit string ("2"); null | "void" | "n" aside.

Leaf inference uses Pydantic TypeAdapter (UUID, EmailStr, AnyUrl, date, datetime,
StrictInt) plus stdlib base64.b64decode(validate=True) for binary — no homemade
regex classifiers.
"""

from __future__ import annotations

import base64
import re
from datetime import date, datetime
from typing import Any
from uuid import UUID

from pydantic import AnyUrl, EmailStr, StrictInt, TypeAdapter, ValidationError

from fire.core.domain.cardinality_resolver import format_count
from fire.core.domain.dossier import (
    Shape,
    ShapeField,
    ValueType,
)

_UUID = TypeAdapter(UUID)
_EMAIL = TypeAdapter(EmailStr)
_URL = TypeAdapter(AnyUrl)
_DATE = TypeAdapter(date)
_DATETIME = TypeAdapter(datetime)
_STRICT_INT = TypeAdapter(StrictInt)

_MIN_BINARY_LEN = 16


def _try_adapter(adapter: TypeAdapter[Any], value: str) -> bool:
    try:
        adapter.validate_python(value)
        return True
    except ValidationError:
        return False


def _is_binary(value: str) -> bool:
    """Slim standard check: stdlib base64 decode with validate=True."""
    if len(value) < _MIN_BINARY_LEN or len(value) % 4 != 0:
        return False
    try:
        base64.b64decode(value, validate=True)
        return True
    except (ValueError, TypeError):
        return False


def _classify_string(value: str) -> ValueType:
    """Form-only string refinements via Pydantic / stdlib; never store the value."""
    if _try_adapter(_UUID, value):
        return ValueType.UUID
    if _try_adapter(_EMAIL, value):
        return ValueType.EMAIL
    if _try_adapter(_URL, value):
        return ValueType.URI
    # date before datetime: Pydantic accepts YYYY-MM-DD as datetime midnight
    if _try_adapter(_DATE, value):
        return ValueType.DATE
    if _try_adapter(_DATETIME, value):
        return ValueType.DATETIME
    if _is_binary(value):
        return ValueType.BINARY
    return ValueType.STRING


def _classify_leaf(value: Any) -> ValueType:
    if value is None:
        return ValueType.NULL
    if isinstance(value, bool):
        return ValueType.BOOLEAN
    if isinstance(value, int) and not isinstance(value, bool):
        # Prefer StrictInt semantics (bool already excluded).
        try:
            _STRICT_INT.validate_python(value)
            return ValueType.INTEGER
        except ValidationError:
            return ValueType.NUMBER
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
                    cardinality=format_count(len(value)),
                )
            )
            if not value:
                return
            self._walk_array_elements(value, path, fields)
            return

        leaf = _classify_leaf(value)
        cardinality: str | None = None if leaf is ValueType.NULL else "1"
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
