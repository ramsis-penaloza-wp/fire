"""Flat-star ShapeGeneralizer: JSON → ShapeField paths with [*] / #variant.

Locked model (Project Ops 2026-09-27): homogeneous arrays share one template under
`path[*].…`; mixed arrays use `path[*]#k.…`. Never emit `path[0]`…`path[N]`,
element counts, values, or fingerprints.
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


def _structural_signature(value: Any) -> tuple[Any, ...]:
    """Canonical structural id for equality (keys sorted; no values).

    Arrays are length-agnostic: homogeneous → ("array", element_sig);
    mixed → ("array", ("mixed", unique_element_sigs…)). Never encode length.
    """
    if value is None:
        return ("null",)
    if isinstance(value, bool):
        return ("boolean",)
    if isinstance(value, (int, float)):
        return ("number",)
    if isinstance(value, str):
        return ("string",)
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
    if isinstance(value, dict):
        return (
            "object",
            tuple(
                (key, _structural_signature(child))
                for key, child in sorted(value.items(), key=lambda kv: kv[0])
            ),
        )
    return ("unknown",)


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
        if value is None:
            fields.append(
                ShapeField(
                    path=path,
                    value_type=ValueType.NULL,
                    cardinality=Cardinality.OPTIONAL,
                )
            )
            return
        if isinstance(value, bool):
            fields.append(
                ShapeField(
                    path=path, value_type=ValueType.BOOLEAN, cardinality=Cardinality.ONE
                )
            )
            return
        if isinstance(value, (int, float)):
            fields.append(
                ShapeField(
                    path=path, value_type=ValueType.NUMBER, cardinality=Cardinality.ONE
                )
            )
            return
        if isinstance(value, str):
            fields.append(
                ShapeField(
                    path=path, value_type=ValueType.STRING, cardinality=Cardinality.ONE
                )
            )
            return
        if isinstance(value, dict):
            if not value:
                # Root {} → empty Shape. Nested / slot {} → object|one marker.
                if path:
                    fields.append(
                        ShapeField(
                            path=path,
                            value_type=ValueType.OBJECT,
                            cardinality=Cardinality.ONE,
                        )
                    )
                return
            # Non-empty object under [*] / [*]#k: skip emitting the slot as object;
            # only children (lines[*].sku). Named objects still get object|one.
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
        fields.append(
            ShapeField(
                path=path, value_type=ValueType.UNKNOWN, cardinality=Cardinality.ONE
            )
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
