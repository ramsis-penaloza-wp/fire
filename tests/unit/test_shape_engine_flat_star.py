"""Shape engine v0: flat paths + [*] / #variant (no fingerprint, no counts)."""

from __future__ import annotations

import pytest

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.core.domain.dossier import Cardinality, ValueType
from fire.core.ports.shape_generalizer import ShapeGeneralizer


def _paths(shape) -> dict[str, tuple[str, str]]:
    return {
        field.path: (field.value_type.value, field.cardinality.value)
        for field in shape.fields
    }


@pytest.fixture
def engine() -> FlatStarShapeGeneralizer:
    return FlatStarShapeGeneralizer()


@pytest.mark.unit
def test_port_protocol(engine: FlatStarShapeGeneralizer) -> None:
    assert isinstance(engine, ShapeGeneralizer)


@pytest.mark.unit
def test_empty_object_root(engine: FlatStarShapeGeneralizer) -> None:
    shape = engine.generalize({})
    assert shape.fields == []


@pytest.mark.unit
def test_empty_array_root(engine: FlatStarShapeGeneralizer) -> None:
    shape = engine.generalize([])
    assert _paths(shape) == {"": ("array", "many")}
    assert all("[0]" not in f.path and "[1]" not in f.path for f in shape.fields)


@pytest.mark.unit
def test_null_leaf_optional(engine: FlatStarShapeGeneralizer) -> None:
    shape = engine.generalize({"a": None})
    assert _paths(shape)["a"] == ("null", "optional")
    # missing keys are not invented
    assert "b" not in _paths(shape)


@pytest.mark.unit
def test_homogeneous_order_lines(engine: FlatStarShapeGeneralizer) -> None:
    payload = {
        "order_id": "x",
        "lines": [
            {"sku": "a", "qty": 1},
            {"sku": "b", "qty": 2},
        ],
    }
    paths = _paths(engine.generalize(payload))
    assert paths["order_id"] == ("string", "one")
    assert paths["lines"] == ("array", "many")
    assert paths["lines[*].qty"] == ("integer", "one")
    assert paths["lines[*].sku"] == ("string", "one")
    assert "lines[0]" not in paths
    assert "lines[1]" not in paths
    assert "lines[*]" not in paths  # non-empty object slot: children only


@pytest.mark.unit
def test_mixed_scalars_and_object(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(engine.generalize({"events": [1, "a", {}]}))
    assert paths["events"] == ("array", "many")
    assert paths["events[*]#0"] == ("integer", "one")
    assert paths["events[*]#1"] == ("string", "one")
    assert paths["events[*]#2"] == ("object", "one")
    assert not any(p.endswith("[0]") or "[1]." in p for p in paths)


@pytest.mark.unit
def test_mixed_object_variants(engine: FlatStarShapeGeneralizer) -> None:
    payload = {
        "events": [
            {"type": "n", "n": 1},
            {"type": "m", "msg": "x"},
        ]
    }
    paths = _paths(engine.generalize(payload))
    assert paths["events"] == ("array", "many")
    assert paths["events[*]#0.n"] == ("integer", "one")
    assert paths["events[*]#0.type"] == ("string", "one")
    assert paths["events[*]#1.msg"] == ("string", "one")
    assert paths["events[*]#1.type"] == ("string", "one")


@pytest.mark.unit
def test_array_of_arrays_homogeneous(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(engine.generalize({"matrix": [[1, 2], [3, 4]]}))
    assert paths["matrix"] == ("array", "many")
    assert paths["matrix[*]"] == ("array", "many")
    assert paths["matrix[*][*]"] == ("integer", "one")


@pytest.mark.unit
def test_structural_equality_ignores_key_order(engine: FlatStarShapeGeneralizer) -> None:
    payload = {
        "lines": [
            {"qty": 1, "sku": "a"},
            {"sku": "b", "qty": 2},
        ]
    }
    paths = _paths(engine.generalize(payload))
    assert "lines[*].sku" in paths
    assert "lines[*].qty" in paths
    assert not any("#" in p for p in paths)  # still homogeneous


@pytest.mark.unit
def test_no_values_or_counts_in_shape(engine: FlatStarShapeGeneralizer) -> None:
    shape = engine.generalize({"n": 99, "items": [1, 1, 1, 1]})
    dumped = shape.model_dump()
    assert "99" not in str(dumped)
    assert "count" not in str(dumped).lower()
    assert all("[0]" not in f.path for f in shape.fields)


@pytest.mark.unit
def test_homogeneous_rows_different_lengths(engine: FlatStarShapeGeneralizer) -> None:
    """Length must not force #variant — [[1],[1,2,3]] is still matrix[*][*] integer."""
    paths = _paths(engine.generalize({"matrix": [[1], [1, 2, 3]]}))
    assert paths["matrix"] == ("array", "many")
    assert paths["matrix[*]"] == ("array", "many")
    assert paths["matrix[*][*]"] == ("integer", "one")
    assert not any("#" in p for p in paths)


@pytest.mark.unit
def test_integer_vs_number(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(engine.generalize({"n": 1, "x": 1.5}))
    assert paths["n"] == ("integer", "one")
    assert paths["x"] == ("number", "one")


@pytest.mark.unit
def test_refined_string_types(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(
        engine.generalize(
            {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "mail": "a@b.co",
                "link": "https://example.com/x",
                "when": "2024-01-15T12:00:00Z",
                "day": "2024-01-15",
                "blob": "SGVsbG8gV29ybGQhISE=",  # 20 chars, base64
                "plain": "hello",
            }
        )
    )
    assert paths["id"] == ("uuid", "one")
    assert paths["mail"] == ("email", "one")
    assert paths["link"] == ("uri", "one")
    assert paths["when"] == ("datetime", "one")
    assert paths["day"] == ("date", "one")
    assert paths["blob"] == ("binary", "one")
    assert paths["plain"] == ("string", "one")


@pytest.mark.unit
def test_value_type_is_formal_enum() -> None:
    assert ValueType.INTEGER.value == "integer"
    assert ValueType.DATETIME.value == "datetime"
    assert "image" not in {m.value for m in ValueType}  # media MIME = P1
