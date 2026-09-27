"""Shape engine v0: flat paths + [*] / #variant (no fingerprint)."""

from __future__ import annotations

import pytest

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.core.domain.cardinality_resolver import (
    LegacyCardinality,
    format_count,
    from_shape_cardinality,
    to_shape_cardinality,
)
from fire.core.domain.dossier import ValueType
from fire.core.ports.shape_generalizer import ShapeGeneralizer


def _paths(shape) -> dict[str, tuple[str, object]]:
    return {
        field.path: (field.value_type.value, field.cardinality)
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
    assert engine.generalize({}).fields == []


@pytest.mark.unit
def test_empty_array_root(engine: FlatStarShapeGeneralizer) -> None:
    shape = engine.generalize([])
    assert _paths(shape) == {"": ("array", "0")}
    assert all("[0]" not in f.path for f in shape.fields)


@pytest.mark.unit
def test_null_leaf(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(engine.generalize({"a": None}))
    assert paths["a"] == ("null", None)
    assert "b" not in paths


@pytest.mark.unit
def test_homogeneous_order_lines(engine: FlatStarShapeGeneralizer) -> None:
    payload = {
        "order_id": "x",
        "lines": [{"sku": "a", "qty": 1}, {"sku": "b", "qty": 2}],
    }
    paths = _paths(engine.generalize(payload))
    assert paths["order_id"] == ("string", "1")
    assert paths["lines"] == ("array", "2")
    assert paths["lines[*].qty"] == ("integer", "1")
    assert paths["lines[*].sku"] == ("string", "1")
    assert isinstance(paths["lines"][1], str)
    assert "lines[0]" not in paths


@pytest.mark.unit
def test_mixed_scalars_and_object(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(engine.generalize({"events": [1, "a", {}]}))
    assert paths["events"] == ("array", "3")
    assert paths["events[*]#0"] == ("integer", "1")
    assert paths["events[*]#1"] == ("string", "1")
    assert paths["events[*]#2"] == ("object", "void")


@pytest.mark.unit
def test_mixed_object_variants(engine: FlatStarShapeGeneralizer) -> None:
    payload = {"events": [{"type": "n", "n": 1}, {"type": "m", "msg": "x"}]}
    paths = _paths(engine.generalize(payload))
    assert paths["events"] == ("array", "2")
    assert paths["events[*]#0.n"] == ("integer", "1")


@pytest.mark.unit
def test_array_of_arrays_homogeneous(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(engine.generalize({"matrix": [[1, 2], [3, 4]]}))
    assert paths["matrix"] == ("array", "2")
    assert paths["matrix[*]"] == ("array", "2")
    assert paths["matrix[*][*]"] == ("integer", "1")


@pytest.mark.unit
def test_structural_equality_ignores_key_order(engine: FlatStarShapeGeneralizer) -> None:
    payload = {"lines": [{"qty": 1, "sku": "a"}, {"sku": "b", "qty": 2}]}
    paths = _paths(engine.generalize(payload))
    assert "lines[*].sku" in paths
    assert not any("#" in p for p in paths)


@pytest.mark.unit
def test_no_values_in_shape(engine: FlatStarShapeGeneralizer) -> None:
    shape = engine.generalize({"n": 99, "items": [1, 1, 1, 1]})
    assert "99" not in str(shape.model_dump())
    assert _paths(shape)["items"] == ("array", "4")


@pytest.mark.unit
def test_homogeneous_rows_different_lengths(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(engine.generalize({"matrix": [[1], [1, 2, 3]]}))
    assert paths["matrix"] == ("array", "2")
    assert paths["matrix[*]"] == ("array", "1")
    assert paths["matrix[*][*]"] == ("integer", "1")
    assert not any("#" in p for p in paths)


@pytest.mark.unit
def test_integer_vs_number(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(engine.generalize({"n": 1, "x": 1.5}))
    assert paths["n"] == ("integer", "1")
    assert paths["x"] == ("number", "1")


@pytest.mark.unit
def test_refined_string_types(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(
        engine.generalize(
            {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "mail": "user@example.com",
                "link": "https://example.com/x",
                "when": "2024-01-15T12:00:00Z",
                "day": "2024-01-15",
                "blob": "SGVsbG8gV29ybGQhISE=",
                "plain": "hello",
            }
        )
    )
    assert paths["id"] == ("uuid", "1")
    assert paths["mail"] == ("email", "1")
    assert paths["link"] == ("uri", "1")
    assert paths["when"] == ("datetime", "1")
    assert paths["day"] == ("date", "1")
    assert paths["blob"] == ("binary", "1")
    assert paths["plain"] == ("string", "1")


@pytest.mark.unit
def test_value_type_is_formal_enum() -> None:
    assert ValueType.INTEGER.value == "integer"
    assert "image" not in {m.value for m in ValueType}


@pytest.mark.unit
def test_binary_rejects_invalid_base64(engine: FlatStarShapeGeneralizer) -> None:
    assert _paths(engine.generalize({"x": "not!!valid!!base64!!"}))["x"] == ("string", "1")


@pytest.mark.unit
def test_cardinality_null_void_digit_strings(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(engine.generalize({"a": None, "empty": {}, "n": 1, "xs": [1, 2]}))
    assert paths["a"] == ("null", None)
    assert paths["empty"] == ("object", "void")
    assert paths["n"] == ("integer", "1")
    assert paths["xs"] == ("array", "2")
    dumped = engine.generalize({"xs": [1, 2]}).model_dump()
    assert dumped["fields"][0]["cardinality"] == "2"
    assert isinstance(dumped["fields"][0]["cardinality"], str)


@pytest.mark.unit
def test_cardinality_resolver_round_trip() -> None:
    assert to_shape_cardinality(LegacyCardinality.OPTIONAL) == "0"
    assert to_shape_cardinality(LegacyCardinality.ONE) == "1"
    assert to_shape_cardinality(LegacyCardinality.MANY) == "n"
    assert to_shape_cardinality(LegacyCardinality.VOID) == "void"
    assert format_count(2) == "2"
    assert from_shape_cardinality(None) is LegacyCardinality.OPTIONAL
    assert from_shape_cardinality("0") is LegacyCardinality.OPTIONAL
    assert from_shape_cardinality("1") is LegacyCardinality.ONE
    assert from_shape_cardinality("n") is LegacyCardinality.MANY
    assert from_shape_cardinality("void") is LegacyCardinality.VOID
    assert from_shape_cardinality("5") is LegacyCardinality.MANY
