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
    assert paths["lines[*].qty"] == ("number", "one")
    assert paths["lines[*].sku"] == ("string", "one")
    assert "lines[0]" not in paths
    assert "lines[1]" not in paths
    assert "lines[*]" not in paths  # non-empty object slot: children only


@pytest.mark.unit
def test_mixed_scalars_and_object(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(engine.generalize({"events": [1, "a", {}]}))
    assert paths["events"] == ("array", "many")
    assert paths["events[*]#0"] == ("number", "one")
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
    assert paths["events[*]#0.n"] == ("number", "one")
    assert paths["events[*]#0.type"] == ("string", "one")
    assert paths["events[*]#1.msg"] == ("string", "one")
    assert paths["events[*]#1.type"] == ("string", "one")


@pytest.mark.unit
def test_array_of_arrays_homogeneous(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(engine.generalize({"matrix": [[1, 2], [3, 4]]}))
    assert paths["matrix"] == ("array", "many")
    assert paths["matrix[*]"] == ("array", "many")
    assert paths["matrix[*][*]"] == ("number", "one")


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
    """Length must not force #variant — [[1],[1,2,3]] is still matrix[*][*] number."""
    paths = _paths(engine.generalize({"matrix": [[1], [1, 2, 3]]}))
    assert paths["matrix"] == ("array", "many")
    assert paths["matrix[*]"] == ("array", "many")
    assert paths["matrix[*][*]"] == ("number", "one")
    assert not any("#" in p for p in paths)
