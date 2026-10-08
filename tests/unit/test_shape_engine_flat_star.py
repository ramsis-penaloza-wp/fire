"""Shape engine v0: flat paths + [*] / #variant (no fingerprint)."""

from __future__ import annotations

import pytest

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.core.domain.dossier import ValueType
from fire.core.ports.shape_generalizer import ShapeGeneralizer


def _paths(shape) -> dict[str, tuple[str, object]]:
    return {field.path: (field.value_type.value, field.cardinality) for field in shape.fields}


def _leaf_type(engine: FlatStarShapeGeneralizer, value: object) -> str:
    [field] = engine.generalize({"x": value}).fields
    return field.value_type.value


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
def test_leaf_types(engine: FlatStarShapeGeneralizer) -> None:
    assert _leaf_type(engine, None) == "null"
    assert _leaf_type(engine, True) == "boolean"
    assert _leaf_type(engine, 0) == "integer"
    assert _leaf_type(engine, 10**30) == "integer"
    assert _leaf_type(engine, 1.0) == "number"
    assert _leaf_type(engine, "hello") == "string"
    assert _leaf_type(engine, {1, 2}) == "unknown"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("550e8400-e29b-41d4-a716-446655440000", "uuid"),
        ("550E8400-E29B-41D4-A716-446655440000", "uuid"),
        ("user@example.com", "email"),
        ("User.Name+tag@Example.COM", "email"),
        ("https://example.com/x", "uri"),
        ("http://localhost:8080/health", "uri"),
        ("file:///etc/hosts", "uri"),
        ("2024-01-15", "date"),
        ("0999-12-31", "date"),
        ("2024-01-15T12:00", "datetime"),
        ("2024-01-15 12:00", "datetime"),
        ("2024-01-15T12:00:00", "datetime"),
        ("2024-01-15T12:00:00Z", "datetime"),
        ("2024-01-15T12:00Z", "datetime"),
        ("2024-01-15T12:00:00.5", "datetime"),
        ("2024-01-15T12:00:00.123456789Z", "datetime"),
        ("2024-01-15T12:00:00+05:30", "datetime"),
        ("2024-01-15 12:00-08:00", "datetime"),
    ],
)
def test_strict_string_forms(engine: FlatStarShapeGeneralizer, value: str, expected: str) -> None:
    assert _leaf_type(engine, value) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    "value",
    [
        # numbers and words that lax parsers read as dates or datetimes
        "12345",
        "1700000000",
        "2024",
        "1.5",
        "0",
        # SKUs and hex whose length is a multiple of 4
        "WP1234567890ABCD",
        "deadbeefdeadbeef",
        "WP1234567890ABC",
        # scheme-like text without "://"
        "orders:create",
        "localhost:8080",
        "foo:bar",
        "mailto:a@b.com",
        "urn:isbn:0451450523",
        # URLs with whitespace or text around them
        " https://example.com",
        "https://example.com/a b",
        "see https://example.com",
        # non-canonical uuids
        "550e8400e29b41d4a716446655440000",
        "{550e8400-e29b-41d4-a716-446655440000}",
        "urn:uuid:550e8400-e29b-41d4-a716-446655440000",
        "+50e8400-e29b-41d4-a716-446655440000",
        "5_0e8400-e29b-41d4-a716-446655440000",
        "550e8400-e29b41d4-a716-4466554400-00",
        # email with a display name, brackets, or padding
        "Name <addr@x.com>",
        "<addr@x.com>",
        " user@example.com",
        "user@example.com ",
        # dates outside the extended YYYY-MM-DD form
        "20240115",
        "2024-W03-1",
        "2024-015",
        "2024-1-15",
        "2024-02-30",
        # datetimes outside the extended form
        "20240115T120000",
        "2024-01-15T12",
        "2024-01-15T1200",
        "2024-01-15x12:00",
        "2024-01-15t12:00",
        "2024-01-15T12:00z",
        "2024-01-15T12:00:00,5",
        "2024-01-15T12:00.5",
        "2024-01-15T12:00:00+0530",
        "2024-01-15T12:00:00+05",
        "2024-01-15T12:00:00+05:30:00",
        "2024-01-15T12:00:00.5+0530",
        "2024-01-15T23:59:60",
        "",
    ],
)
def test_lookalike_strings_are_strings(engine: FlatStarShapeGeneralizer, value: str) -> None:
    assert _leaf_type(engine, value) == "string"


@pytest.mark.unit
def test_numeric_ids_and_skus_are_strings_at_every_length(
    engine: FlatStarShapeGeneralizer,
) -> None:
    digits = "1234567890" * 4
    sku = "WP1234567890ABCDEFGHJKLMNPQRSTUV"
    samples = [digits[:length] for length in range(1, 41)] + [
        sku[:length] for length in range(1, len(sku) + 1)
    ]
    shapes = [engine.generalize({"id": value}) for value in samples]
    assert all(shape == shapes[0] for shape in shapes)
    assert _paths(shapes[0]) == {"id": ("string", "1")}


@pytest.mark.unit
def test_base64_text_is_a_string(engine: FlatStarShapeGeneralizer) -> None:
    assert _leaf_type(engine, "SGVsbG8gV29ybGQhISE=") == "string"
    assert _leaf_type(engine, "not!!valid!!base64!!") == "string"


@pytest.mark.unit
def test_value_type_is_formal_enum() -> None:
    values = {member.value for member in ValueType}
    assert ValueType.INTEGER.value == "integer"
    assert "image" not in values
    assert "binary" not in values


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
