# Shape generalizer

This is the contract of `FlatStarShapeGeneralizer` (`fire/adapters/shapes/flat_star.py`), the
adapter behind the `ShapeGeneralizer` port (`fire/core/ports/shape_generalizer.py`). It turns a
JSON payload into a `Shape` (`fire/core/domain/dossier.py`).

## Goal

A shape describes the form of a payload: its paths, value types, cardinality, and which keys
may be absent. It never holds a value. Payloads of the same form produce the same `Shape`,
whatever their values, array lengths, array element order, or key order.

## Output

`Shape.fields` is a list of `ShapeField`:

| Attribute | Type | Meaning |
|-----------|------|---------|
| `path` | `str` | Where the field sits, see [Paths](#paths). |
| `value_type` | `ValueType` | What kind of value sits there, see [Value types](#value-types). |
| `cardinality` | `str \| None` | How many, see [Cardinality](#cardinality). |
| `optional` | `bool` | Whether the key may be absent, see [Optional keys](#optional-keys). |

One path can carry several fields, one per value type seen there. The list is sorted by
`(path, value_type)` in code-point order and never holds two fields with the same
`(path, value_type)`.

`Shape`, `ShapeField`, and `ShapeFingerprint` reject unknown fields, so a shape that arrives
with a `value` key fails validation instead of losing the key silently.

Serialized, a field looks like this:

```json
{"path": "lines[*].gift", "value_type": "boolean", "cardinality": "1", "optional": true}
```

## Paths

- Object keys join with `.`: `order.customer.email`.
- Array elements append `[*]`: `lines[*].sku`, `matrix[*][*]`. A path never holds an index
  (`lines[0]`) or a variant suffix.
- The root path is the empty string `""`.
  - A root object emits no field of its own. Its keys start the paths: `{"order_id": …}`
    gives `order_id`.
  - A root array emits one `array` field at `""`. Its elements sit at `[*]`.
  - A root scalar emits one field at `""`.
- Keys are not escaped. A key that contains `.` or `[*]` can land on the same path as a nested
  key (`{"a.b": 1}` and `{"a": {"b": 1}}` both give `a.b`). Fields that collide this way
  merge, see [Merging](#merging).

## Samples

The generalizer reasons over *samples*: every value seen at a path.

- The payload is the only sample at the root.
- Every element of every array sample at `p` is a sample at `p[*]`. Elements of all arrays at
  one path pool together, so arrays nested in arrays merge too.
- For every key of the object samples at `p`, the values under that key are the samples at
  `p.key`.

At each path the generalizer emits one field per distinct value type among the samples. The
object samples at a path merge into one template whose keys are the union of their keys.

## Value types

| `value_type` | JSON value |
|--------------|------------|
| `object` | An object. |
| `array` | An array. |
| `null` | `null`. |
| `boolean` | `true` or `false`. |
| `integer` | A number with no fraction and no exponent (`json` decodes it to `int`), of any size. |
| `number` | Any other number. |
| `uuid`, `email`, `uri`, `date`, `datetime` | A string in one exact form, see below. |
| `string` | Any other string. |
| `unknown` | A value JSON cannot produce (for example a Python `set` passed in directly). |

### String forms

A string gets a refined type only when it matches one exact form. The structural check
comes first, then a standard parser validates it. Anything else is `string`, so a field keeps
its type whatever its value.

| Type | Accepted form | Examples that stay `string` |
|------|---------------|-----------------------------|
| `uuid` | Canonical hyphenated 8-4-4-4-12 hex (36 characters, either case) that `uuid.UUID` parses. | `550e8400e29b41d4a716446655440000`, `{550e8400-…}`, `urn:uuid:550e8400-…`, `+50e8400-…` |
| `email` | A plain address that Pydantic `EmailStr` accepts, with no `<` and no whitespace. | `Name <addr@x.com>`, `<addr@x.com>`, ` user@example.com` |
| `uri` | Contains `://`, no whitespace, and parses as Pydantic `AnyUrl`. | `orders:create`, `localhost:8080`, `foo:bar`, `mailto:a@b.com`, ` https://x.com` |
| `date` | ISO 8601 extended `YYYY-MM-DD` (10 characters, `-` at index 4 and 7) that `date.fromisoformat` parses. | `20240115`, `2024-W03-1`, `2024-02-30`, `2024`, `0` |
| `datetime` | `YYYY-MM-DD`, then `T` or one space, then `HH:MM`, optional `:SS`, optional `.` and one or more digits (only after seconds), optional `Z`, `+HH:MM`, or `-HH:MM`; `datetime.fromisoformat` must parse it. | `12345`, `1700000000`, `1.5`, `20240115T120000`, `2024-01-15T12`, `2024-01-15T1200`, `2024-01-15t12:00`, `2024-01-15T12:00:00+0530`, `2024-01-15T12:00:00,5` |

The parsers also check ranges, so an impossible calendar value such as `2024-02-30` or a leap
second `23:59:60` is `string`.

There is no other refinement. In particular, base64-looking text, hex, numeric ids, and SKUs
are `string` at every length.

## Cardinality

| `value_type` | `cardinality` |
|--------------|---------------|
| `object` | `"void"` when every object sample at the path is empty, otherwise `"1"`. |
| `array` | `"0"` when every array sample at the path is empty, otherwise `"n"`. Never the observed length. |
| `null` | `null` |
| Any other type | `"1"` |

`ShapeField` enforces the vocabulary: `null`, `"void"`, `"n"`, or a non-negative integer
written in decimal without leading zeros (`"0"`, `"1"`, `"42"`). It rejects anything else,
including `"many"`, `"-1"`, `"2.5"`, `"01"`, and a JSON number. The generalizer emits only
`null`, `"void"`, `"0"`, `"1"`, and `"n"`; the other digit strings stay valid for shapes that
other producers build.

## Optional keys

A key is optional at `p.key` when at least one object sample at `p` lacks it. Every field at
`p.key`, whatever its value type, then carries `optional: true`. All other fields carry
`optional: false`.

Optionality is relative to the parent object. The fields under an optional key are judged
against the samples of that key alone, so `p.key.child` is optional only when some object at
`p.key` lacks `child`. Whether a path may be absent from the whole payload follows by walking
up its ancestors. `[*]` slots and the root are never optional: they are not keys.

Because an empty object sample lacks every key, the keys of an object that is sometimes empty
are all optional.

## Merging

Fields merge in two places.

- **Object samples at one path** merge into one template, as described in
  [Samples](#samples).
- **Colliding paths** (see [Paths](#paths)) merge into one field per `(path, value_type)`. The
  merged field is optional only when every colliding field is, and its cardinality is the
  populated one (`"1"` or `"n"`) when any colliding field is populated.

## Determinism and order independence

- The `Shape` is identical under any permutation of array elements and any key order, at
  every depth.
- Fields are sorted by `(path, value_type)`.
- The generalizer reads the payload and never mutates it.

## Examples

### Order with an optional key on one line

```json
{
  "order_id": "A-1001",
  "lines": [
    {"sku": "WP1234567890ABCD", "qty": 1},
    {"sku": "WP1", "qty": 2, "gift": true}
  ]
}
```

| path | value_type | cardinality | optional |
|------|------------|-------------|----------|
| `lines` | `array` | `"n"` | false |
| `lines[*]` | `object` | `"1"` | false |
| `lines[*].gift` | `boolean` | `"1"` | true |
| `lines[*].qty` | `integer` | `"1"` | false |
| `lines[*].sku` | `string` | `"1"` | false |
| `order_id` | `string` | `"1"` | false |

Swapping the two lines, adding a third line `{"sku": "X", "qty": 3}`, or reordering the keys
gives the same fields.

### Different kinds in one array

`{"xs": [1, "a", {}]}`

| path | value_type | cardinality | optional |
|------|------------|-------------|----------|
| `xs` | `array` | `"n"` | false |
| `xs[*]` | `integer` | `"1"` | false |
| `xs[*]` | `object` | `"void"` | false |
| `xs[*]` | `string` | `"1"` | false |

### Nested arrays pool their elements

`{"matrix": [[1, 2], [], [3]]}`

| path | value_type | cardinality | optional |
|------|------------|-------------|----------|
| `matrix` | `array` | `"n"` | false |
| `matrix[*]` | `array` | `"n"` | false |
| `matrix[*][*]` | `integer` | `"1"` | false |

`{"matrix": [[], []]}` gives `matrix` `array` `"n"` and `matrix[*]` `array` `"0"`, with no
`matrix[*][*]` field.

### Null and string under one key, in a root array

`[{"a": null}, {"a": "x"}, {}]`

| path | value_type | cardinality | optional |
|------|------------|-------------|----------|
| `""` | `array` | `"n"` | false |
| `[*]` | `object` | `"1"` | false |
| `[*].a` | `null` | `null` | true |
| `[*].a` | `string` | `"1"` | true |

### Optionality is relative to the parent

`{"xs": [{"addr": {"zip": "10001"}}, {}]}`

| path | value_type | cardinality | optional |
|------|------------|-------------|----------|
| `xs` | `array` | `"n"` | false |
| `xs[*]` | `object` | `"1"` | false |
| `xs[*].addr` | `object` | `"1"` | true |
| `xs[*].addr.zip` | `string` | `"1"` | false |
