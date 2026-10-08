# Dossier and Decision contract

What `fire/core/domain/dossier.py` and `fire/core/domain/decision.py` guarantee to the
reasoner, the narrator, and the incident engine. The shape models (`ValueType`,
`ShapeField`, `Shape`, `ShapeFingerprint`) are out of scope here.

## ProcessHop

A hop names one step of the process path by reference. It never embeds a `Shape` or
payload values.

Every ref (`ref`, `from_ref`, `to_ref`) has one meaning: an `origin_id` or a service name.
Refs are non-empty strings.

A hop has exactly one identity form:

| Form | Fields set | `display_label()` |
|------|------------|-------------------|
| Node | `ref` | `ref` |
| Edge | `from_ref` and `to_ref` | `from_ref->to_ref` |
| Edge, one end known | `from_ref` or `to_ref` | the ref that is set |

`display_label()` appends `/operation` when `operation` is set. `operation`, `span_kind`,
and `status` are optional context and never identify the hop.

Construction raises `ValidationError` when:

- no ref is set;
- `ref` is set together with `from_ref` or `to_ref` (a hop cannot be both a node and an
  edge).

The same shape is the wire format of `process_hops` in `POST /v0/diagnose`:

| JSON hop | Result |
|----------|--------|
| `{"ref": "checkout", "operation": "POST /orders"}` | node, label `checkout/POST /orders` |
| `{"from_ref": "orig-orders", "to_ref": "orig-payments"}` | edge, label `orig-orders->orig-payments` |
| `{"to_ref": "payments"}` | edge, label `payments` |
| `{"ref": "checkout", "from_ref": "orig-orders"}` | `422`: both forms |
| `{"operation": "POST /orders"}` | `422`: no ref |

There is no `service` or `origin_id` field on a hop; use `ref`.

## Dossier

The evidence bundle the reasoner and narrator read. Each fact has exactly one field:

| Field | Holds |
|-------|-------|
| `incident_id`, `project_id`, `service` | Incident identity (see [Follow-ups](#follow-ups)) |
| `origin` | `Origin`, or `None` when not provided |
| `shape` | `Shape`, or `None` |
| `fingerprint` | `ShapeFingerprint`, or `None` |
| `process_path` | Ordered `ProcessHop`s |
| `fragments` | Every `EvidenceFragment`, present or missing |

No field restates another. The dossier does not carry a `ShapeRecord`: its `project_id`,
`origin_id`, `shape`, and `fingerprint` are already on the dossier. `ShapeRecord` remains a
domain model for the `ShapeCatalog` port.

`Dossier` rejects unknown fields, so a caller passing a removed field fails instead of
silently losing data.

### Evidence

Evidence lives in one list, `fragments`. Each `EvidenceFragment` has a `collector`, a
`quality` (`present` or `missing`), a `summary`, and shape-safe `data` (never raw payload
values). Missing evidence is declared as a fragment with quality `missing`; it is never
invented and never kept in a second list.

`missing` is a read-only property: the fragments whose quality is `missing`, in `fragments`
order. It is not a field, so the constructor does not accept it and `model_dump()` does not
include it.

```python
dossier = Dossier(
    incident_id="inc-1",
    project_id="proj-a",
    fragments=[
        EvidenceFragment(collector="shape", quality=EvidenceQuality.PRESENT, summary="ok"),
        EvidenceFragment(
            collector="origin", quality=EvidenceQuality.MISSING, summary="No Origin provided"
        ),
    ],
)
[f.collector for f in dossier.missing]  # ["origin"]
```

The incident engine declares these missing fragments:

| Collector | When | Summary |
|-----------|------|---------|
| `origin` | no `origin` | `No Origin provided` |
| `shape` | neither `shape` nor `fingerprint` | `No shape or fingerprint provided` |
| `fingerprint` | `shape` without `fingerprint` | `Shape present without a fingerprint` |

## Decision

The reasoner's output. The narrator formats it and never changes it.

| Field | Contract |
|-------|----------|
| `incident_id` | Incident the decision is about |
| `possibilities` | `Possibility` list; each `probability` is in `[0, 1]` |
| `confidence` | In `[0, 1]`, default `0.0` |
| `noul` | Gate: when `True`, do not narrate the decision as a settled cause |
| `primitive` | `Primitive` enum: `choice` (default), `score`, or `noul` |

`primitive` is a `StrEnum`; on the wire it is the lowercase value. Any other value raises
`ValidationError`.

### Distribution rule

When `possibilities` is non-empty, their probabilities sum to 1 within an absolute tolerance
of `1e-6` (`math.isclose(total, 1.0, abs_tol=1e-6)`). An empty list is valid: no hypothesis
yet. The rule is checked at construction; models are not re-validated on assignment.

Probabilities are unrounded floats. Rounding is presentation, done by the narrator.

| Probabilities | Result |
|---------------|--------|
| `[1/3, 1/3, 1/3]` | valid |
| `[]` | valid |
| `[0.3333, 0.3333, 0.3333]` | rejected: sum `0.9999` |
| `[0.25, 0.25]` | rejected: sum `0.5` |

### `noul` and `primitive`

The model does not couple them. `EchoReasoner` sets `primitive=Primitive.NOUL` exactly when
`noul` is `True`: when any fragment is missing or the dossier has neither `shape` nor
`fingerprint`.

## Follow-ups

- `Dossier.incident_id`, `project_id`, and `service` duplicate the `Incident` that
  `DiagnosisService.run` receives alongside the dossier.
