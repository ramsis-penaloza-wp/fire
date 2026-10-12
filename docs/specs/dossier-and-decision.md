# Dossier and Decision contract

What `fire/core/domain/dossier.py`, `fire/core/domain/decision.py`, and
`fire/core/domain/incident.py` guarantee to the reasoner, the narrator, and the incident engine,
how `POST /v0/diagnose` builds an incident and its dossier, and what `EchoReasoner` decides. The
shape models (`ValueType`, `ShapeField`, `Shape`, `ShapeFingerprint`) are out of scope here.

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

`via` (`HopVia`) says how an edge was traversed: `child`, a parent-child span edge, or `link`, a
span link. It is optional and set on edge hops only. Like the context fields, it never
identifies the hop and `display_label()` ignores it.
[Process path in the dossier](flows-and-paths.md#process-path-in-the-dossier) says which flow
edges set it; step 1.9 delivered that mapping, and path drift dossiers carry it (step 2.3).

Construction raises `ValidationError` when:

- no ref is set;
- `ref` is set together with `from_ref` or `to_ref` (a hop cannot be both a node and an
  edge);
- `via` is set on a node hop.

The same shape is the wire format of `process_hops` in `POST /v0/diagnose`:

| JSON hop | Result |
|----------|--------|
| `{"ref": "checkout", "operation": "POST /orders"}` | node, label `checkout/POST /orders` |
| `{"from_ref": "orig-orders", "to_ref": "orig-payments"}` | edge, label `orig-orders->orig-payments` |
| `{"from_ref": "orig-orders", "to_ref": "orig-payments", "via": "link"}` | edge across a span link, label `orig-orders->orig-payments` |
| `{"to_ref": "payments"}` | edge, label `payments` |
| `{"ref": "checkout", "from_ref": "orig-orders"}` | `422`: both forms |
| `{"operation": "POST /orders"}` | `422`: no ref |
| `{"ref": "checkout", "via": "child"}` | `422`: `via` on a node |
| `{"to_ref": "payments", "via": "parent"}` | `422`: not a `HopVia` |

There is no `service` or `origin_id` field on a hop; use `ref`.

## Dossier

The evidence bundle the reasoner and narrator read. Each fact has exactly one field:

| Field | Holds |
|-------|-------|
| `incident_id`, `system`, `service` | Incident identity: its `id`, its system, and its service (see [Follow-ups](#follow-ups)) |
| `origin` | `Origin`, or `None` when not provided |
| `shape` | `Shape`, or `None` |
| `fingerprint` | `ShapeFingerprint`, or `None` |
| `process_path` | Ordered `ProcessHop`s |
| `fragments` | Every `EvidenceFragment`, present or missing |

No field restates another.

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
    system="omg",
    fragments=[
        EvidenceFragment(collector="shape", quality=EvidenceQuality.PRESENT, summary="ok"),
        EvidenceFragment(
            collector="origin", quality=EvidenceQuality.MISSING, summary="No Origin provided"
        ),
    ],
)
[f.collector for f in dossier.missing]  # ["origin"]
```

Whoever opens an incident declares the missing fragments of its dossier: `POST /v0/diagnose`
those in [Diagnose endpoint](#diagnose-endpoint), and drift incidents their own
([Drift incidents](ingest-drift-and-feedback.md#drift-incidents)).

## Incident

`Incident` (`fire/core/domain/incident.py`) is one incident of one loaded system. It has no
payload fields. `DiagnosisService.run` receives it with its dossier, and the notifier reads it.

| Field | Holds |
|-------|-------|
| `id` | The incident id, which the dossier carries as `incident_id` |
| `system` | The loaded system the incident belongs to |
| `trigger` | What opened it, a `Trigger`; required |
| `service` | A service name, or `None` |
| `summary` | A one-line summary, or `None` |
| `started_at` | When it started; defaults to the current UTC time |

`Trigger` is a `StrEnum`; on the wire it is the lowercase value. Any other value raises
`ValidationError`.

| `Trigger` | Opened by | Step |
|-----------|-----------|------|
| `shape_drift` | A new shape drift signal ([Drift incidents](ingest-drift-and-feedback.md#drift-incidents)) | 2.4 |
| `path_drift` | A flow that fires a new path drift signal ([Path drift](flows-and-paths.md#path-drift)) | 2.3 |
| `incident_io` | An incident.io incident | 3.4 |
| `http_errors` | A burst of 5xx responses from one endpoint | 3.3 |
| `manual` | `POST /v0/diagnose` ([Diagnose endpoint](#diagnose-endpoint)) | 2.4 |

The step is the one that opens incidents with that trigger; every value exists since step 2.4.

## Diagnose endpoint

`POST /v0/diagnose` opens a `manual` incident from what a caller posts, answers `202`, and runs
the diagnosis (reasoner, then narrator, then notifier) in a background task. It requires
`X-FIRE-Token`: missing or wrong is `401`, and an unset `api.token_env` is `503`.

| Field | Required | Holds |
|-------|----------|-------|
| `system` | yes | A loaded system |
| `service`, `summary` | no | Copied to the incident; `service` to the dossier too |
| `origin` | no | `Origin` |
| `shape` | no | `Shape` |
| `fingerprint` | no | `ShapeFingerprint` |
| `process_hops` | no | `ProcessHop`s ([ProcessHop](#processhop)); default `[]` |

A body that does not validate as these fields is `422`, with each error's `loc` and `type`;
unlike on `/v0/observations`, this runs before the token check. Then the token is checked
(`401`, `503`), and then the first failing check below decides:

| Check | `422` detail |
|-------|--------------|
| `system` is a loaded system | `{"detail": [{"loc": ["body", "system"], "type": "unknown_system"}]}` |
| With a `shape`, `fingerprint` is absent or equals the recomputed one | `{"detail": [{"loc": ["body", "fingerprint"], "type": "fingerprint_mismatch"}]}` |

When the request carries a `shape`, the engine recomputes its fingerprint with the
`ShapeFingerprinter` port (SHA-256 of the fields in canonical order) and compares the whole
`ShapeFingerprint`: `schema_hash`, `normalized_paths`, and `notes`, as the ingest endpoint does
([Ingest endpoint](observations.md#ingest-endpoint)). A matching or absent client `fingerprint`
gives the dossier the computed one, so no `fingerprint` fragment is declared. A `fingerprint`
sent without a `shape` is kept as is: there is nothing to recompute it from.

The engine declares these missing fragments:

| Collector | When | Summary |
|-----------|------|---------|
| `origin` | no `origin` | `No Origin provided` |
| `shape` | neither `shape` nor `fingerprint` | `No shape or fingerprint provided` |

The response returns before the diagnosis runs. `incident_id` is a new uuid4; `service` and
`origin_id` are `null` when not sent:

```json
{"status": "accepted", "incident_id": "<uuid>", "system": "omg", "service": "checkout",
 "origin_id": "orig-orders"}
```

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
`noul` is `True`: when any fragment is missing or the dossier has no evidence
([EchoReasoner](#echoreasoner)).

## EchoReasoner

`EchoReasoner` (`fire/adapters/reasoners/echo.py`) is the local stub reasoner: deterministic,
not a generative model, and it reads only the dossier. Its possibilities, in this order:

| `id` | Added | `label` | `rationale_refs` |
|------|-------|---------|------------------|
| `hop-{i}` | One per process-path hop | `Failure near <display_label()>` | `["process_path:<display_label()>"]` |
| `drift-{i}` | One per `present` `shape_drift` or `path_drift` fragment, in fragment order | From the fragment, below | `[<collector>]` |
| `insufficient-evidence` | When any fragment is `missing` | `Insufficient evidence (MISSING fragments)` | The missing fragments' collectors, in fragment order |
| `unknown` | When none of the above was added | `No process path or drift evidence yet` when the dossier has a `shape` or `fingerprint`, else `No process path, shape, or drift evidence yet` | `[]` |

`{i}` counts from `0` within its kind. Probabilities split evenly, unrounded: each is `1 / n`
for `n` possibilities.

| Fragment | `label` |
|----------|---------|
| `shape_drift` | `Shape drift on <stream>: <changes>`, the changes joined by `; ` in this order: `<path> (<value_type>) added`, `<path> changed <old> -> <new>` with each type list joined by `\|`, and `<path> (<value_type>) removed` |
| `path_drift` | `<rule>: <node operation> on <entry operation>`, from `rule`, `node.operation`, and `entry.operation` ([Path drift](flows-and-paths.md#path-drift)) |
| Either, with malformed `data` | The fragment's `summary` |

`shape_drift` data is malformed when it lacks `stream` or `diff`, when `diff` is not a valid
`ShapeDiff`, or when the diff is empty; `path_drift` data when it lacks `rule`,
`entry.operation`, or `node.operation`.

A dossier has evidence when it has a `shape`, a `fingerprint`, or a `present` drift fragment.
`confidence` is `0.2` when any fragment is missing or there is no evidence, else `0.45`. `noul`
is `True` when `confidence` is below `0.3` or there is no evidence; `primitive` is `noul`
exactly when `noul` is `True`, else `choice`.

In the L1 shape drift scenario with no manifest
([Drift incidents](ingest-drift-and-feedback.md#drift-incidents)), the dossier has a present
`shape_drift` fragment and missing `coverage` and `process_path` fragments: `drift-0` and
`insufficient-evidence` at `0.5` each, `confidence` `0.2`, and `noul`.

In the L1 path drift scenario, the recorded place-order flow without its Helios call
([Path drift incidents](flows-and-paths.md#path-drift-incidents)), the dossier has 28 hops and one
present `path_drift` fragment, and no missing one: `hop-0` to `hop-27` and `drift-0` at `1/29`
each, `confidence` `0.45`, and `choice`.

## Follow-ups

- `Dossier.incident_id`, `system`, and `service` duplicate the `Incident` that
  `DiagnosisService.run` receives alongside the dossier.
- On a long process path, the `hop-{i}` possibilities outnumber the drift ones: 28 hops leave a
  path drift possibility at 3% ([flows-and-paths.md, open question 8](flows-and-paths.md#open-questions)).
