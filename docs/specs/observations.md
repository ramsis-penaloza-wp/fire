# Observations

What the tentacle sends the body and what the body keeps: the models in
`fire/core/domain/observation.py` (`Channel`, `ShapeObservation`, `SpanObservation`), the
ingest endpoint `POST /v0/observations`, the `ObservationCatalog` port, and
`GET /v0/systems/{system}/streams`. Sources are in [tentacle.md](tentacle.md), span and HTTP
capture in [http-and-span-capture.md](http-and-span-capture.md), flows and paths in
[flows-and-paths.md](flows-and-paths.md), and baselines and drift in
[ingest-drift-and-feedback.md](ingest-drift-and-feedback.md#phase-2-drift).

**Status:** the contract of the shipped code. [Channel](#channel),
[ShapeObservation](#shapeobservation), [SpanObservation](#spanobservation),
[Stream key and deduplication](#stream-key-and-deduplication), and the model rules of
[Privacy invariants](#privacy-invariants) are `fire/core/domain/observation.py` (roadmap step
1.1). The [ingest endpoint](#ingest-endpoint), the [catalog](#observationcatalog), and the
[streams endpoint](#streams-endpoint) are step 1.2: `fire/core/services/ingest.py`,
`fire/core/ports/observation_catalog.py`, `fire/adapters/memory/observation_catalog.py`, and
`fire/apps/incident_engine/app.py`. Step 2.1 added the baseline fold to the catalog and each
stream's `status` and `samples` (`fire/core/domain/baseline.py`); the baseline's contract is
[Baseline](ingest-drift-and-feedback.md#baseline). Step 2.2 added shape drift: `add_shape`
returns a `ShapeAdmission`, `IngestService` calls `on_drift` once per new signal, and each
stream lists its open signals (`fire/core/domain/drift.py`); the contract is
[Drift rules](ingest-drift-and-feedback.md#drift-rules). Step 2.4 moved `on_drift` to
`IngestService.ingest`, passed per call, and the ingest endpoint's hook opens a drift incident
([Drift incidents](ingest-drift-and-feedback.md#drift-incidents)).

## Channel

Where a payload was seen. Unknown fields are rejected.

| Field | Values | Meaning |
|-------|--------|---------|
| `kind` | `http`, `event` | Transport (`ChannelKind`) |
| `direction` | `inbound`, `internal`, `outbound` | Relative to the watched system (`ChannelDirection`) |
| `name` | `str` | Route template, outbound operation name ([Operation names](http-and-span-capture.md#operation-names)), or event type |
| `part` | `request`, `response`, `payload` | `http` uses `request` and `response`; `event` uses `payload` (`ChannelPart`) |
| `status_class` | `2xx`, `3xx`, `4xx`, `5xx`, `none` | Required on a response, forbidden otherwise; `none` means no response came (`StatusClass`) |

A `part` that does not fit `kind` fails at `channel.part`; a `status_class` that is missing on
a response, or set on anything else, fails at `channel.status_class`. When `part` itself
fails, `status_class` is not checked, so each mistake gives one error.

The `key` property renders `{kind}:{direction}:{name}:{part}`, plus `:{status_class}` for a
response. `name` may contain `:`; the key stays unambiguous because `kind` and `direction`
never contain `:` and every key ends in `:request`, `:payload`, or `:response:{status_class}`.
`key` is not a field, so `model_dump()` leaves it out and a dumped channel validates again.

| Channel | Key |
|---------|-----|
| Inbound response | `http:inbound:POST /v1/orders/place_order:response:2xx` |
| Outbound call to an aliased host, no response | `http:outbound:GET partner/api/{version}/orders/{order_id}:response:none` |
| Internal event | `event:internal:fulfillment.allocated:payload` |

An observation on a `none` channel carries an empty shape, `{"fields": []}`; any field there
fails at `shape`.

## ShapeObservation

One payload seen by the tentacle, as its shape. Unknown fields are rejected. Fields typed
`| None` default to `null` and may be absent; every other field is required.

| Field | Type | Meaning |
|-------|------|---------|
| `system` | `str` | Must be a loaded `config/systems/<system>.yml` |
| `source` | `str` | Must be in that system's `sources[].id` |
| `channel` | `Channel` | Where the payload was seen |
| `observation_id` | `str` | Unique within `(system, source)`, see [deduplication](#stream-key-and-deduplication) |
| `occurred_at` | `datetime` | The record's time for events, the exchange time for HTTP |
| `observed_at` | `datetime` | When the tentacle read it |
| `region` | `str \| None` | Region of the stream or service it was read from |
| `trace_id`, `span_id` | `str \| None` | Trace and span the payload belongs to; `span_id` requires `trace_id` |
| `work_id` | `str \| None` | Unit of work the payload belongs to; joins events, captures, and outcomes |
| `correlation_id` | `str \| None` | Set only when the source carries one |
| `shape` | `Shape` | From the `ShapeGeneralizer` port |
| `fingerprint` | `ShapeFingerprint` | From the `ShapeFingerprinter` port |

OMG maps its `work_unit_id` to `work_id`. Which envelope value fills each field is declared
per source in the system YAML ([Envelope](tentacle.md#envelope)).

A `span_id` without a `trace_id` fails at `span_id`.

## SpanObservation

One span, without its attributes. Unknown fields are rejected. `parent_span_id` and
`status_code` default to `null`, `links` to `[]`; every other field is required, `error`
included.

| Field | Type | Meaning |
|-------|------|---------|
| `system`, `source` | `str` | As on `ShapeObservation` |
| `trace_id` | `str` | 32 lowercase hex characters |
| `span_id`, `parent_span_id` | `str`, `str \| None` | 16 lowercase hex characters; no parent on a root span |
| `links` | list of `SpanLink` (`{trace_id, span_id}`) | Spans this one links to; a link rejects unknown fields |
| `service` | `str` | Service that emitted the span |
| `kind` | `server`, `client`, `producer`, `consumer`, `internal` | Span kind (`SpanKind`) |
| `operation` | `str` | Operation name ([Operation names](http-and-span-capture.md#operation-names)) |
| `status_code` | `int \| None` | HTTP status, `100` to `599`; `None` when not HTTP or no response |
| `error` | `bool` | The span ended in error |
| `start`, `duration_ms` | `datetime`, `float` | Span start; a finite duration of zero or more |

## Stream key and deduplication

The stream key of a shape observation is `(system, source, channel.key)`. Baselines, drift,
and coverage are per stream key, so two sources never share a baseline, even when they carry
the same events.

| Observation | Dedupe key | `observation_id` |
|-------------|------------|------------------|
| Shape from an event source | `(system, source, observation_id)` | The record's own id: one event read from two regions is one observation |
| Shape from a span body | `(system, source, observation_id)` | `{trace_id}:{span_id}:{part}` ([Bodies on client spans](http-and-span-capture.md#bodies-on-client-spans)) |
| Shape from the inbound proxy | `(system, source, observation_id)` | `{exchange_id}:{part}` ([Shapes](http-and-span-capture.md#shapes)) |
| Shape from an `s3_capture` pair | `(system, source, observation_id)` | `{pair id}:{part}` ([Kinds](tentacle.md#kinds)) |
| Span | `(system, trace_id, span_id)` | Not used |

One event carried by two sources is two observations; one span sent by two sources is one.

`ShapeObservation.stream_key` and `ShapeObservation.dedupe_key` return these tuples, and so
does `SpanObservation.dedupe_key`. Like `Channel.key`, they are properties, not fields.

## Privacy invariants

- **No open types.** No field is `Any`, a `dict`, or free JSON. The only field built from a
  payload is `shape`: paths, value types, cardinality, and optionality.
- **Unknown fields fail at every depth.** The observation models, `Channel`, `SpanLink`,
  `Shape`, `ShapeField`, and `ShapeFingerprint` all reject unknown fields (the last three on
  `POST /v0/diagnose` too), so a `payload`, `body`, `value`, or `attributes` key anywhere
  fails.
- **Bounded fields.** Every string field of the observation models and `Channel` is non-empty
  and at most 256 characters. Trace and span ids are fixed-length lowercase hex, not all
  zeros. Every `datetime` is timezone-aware.
- **Names and ids carry no payload data.** `channel.name`, `service`, and `operation` are
  templates or declared names; `observation_id`, `work_id`, and `correlation_id` come from
  envelopes, capture keys, or the tentacle. The body cannot check this, so the tentacle
  enforces it and the canary test proves it ([Canary harness](tentacle.md#canary-harness)).
- **The fingerprint adds nothing.** It must equal the recomputed one on every field, so
  `notes` cannot carry a value.
- **Nothing is echoed.** A rejection names an index, a reason, and field locations. A `422`
  on any route, `/v0/diagnose` included, lists each error's `loc` and `type`, never its
  `input`: `{"detail": [{"loc": ["body", "shapes"], "type": "list_type"}]}`. Ingest logs carry
  counts only.

## Ingest endpoint

`POST /v0/observations` with header `X-FIRE-Token`. Either list may be empty or absent.

```json
{"shapes": [{
  "system": "omg", "source": "domain-events", "observation_id": "01J9Z3K4N8Q2R5T7V9X1Y3Z5A7",
  "channel": {"kind": "event", "direction": "internal", "name": "fulfillment.allocated",
              "part": "payload"},
  "occurred_at": "2026-10-11T16:30:00Z", "observed_at": "2026-10-11T16:30:01Z",
  "shape": {"fields": [{"path": "order_id", "value_type": "string", "cardinality": "1",
                        "optional": false}]},
  "fingerprint": {"schema_hash": "3f1c…", "normalized_paths": ["order_id"]}}],
 "spans": []}
```

The token is checked before the body, with `/v0/diagnose`'s rules: missing or wrong is `401`,
an unset `api.token_env` is `503`. The body is read only after the token passes, so even
malformed JSON from a caller without the token is `401`. A body that is not an object holding
only `shapes` and `spans` lists is `422`. Otherwise the response is `202`, even when every item
is rejected.

Items are checked one by one, `shapes` first, each list in order. The first failing check
decides:

| Check | Outcome when it fails |
|-------|-----------------------|
| The item validates as its model | rejected, `invalid` |
| `system` is loaded | rejected, `unknown_system` |
| `source` is in that system's `sources[].id` | rejected, `unknown_source` |
| The recomputed fingerprint equals `fingerprint` (shapes only) | rejected, `fingerprint_mismatch` |
| The dedupe key is not stored yet | `duplicate` |

An item that passes every check is stored and counted as `accepted`, a drifted shape included
([Drift rules](ingest-drift-and-feedback.md#drift-rules)). The body recomputes the fingerprint
with its `ShapeFingerprinter` (`Sha256ShapeFingerprinter`) and compares `schema_hash`,
`normalized_paths`, and `notes`. A rejected item is not stored and holds no dedupe key. A
duplicate changes nothing, even when its shape differs: the first write wins. Two equal items
in one batch give one `accepted` and one `duplicate`.

`IngestService` (`fire/core/services/ingest.py`) runs the checks against the loaded systems'
`sources[].id`; the response models (`IngestReport`, `IngestCounts`, `Rejection`,
`RejectionReason`) are in `fire/core/domain/ingest.py`. `ingest(shapes, spans, on_drift=None)`
calls its optional hook once per drift signal the batch raised, after the whole batch is
counted, in batch order. Each batch logs one `INFO` line with the six counts and nothing else.
For each new drift signal, the endpoint's hook logs one more `INFO` line with `log_drift`, with
names and counts only ([Drift rules](ingest-drift-and-feedback.md#drift-rules)), and opens a
drift incident whose diagnosis runs in a background task after the response
([Drift incidents](ingest-drift-and-feedback.md#drift-incidents)). Drift does not change the
response format.

```json
{"shapes": {"accepted": 41, "duplicate": 2,
            "rejected": [{"index": 7, "reason": "unknown_source"},
                         {"index": 9, "reason": "invalid", "fields": ["channel.status_class"]}]},
 "spans": {"accepted": 120, "duplicate": 0, "rejected": []}}
```

`index` is the position in the item's own list; `fields`, only on `invalid`, lists dotted
locations, each once. An item that is not an object fails as a whole, so its `fields` is empty.
Accepted items are in the catalog when the response returns, so the counts are final.

## ObservationCatalog

Port in `fire/core/ports/observation_catalog.py`; the first adapter is in memory, in
`fire/adapters/memory/`.

| Operation | Contract |
|-----------|----------|
| `add_shape(observation) -> ShapeAdmission` | Stores the observation unless its dedupe key is known. A stored shape is admitted to the baseline of its stream key in the same atomic step, under the system's drift policy. Returns `duplicate`, `folded`, `drift_raised`, or `drift_repeated`, with the signal on a drift outcome ([Drift rules](ingest-drift-and-feedback.md#drift-rules)) |
| `add_span(observation) -> bool` | Stores the span unless `(system, trace_id, span_id)` is known; returns whether it did |
| `streams(system) -> list[StreamSummary]` | One summary per stream key of the system, with its baseline's `status`, `samples`, and open drift signals, sorted as in [Streams endpoint](#streams-endpoint); empty for a system with no observations |

The operations are synchronous. A `StreamSummary` holds `source`, `channel`, `status` (a
`BaselineStatus` under the system's drift policy), `samples` (the observations folded into the
baseline), `fingerprints`, each a `FingerprintSummary` (`schema_hash`, `observations`,
`first_observed_at`, `last_observed_at`), and `drift`, the stream's open `DriftSignal`s sorted
by `first_seen`, then `schema_hash`; `key` and `observations` (the sum over its fingerprints)
are properties. Both summary models are in `fire/core/domain/observation.py` and reject
unknown fields.

`samples` counts folded observations only, while `observations` and the fingerprint counts
include drifted ones, so `samples` is lower than `observations` once a stream has drifted.

Per stream key the catalog keeps the observation count, a `Baseline`, and, per fingerprint, the
`Shape`, its count, and the earliest and latest `observed_at`, whatever the arrival order. Per
accepted shape it keeps an index entry: the dedupe key, the stream key, `schema_hash`, both
timestamps, `region`, `trace_id`, `span_id`, `work_id`, and `correlation_id`. Spans are kept
whole. Flows read spans and index entries ([Shapes on flows](flows-and-paths.md#shapes-on-flows));
baselines fold per stream key ([Baseline](ingest-drift-and-feedback.md#baseline)). Steps 2.1 and
2.2 gave the port its baseline reads, the `status`, `samples`, and `drift` of `streams`; the
flow reads come with step 1.9.

Checking and storing a dedupe key is one atomic step, so concurrent tentacles, two regions, and
resent batches store each observation once. `MemoryObservationCatalog` guards every operation
with one lock and keeps its dedupe index, baselines, and drift signals for the life of the
process; a retention window comes with the SQLite adapter in step 3.1. It takes each system's
drift policy, the system YAML's `drift` block, from `build_container`; a system it does not name
gets the defaults.

## Streams endpoint

`GET /v0/systems/{system}/streams` requires `X-FIRE-Token` (`401`, `503` as above, checked
first). An unknown system is `404`; a known system with no observations has an empty `streams`
list.

```json
{"system": "omg", "streams": [{
  "source": "domain-events", "key": "event:internal:fulfillment.allocated:payload",
  "status": "warm", "samples": 40, "observations": 43,
  "fingerprints": [{"schema_hash": "3f1c…", "observations": 40,
                    "first_observed_at": "2026-10-11T16:30:01Z",
                    "last_observed_at": "2026-10-11T17:02:44Z"},
                   {"schema_hash": "9b2e…", "observations": 3,
                    "first_observed_at": "2026-10-11T17:05:12Z",
                    "last_observed_at": "2026-10-11T17:06:40Z"}],
  "drift": [{"schema_hash": "9b2e…",
             "diff": {"added": [],
                      "type_changed": [{"path": "order_id", "old": ["string"],
                                        "new": ["integer"]}],
                      "removed": []},
             "occurrences": 3, "first_seen": "2026-10-11T17:05:12Z",
             "last_seen": "2026-10-11T17:06:40Z"}]}]}
```

Streams are sorted by `source`, then `key`; fingerprints by `observations` descending, then
`schema_hash`; drift signals by `first_seen`, then `schema_hash`. Spans are not streams; they
show in `/paths` ([Paths endpoint](flows-and-paths.md#paths-endpoint)).

Each stream carries its baseline's `status` and `samples` (step 2.1) and, after `fingerprints`,
its open drift signals under `drift` (step 2.2;
[Read endpoints](ingest-drift-and-feedback.md#read-endpoints)). `status` is
`baseline rebuilding` until `samples` reaches the system's `drift.min_samples`, then `warm`
([Baseline](ingest-drift-and-feedback.md#baseline)). A stream with no drift has `"drift": []`.

A drifted fingerprint is never folded, so `samples` leaves it out while `observations` counts
it. It is listed in `fingerprints` too, with `observations`, `first_observed_at`, and
`last_observed_at` equal to its signal's `occurrences`, `first_seen`, and `last_seen`
([Drift rules](ingest-drift-and-feedback.md#drift-rules)).

## Migration

Step 1.1 replaced the main spec's event-only `Observation`; its
[Observation](ingest-drift-and-feedback.md#observation) and
[Ingest endpoint and catalog](ingest-drift-and-feedback.md#ingest-endpoint-and-catalog)
sections are links here.

| Before | After |
|--------|-------|
| `event_type` | `channel` with `kind: event`, `part: payload`, and the event type as `name` |
| `event_id`; DLQ joins on it | `observation_id`; DLQ joins on `work_id` |
| Stream key `(system, source, event_type)` | `(system, source, channel.key)` |
| `{"observations": [...]}` | `{"shapes": [...], "spans": [...]}` |
| Observed event types for `audit_coverage` | Names of `kind: event` channels |
| `ShapeCatalog`, `ShapeRecord`, `OriginRegistry` | `ObservationCatalog`; the other two are removed |

Step 1.2 deleted `fire/core/ports/shape_catalog.py`, `fire/core/ports/origin_registry.py`,
`ShapeRecord`, their checks in `tests/unit/test_origin_and_process_hop.py`, the `ShapeRecord`
sentence in [dossier-and-decision.md](dossier-and-decision.md), and the repo map's note on
them. `Origin` stays. Step 1.2 also added `sources[].id` ahead of step 1.3: required,
`^[a-z0-9][a-z0-9_-]*$`, unique within a system, and set in `omg.yml` and `_template.yml`.

## Tests

Step 1.2's tests are `tests/unit/test_observation_catalog_memory.py`,
`tests/unit/test_observation_ingest.py`, and `tests/integration/test_observations_api.py`.
Step 2.1 added `tests/unit/test_baseline.py` and cases in
`tests/unit/test_observation_catalog_memory.py`, `tests/unit/test_system_config.py`, and
`tests/integration/test_observations_api.py`. Step 2.2 added `tests/unit/test_shape_drift.py`,
cases in `tests/unit/test_observation_catalog_memory.py`, `tests/unit/test_observation_ingest.py`,
and `tests/integration/test_observations_api.py`, and the L1 test
`tests/integration/test_order_payload_drift.py`. Its fixture,
`tests/fixtures/omg/api_requests/test_order_payload.json`, is OMG's place-order request body
copied with placeholder values that keep every value type, so its shape equals OMG's; an
`omg_local` test checks that. Step 2.4 added cases in `tests/unit/test_observation_ingest.py`
for the hook passed per call; its other tests are in
[Drift incidents](ingest-drift-and-feedback.md#drift-incidents). Integration tests use
`TestClient` with config written to `tmp_path`.

| Step | Level | Proves |
|------|-------|--------|
| 1.1 | L0 | Valid channels render their keys; invalid `part` and `status_class` combinations raise; names with `:` never collide |
| 1.1 | L0 | `payload`, `body`, and `attributes` fail at the top level, in `channel`, in `shape.fields[*]`, and in `links`; bad ids, naive datetimes, empty or long strings, and negative durations fail; JSON round trips |
| 1.2 | L0 | A second add of one key returns `False` and changes nothing; concurrent adds store once; one event from two sources is two observations, one span from two sources is one |
| 1.2 | Integration | `401` and `503` come before `422`; a mixed batch returns each reason at its index with correct counts; a changed `schema_hash` and a set `notes` are mismatches; a resent batch is all `duplicate` |
| 1.2 | Integration | `/streams` counts accepted items only and is `404` for an unknown system; a canary under an unknown key of a rejected item reaches neither the response nor the logs |
| 2.1 | L0 | A fold counts a pair listed twice in one shape once and a known fingerprint again; status is `warm` at `min_samples`, not one sample before; `compares` is `False` while rebuilding and for a folded fingerprint; each stream key has its own baseline; each system gets its own policy and an unnamed one the defaults; a duplicate folds nothing; `drift` rejects unknown keys and out-of-range values |
| 2.1 | Integration | `/streams` turns a stream from `baseline rebuilding` to `warm` at its system's `drift.min_samples`, with `samples` equal to `observations`, while a system on the default stays rebuilding; a resent batch folds nothing |
| 2.2 | L0 | Each rule fires alone and all at once, each list sorted; `type_changed` lists every seen type and only the unseen new ones, with no removal at that path; a pair carried by exactly `required_ratio` of samples is required and one fewer is not; a required pair whose path comes only with another seen type is removed; optional-key variation, `cardinality`, and `optional` fold silently; the rules behave the same on event, request, and response channels |
| 2.2 | L0 | Nothing is compared while rebuilding, the observation that reaches `min_samples` included; a folded fingerprint folds again without comparison; a drifted fingerprint never folds, 100 occurrences in any order are one signal of 100, and two drifted fingerprints are two signals; `admit` and `streams` return copies; drifted observations count in fingerprints, not `samples`; a duplicate adds no occurrence; concurrent adds of one drifted fingerprint raise one signal; `streams` sorts signals by `first_seen`, then `schema_hash` |
| 2.2 | L0 | `on_drift` is called once per new signal with the observation that raised it, after the whole batch is counted, in batch order, and never for a later occurrence or a rejected item; drifted items count as `accepted`; the drift models reject unknown keys at every depth, empty type lists, zero occurrences, and naive datetimes; `log_drift` writes one `INFO` line with counts and no paths |
| 2.2 | L1 | 20 variants of OMG's place-order request toggling `adminId`, `adminName`, and `partyId` warm the request stream with no drift, and a combination never seen folds silently; a copy without `timezone` and with an integer `referenceOrderId` raises one signal with that diff, and 100 copies are one signal with 100 occurrences and one log line; removing a required `checkoutId`, or `totals` with its children, reports only those removals; no placeholder value reaches `/streams`, the logs, or the catalog |
| 2.2 | Integration | A different shape on a rebuilding stream folds and raises nothing; a drifted shape on a warm stream is `accepted` and listed under `drift` with its exact diff, with `samples` lower than `observations`; sent again, it adds occurrences and logs one line without its paths; a duplicate of it adds no occurrence |
| 2.4 | L0 | Each `ingest` call reports its own signals to the hook passed with it, by 2.2's rules; a call without a hook still records the drift; a hook call that raises logs one `ERROR` line without the exception's message, and the next signal's call still runs |

## Open questions

1. Object keys that are data, such as a map keyed by SKU, put values into `ShapeField.path`
   and give every payload a new fingerprint. Whether the generalizer collapses them belongs in
   [shape-generalizer.md](shape-generalizer.md).
2. A duplicate whose fingerprint differs from the stored one means two records share an id.
   Whether to count it apart, as `conflict`.
3. The largest batch the body takes, and the status for a larger one (`413`).
4. The length of the step 3.1 retention window, and whether `config/fire.yml` sets it.
