# Ingest, drift, and feedback

**Status:** phase 0, [Baseline](#baseline) (roadmap step 2.1), [Drift rules](#drift-rules)
(step 2.2), and [Drift incidents](#drift-incidents) for shape drift (step 2.4) are the contract
of the shipped code; step 2.3 opens path drift incidents. Phase 1 lives in the specs its
sections link to, each with its own status; the rest of phases 2 to 4 is a proposal.

This is the route from the first engine, which only diagnosed what a caller posted to
`POST /v0/diagnose`, to an engine that watches a system on its own, detects shape drift,
diagnoses incidents, and learns from feedback. The order of work, with what each step must
achieve and how it is tested, is in [docs/roadmap.md](../roadmap.md).

## Goal

FIRE receives nothing from clients. A **tentacle** reads a watched system's data streams,
turns every payload into a shape and a fingerprint at the edge, and sends only those to the
**body** (the incident engine). The body keeps a baseline of shapes per event type, reports
drift and coverage gaps, opens incidents, and asks a reasoner for a decision. A person grades
each decision, and the grades feed the reasoner's retraining.

The first watched system is OMG. Everything OMG-specific lives in `config/systems/omg.yml`.

## Milestones

| Phase | Delivers | Done when |
|-------|----------|-----------|
| 0 | Repo-level Cursor rules for FIRE; `config/systems/omg.yml` | Agents get an accurate repo map; the engine loads OMG's manifest |
| 1 | Observation contract, tentacle, ingest endpoint, catalog | A replay of OMG events fills the catalog |
| 2 | Baselines, drift, drift incidents, coverage | **Local demo**: a changed payload produces one drift notification |
| 3 | Triggers: incident.io, 5xx bursts, DLQ attribution | An incident.io incident or a 5xx burst produces a diagnosis |
| 4 | Decision ledger, feedback endpoint, Laya/Jev reasoner | A graded decision is stored and exportable for retraining |

Phases 3 and 4 depend on the catalog and the event ids from phase 1.

## Topology

```
watched system streams          tentacle (fire.apps.tentacle)            body (fire.apps.incident_engine)
Kinesis / replay files  ──raw──▶ envelope → generalize → fingerprint ──▶ POST /v0/observations
                                 raw payload never leaves this process    catalog → drift → incident
                                                                          → reasoner → narrator → notifier
```

- The tentacle is a separate process. It runs the `ShapeGeneralizer` and
  `ShapeFingerprinter` adapters and sends `ShapeObservation`s and `SpanObservation`s over HTTP
  with `X-FIRE-Token`.
- The body never sees a payload. It recomputes the fingerprint from the observation's shape
  and rejects the observation on a mismatch.
- A durable buffer between tentacle and body (SQS, Kinesis) replaces HTTP once FIRE runs
  outside a laptop. The observation contracts do not change.

## Facts about OMG this design relies on

Checked against `order-management-gateway`:

- A Kinesis record is a `DomainEvent` (`core/src/models/domain_event.py`) with camelCase
  keys: `id`, `domain`, `eventType`, `occurredAt`, `payload`. `id` is the work unit's
  `work_unit_id` (a ULID). There is no `correlation_id` and no region on the record.
- `kinesis-publisher` runs in every region, so the same `id` can appear in the us-east-1 and
  us-west-2 streams. Deduplication is by `id`.
- Outbox rows, including `ABANDONED` ones, are keyed by `work_unit_id`. A DLQ outcome joins
  to an observation on that id.
- The manifest declares payload fields by their snake_case Python names with Python types
  (`allocated_items: list[AllocatedItem]`), top level only. The Kinesis payload uses
  camelCase (`allocatedItems`).
- `scripts/test_payloads/` holds five `WorkUnit` samples (snake_case, internal form). The
  other files there are API request bodies, not events.

## Phase 0: repo rules and OMG config

- `.cursor/rules/` holds FIRE's repo map, its conventions, and a rule scoped to
  `config/systems/*.yml`.
- `config/systems/omg.yml` points discovery at OMG's `event_chains_graph` through
  `OMG_MANIFEST_PATH` and declares the Kinesis source with `dedupe_key: id` and
  `sample_rate: 1.0`. Against OMG's committed graph the engine discovers 125 events, 31
  processors, and 5 ingress points. With the variable unset it starts and reports the
  inventory as missing.
- `notify.slack.channel_id` is the placeholder `C0000000000` until open question 1 is
  answered.

## Phase 1: ingest plane

The tentacle intercepts what enters and leaves the system: inbound HTTP, inbound external
events, internal domain events, outbound partner calls, and outbound AWS calls. It keeps
fingerprints per endpoint and the trace logic of each entry point. The details live in four
proposals, each implemented by the roadmap steps it names.

### Observation

[observations.md](observations.md) defines `Channel` (event or endpoint, with direction,
part, and status class), `ShapeObservation`, `SpanObservation`, the stream key
`(system, source, channel)`, deduplication, the ingest endpoint, and the catalog port. It
replaces the event-only observation this section first proposed.

### Sources in the system YAML

[tentacle.md](tentacle.md#sources-in-the-system-yaml) defines each source's `id`, `kind`, and
`envelope`, and the record kinds `replay`, `kinesis`, `dynamodb_stream`, `sns_fanout`, and
`s3_capture`. [http-and-span-capture.md](http-and-span-capture.md) defines the kinds `otlp`
and `http_proxy`. `envelope.event_id` replaces `dedupe_key`.

### Tentacle

[tentacle.md](tentacle.md) defines the process, the sender, the record pipeline, the privacy
rules, and the canary harness. [http-and-span-capture.md](http-and-span-capture.md) defines
the span attribute allowlist, operation names, and the inbound proxy.
`fingerprinting.sample_rate` is applied in the tentacle; DLQ attribution (phase 3) needs every
event's fingerprint, so OMG uses `1.0`.

### Ingest endpoint and catalog

[observations.md](observations.md#ingest-endpoint) defines `POST /v0/observations` and the
`ObservationCatalog` port that replaces `ShapeCatalog`. The first adapter is in memory
(`fire/adapters/memory/`); a restart loses the baselines and the engine reports them as
rebuilding. SQLite comes after the demo.

### Flows and paths

[flows-and-paths.md](flows-and-paths.md) defines how the body assembles spans into flows,
learns a path per entry operation, and attaches shapes to their spans.

## Phase 2: drift

### Baseline

Per stream key, the catalog keeps a `Baseline` (`fire/core/domain/baseline.py`): what the
observations folded so far carried, and its drift signals. It holds paths, value types, schema
hashes, counts, and observation times, never values. It is a dataclass, not a wire model:
`/streams` shows only its `status`, `samples`, and `signals`, as `drift`
([Streams endpoint](observations.md#streams-endpoint)).

| Field | Holds |
|-------|-------|
| `samples` | The number of observations folded in |
| `presence` | Per `(path, value_type)`, how many folded observations carried it; a shape that lists one pair twice counts once |
| `fingerprints` | The schema hashes folded so far |
| `signals` | One `DriftSignal` per drifted schema hash ([Drift rules](#drift-rules)); a drifted fingerprint is never folded |

The catalog admits each shape it stores to the baseline of its stream key, in the same atomic
step that stores it ([ObservationCatalog](observations.md#observationcatalog)): the shape
folds, or counts on a drift signal ([Drift rules](#drift-rules)). A duplicate changes nothing.

Until `samples` reaches the system's `drift.min_samples`, the stream's status is
`baseline rebuilding` and it raises no drift; from then on it is `warm` (`BaselineStatus`).
This is the honest-degradation rule: no baseline, no drift claims.

A fingerprint seen before skips comparison: `Baseline.compares(schema_hash, policy)` is true
only on a warm baseline, for a fingerprint it has not folded. A folded fingerprint folds
again, so `presence` counts observations, not fingerprints. A compared fingerprint folds only
when no drift rule fires for it ([Drift rules](#drift-rules)).

`MemoryObservationCatalog` keeps the baselines in process memory, so after a restart every
stream starts again in `baseline rebuilding`.

The system YAML's `drift` block (`DriftPolicy`) sets the thresholds per system. It is
optional, unknown keys are rejected, and a system without it gets the defaults. `omg.yml` and
`_template.yml` declare both keys at their defaults.

| Key | Type | Bounds | Default | Meaning |
|-----|------|--------|---------|---------|
| `min_samples` | `int` | `1` or more | `20` | Observations folded before a stream is warm |
| `required_ratio` | `float` | In `(0, 1]`, finite | `1.0` | Share of samples that makes a `(path, value_type)` required; the `removed` rule reads it ([Drift rules](#drift-rules)) |

```yaml
drift:
  min_samples: 20
  required_ratio: 1.0
```

### Drift rules

When the baseline compares a fingerprint ([Baseline](#baseline)), `Baseline.diff(shape, policy)`
checks the shape against it. The rules read `presence` only: a path or a value type the stream
saw is one that some folded sample carried.

| Change | Rule | Reported as |
|--------|------|-------------|
| Field added | The shape carries a `(path, value_type)` whose `path` the stream never saw | `added`, one `FieldRef` per pair |
| Type changed | The shape carries a `path` the stream saw, with a `value_type` never seen at that path | `type_changed`, one `TypeChange` per path, with `old` and `new` types |
| Field removed | A `(path, value_type)` with `presence / samples >= drift.required_ratio` is absent from the shape, and its `path` is not in `type_changed` | `removed`, one `FieldRef` per pair |

The models are in `fire/core/domain/drift.py` and reject unknown fields. They name paths and
value types, never values.

| Model | Holds |
|-------|-------|
| `FieldRef` | `path` and `value_type` |
| `TypeChange` | `path`; `old`, every value type the baseline saw at the path; `new`, the shape's value types there that the baseline never saw. Both are non-empty and sorted |
| `ShapeDiff` | `added`, `type_changed`, and `removed`, each sorted by path, then value type; `is_empty` is true when all three are empty |
| `DriftSignal` | `schema_hash`, `diff`, `occurrences` (`1` or more), and the timezone-aware `first_seen` and `last_seen` |
| `Admission` | `duplicate`, `folded`, `drift_raised`, `drift_repeated` |
| `ShapeAdmission` | `outcome`, an `Admission`; `signal`, set on the two drift outcomes and on no other; `stored`, a property, true on every outcome but `duplicate` |

A path in `type_changed` is not also reported as removed: OMG's `referenceOrderId` sent as an
integer instead of a string is one type change, not a type change and a removed `string`. A
required pair is still reported as removed when the shape carries its path only with another
type the stream already saw. With `required_ratio: 0.9`, a `discount` that was an `integer` in
95% of samples and `null` in the rest is reported removed, as `integer`, for a shape where it
is `null`.

The ratio is absolute: a pair's `presence` over all of the stream's `samples`, compared as
`presence / samples >= required_ratio`. At the default `1.0`, a pair is required only when
every sample carried it. `cardinality` and `optional` are not compared, so a new fingerprint
that differs from the baseline only in them has an empty diff. Open question 7 lists what the
absolute ratio does not cover, and open question 8 how `null` is judged.

Comparing fingerprints alone is not enough: a payload with an optional key present and one
without it have different fingerprints but the same form. A new fingerprint with an empty diff
folds into the baseline silently.

`Baseline.admit(schema_hash, shape, observed_at, policy)` decides what a stored shape does to
the baseline. The catalog calls it under its lock, in the same step that stores the
observation, and `add_shape` returns the `ShapeAdmission`
([ObservationCatalog](observations.md#observationcatalog)). The first case that matches
decides:

| Case | Effect | `outcome` |
|------|--------|-----------|
| The dedupe key is known; the catalog checks this before `admit` | Nothing changes, so no occurrence is added | `duplicate` |
| The fingerprint has a signal | Adds an occurrence; not folded | `drift_repeated` |
| The baseline compares the fingerprint and the diff is not empty | Opens a signal with one occurrence; not folded | `drift_raised` |
| Anything else | Folds | `folded` |

The last case covers a stream below `min_samples`, where nothing is compared, the observation
that brings it to `min_samples` included; a folded fingerprint, which folds again without
comparison; and a new fingerprint with an empty diff, which is optional-key variation. A drift
outcome carries the signal as that occurrence left it.

A drifted fingerprint becomes one `DriftSignal` per `(stream key, fingerprint)`, kept in
`Baseline.signals`. Its `diff` is the one computed for the occurrence that opened it.
`occurrences` counts the stored observations with that fingerprint, and `first_seen` and
`last_seen` are the earliest and latest `observed_at` among them, whatever the arrival order. A
drifted fingerprint is never folded and stays drifted; acknowledging a drift so it joins the
baseline is phase 4 feedback.

`/streams` lists a stream's open signals under `drift`, sorted by `first_seen`, then
`schema_hash` ([Streams endpoint](observations.md#streams-endpoint)). In the L1 test, a warm
`http:inbound:POST /v1/orders/place_order:request` stream receives OMG's place-order request
100 times with `timezone` removed and `referenceOrderId` sent as an integer. That is one
signal:

```json
{"schema_hash": "c47d…",
 "diff": {"added": [],
          "type_changed": [{"path": "referenceOrderId", "old": ["string"],
                            "new": ["integer"]}],
          "removed": [{"path": "timezone", "value_type": "string"}]},
 "occurrences": 100,
 "first_seen": "2026-10-11T16:30:21Z", "last_seen": "2026-10-11T16:32:00Z"}
```

A signal notifies once. `IngestService.ingest(shapes, spans, on_drift=None)` takes an optional
hook (`DriftHook`), called with the observation that raised a signal and the signal, once per
new signal, after the whole batch is counted, in batch order. A later occurrence and a rejected
item call nothing. The hook is passed per call, not to the constructor, so the body's hook can
schedule work on the request's background tasks. For each new signal, the hook of
`POST /v0/observations` logs one `INFO` line with `log_drift`
(`fire/apps/incident_engine/wiring.py`) and opens a drift incident
([Drift incidents](#drift-incidents)):

```
shape drift raised: system=<system> source=<source> stream=<channel key> fingerprint=<schema_hash> added=<n> type_changed=<n> removed=<n>
```

The line carries names and counts only, never paths, because a path can hold data
([observations.md, open question 1](observations.md#open-questions)). The L1 scenario logs one
line, with `added=0 type_changed=1 removed=1`.

A hook call that raises does not stop the others or change the response. The batch is already
stored, so a resend would count as duplicates and never raise those signals again. `ingest`
logs one `ERROR` line with the exception's type and not its message, which may quote the shape,
then calls the hook for the next signal:

```
drift hook failed: system=<system> source=<source> stream=<channel key> fingerprint=<schema_hash> error=<exception type>
```

`drift.min_samples` and `drift.required_ratio` are set per system ([Baseline](#baseline)).

### Drift incidents

A new shape drift signal opens one incident. For each new signal, the hook of
`POST /v0/observations` ([Drift rules](#drift-rules)) calls
`DriftIncidentService.open_shape_drift(observation, signal)`
(`fire/core/services/drift_incident.py`), which returns the incident and its dossier, and runs
their diagnosis (reasoner, then narrator, then notifier) in a background task of the request.
The response is still `202`, in the same format. A later occurrence of the signal opens
nothing, so 100 drifted copies are one incident.

`Incident` (`fire/core/domain/incident.py`) has a required `trigger`
([Incident](dossier-and-decision.md#incident)): `shape_drift` here, `path_drift` from step 2.3,
`http_errors` and `incident_io` from steps 3.3 and 3.4, and `manual` for `POST /v0/diagnose`.
A shape drift incident:

| Field | Value |
|-------|-------|
| `id` | A new uuid4 |
| `system` | The observation's `system` |
| `trigger` | `shape_drift` |
| `service` | `None` |
| `summary` | `Shape drift on <channel key>` |
| `started_at` | The signal's `first_seen` |

Its dossier holds the drifted observation's `shape` and `fingerprint`, an empty `process_path`,
no `origin`, no `service`, and these fragments, in this order:

| Collector | Quality | Summary | Data |
|-----------|---------|---------|------|
| `shape_drift` | `present` | `Shape drift on <channel key>: added=<n> type_changed=<n> removed=<n>` | `source`, `stream` (the channel key), `schema_hash`, `diff`, `occurrences`, `first_seen` |
| `baseline` | `present` | `<status>: <samples> samples, <n> fingerprints known` | `source`, `stream`, `status`, `samples`, `fingerprints`, `observations`, `drift_signals` |
| `baseline`, when the catalog has no summary of the stream | `missing` | `No baseline for this stream` | `source`, `stream` |
| `coverage`, when discovery found an inventory | `present` | `<n> declared events unseen, <n> seen undeclared, <n> unknown references` | The `CoverageReport`: `system`, `declared_unseen`, `seen_undeclared`, `unknown_references` |
| `coverage`, when discovery found none | `missing` | The discovery's `missing_reason`, such as `No manifest declared (observation only)` or `OMG_MANIFEST_PATH is not set` | Empty |
| `coverage`, when the service has no scope for the system | `missing` | `No coverage scope for this system` | Empty |
| `process_path` | `missing` | `No complete flow` | Empty |

`shape_drift` holds the signal as the occurrence that raised it left it: `diff` is its
`ShapeDiff` (`added`, `type_changed`, `removed`, as in [Drift rules](#drift-rules)),
`occurrences` is `1`, and `first_seen` is in ISO 8601. Later occurrences show in `/streams`. In
the L1 scenario:

```json
{"collector": "shape_drift", "quality": "present",
 "summary": "Shape drift on http:inbound:POST /v1/orders/place_order:request: added=0 type_changed=1 removed=1",
 "data": {"source": "api-proxy", "stream": "http:inbound:POST /v1/orders/place_order:request",
  "schema_hash": "c47d…",
  "diff": {"added": [],
           "type_changed": [{"path": "referenceOrderId", "old": ["string"], "new": ["integer"]}],
           "removed": [{"path": "timezone", "value_type": "string"}]},
  "occurrences": 1, "first_seen": "2026-10-11T16:30:21Z"}}
```

`baseline` reads the stream's summary from the catalog's `streams(system)` when the incident
opens, after the whole batch is counted. `status` is its `BaselineStatus`, `warm` on any stream
that raises drift; `samples` counts its folded observations; `fingerprints` counts its folded
fingerprints, the stream's fingerprints minus its drifted ones; `observations` counts every
stored observation of the stream, drifted ones included; and `drift_signals` counts its open
signals.

`build_container` gives `DriftIncidentService` the catalog and one `CoverageScope` per loaded
system (`fire/core/services/coverage.py`), built from the system's `Discovery` and `policy`:
exactly one of `inventory` and `missing_reason`, plus `policy.critical` and `policy.ignore`.
With an inventory, `coverage` runs `audit_coverage`
([Coverage audit](system-config-and-discovery.md#coverage-audit)) with the system's observed
event types: the names of its `kind: event` channels in the catalog, across its sources. `http`
channels are not events and are left out.

`process_path` is missing because flows come with step 1.9
([Process path in the dossier](flows-and-paths.md#process-path-in-the-dossier)). Whether a
shape drift incident should wait for its shape's flow is
[flows-and-paths.md, open question 3](flows-and-paths.md#open-questions). Until then every
shape drift dossier has a missing fragment, so its decision is `noul` at confidence `0.2`.

`EchoReasoner` builds one possibility per `present` `shape_drift` or `path_drift` fragment, in
fragment order, after the process path's hops and before `insufficient-evidence`;
[EchoReasoner](dossier-and-decision.md#echoreasoner) defines every possibility, its label, and
the confidence. A shape drift label names the channel and every change,
`Shape drift on <channel key>: <changes>`, the changes joined by `; ` in the order added, type
changed, removed.

`ConsoleNotifier` prints a header line,
`[fire] notify incident=<id> system=<system> trigger=<trigger>`, then the narration, to the
`sys.stdout` current at publish time. The L1 scenario, with no manifest configured, prints one
notification:

```
[fire] notify incident=<uuid> system=omg trigger=shape_drift
*FIRE ENGINE* incident `<uuid>` (`omg`)
Confidence: 20% — `noul` (do not treat as settled cause)
Possibilities:
• (50%) Shape drift on http:inbound:POST /v1/orders/place_order:request: referenceOrderId changed string -> integer; timezone (string) removed _[shape_drift]_
• (50%) Insufficient evidence (MISSING fragments) _[coverage, process_path]_
Shape hash: `c47d…`
_Narrator: template stub (swap OpenAI-compat via FIRE_NARRATOR_*)._
```

The notification names the changed paths and their types, as `/streams` does; the `INFO` line
names counts only ([Drift rules](#drift-rules)).

`path_drift` is a `Trigger` value and `EchoReasoner` reads `path_drift` fragments
([Path drift](flows-and-paths.md#path-drift)), but nothing opens a path drift incident yet. Step
2.3 does, with one `path_drift` fragment per signal the flow fired and the flow's process path.

Step 2.4's tests are `tests/unit/test_drift_incident.py`, `tests/unit/test_console_notifier.py`,
cases in `tests/unit/test_echo_reasoner.py`, `tests/unit/test_observation_ingest.py`,
`tests/unit/test_inventory_and_coverage.py`, `tests/unit/test_origin_and_process_hop.py`,
`tests/unit/test_dossier.py`, `tests/unit/test_diagnose_with_origin.py`, and
`tests/unit/test_shape_contracts.py`, and the integration test
`tests/integration/test_drift_incidents.py`.

| Step | Level | Proves |
|------|-------|--------|
| 2.4 | L0 | The incident is a `shape_drift` on the channel from the signal's `first_seen`; the dossier carries the drifted shape and the four fragments in order; `shape_drift` names the stream and its diff; `baseline` counts only the folded fingerprints of its own stream, and is missing for a stream the catalog does not hold; `coverage` audits the names of event channels only, honors `critical` and `ignore`, and is missing with the scope's reason or without a scope; `process_path` is missing; a `CoverageScope` rejects both or neither of `inventory` and `missing_reason` |
| 2.4 | L0 | `EchoReasoner` builds one possibility per present drift fragment, after the hops, in fragment order; the shape drift label lists added, then type changed, then removed; the path drift label names the rule, the node, and the entry; malformed data falls back to the summary; a missing drift fragment counts only as missing evidence; a present drift fragment alone is evidence; the `unknown` label names the evidence that is absent; a shape drift with no flow is `noul` |
| 2.4 | L0 | Each `ingest` call reports its own signals to its own hook; `ConsoleNotifier` names the incident, system, and trigger, and writes to the `sys.stdout` current at publish or to a given stream; `via` is accepted on edge hops, rejected on a node hop and as an unknown value, and survives a JSON round trip; `Dossier` takes `system` and rejects `project_id`; `Incident` names its `system` and requires a known `trigger`; `/v0/diagnose` rejects a system that is not loaded and a fingerprint that does not match its shape, and keeps a fingerprint sent without a shape |
| 2.4 | Integration | A baseline batch and a drifted batch posted to `/v0/observations` give exactly one console notification naming the channel and the diff, and 99 more copies give no second one; the dossier holds the diff, the baseline, and the missing evidence; with a manifest, `coverage` audits the observed event types against it; no placeholder value reaches stdout, the logs, or a response; `/v0/diagnose` with `system` notifies once, as `manual` |

### Coverage and contract

- `audit_coverage` runs with the catalog's observed event types per system. Since step 2.4 it
  runs on each shape drift incident ([Drift incidents](#drift-incidents)); step 2.5 runs it on
  `GET /v0/systems/{system}/coverage`.
- Contract drift (optional for the demo): compare an observation's top-level payload keys
  with the manifest's declared fields. Casing differs, so the system YAML declares the wire
  casing (`discovery.manifest.wire_case: camel`). Only names are compared; Python type names
  do not map to `ValueType`.

### Read endpoints

`GET /v0/systems/{system}/streams` lists each stream key with its `status`, `samples`, and open
drift signals (steps 2.1 and 2.2). Both read endpoints require `X-FIRE-Token`.

### Demo

1. Start the body with `config/systems/omg.yml` loaded.
2. Replay a baseline corpus: synthetic records in the form of OMG's `WorkUnit` samples, at
   least `min_samples` per event type, with varied values and optional keys present and
   absent.
3. Replay a drift corpus where `fulfillment.allocated` lacks
   `allocated_items[*].facility_short_name` and `order_id` is an integer.
4. One notification names the stream, the removed path, and the type change; optional-key
   variation in step 2 produced none.
5. Optionally, point the tentacle at the dev Kinesis stream for a real baseline.

## Phase 3: triggers

- **incident.io**: `POST /v0/webhooks/incident-io`, verified with the incident.io signing
  secret. The system YAML says which incidents belong to the system.
- **5xx bursts**: at least `threshold` responses with status at or above `status_min` from one
  endpoint within `window_s` open one incident. Thresholds live in the system YAML under
  `triggers.http_errors`.
- **DLQ attribution**: FIRE reads `ABANDONED` outcomes and joins them to observations on
  `work_unit_id` = `event_id`. Per fingerprint it keeps observed and abandoned counts, so a
  fingerprint whose abandon rate stands out becomes a dossier fragment. Events that never
  pass through a source FIRE reads (39 of OMG's 125 declared events are not on Kinesis)
  cannot be attributed and are reported as `missing`.
- **Correlation**: triggers for one system inside one window collapse into one incident.
- **Slack**: a notifier that posts to `notify.slack.channel_id` of the incident's system.

## Phase 4: decision ledger and feedback

- Port `DecisionLedger`: per decision, a `decision_id`, the incident, the system, the
  dossier as the reasoner saw it, the `Decision`, the reasoner name and version, and the
  feedback history.
- Every decision starts `pending`. `POST /v0/decisions/{decision_id}/feedback` takes
  `{"outcome": ..., "possibility_id": ..., "note": ...}` with `X-FIRE-Token`:

| `outcome` | Meaning |
|-----------|---------|
| `success` | The decision named the cause |
| `failure` | The decision missed the cause entirely |
| `undefined` | The decision was partly right, or right but not useful |

  `possibility_id` (optional) names the possibility that matched. A later grade replaces the
  current one and the history keeps both.
- Graded decisions are exportable as JSONL (dossier, decision, outcome) for retraining.
- Port `Reasoner` gets a Laya/Jev adapter over HTTP (`FIRE_SYSTEMONE_BASE_URL`). It sends
  the dossier and receives a `Decision`.
- Slack buttons for the three outcomes call the feedback endpoint.

## Changes to existing code

| Before | After | Step |
|--------|-------|------|
| `project_id` on `Incident`, `Dossier`, `DiagnoseRequest`, and the `/v0/diagnose` response | `system`, which `/v0/diagnose` checks against the loaded systems, so the notifier can pick the Slack channel | 2.4 |
| `Incident` says nothing about what opened it | A required `trigger` ([Incident](dossier-and-decision.md#incident)) | 2.4 |
| `sources[].dedupe_key` | `sources[].envelope`; new `kind: replay` | 1.3 |
| `fingerprinting.sample_rate` validated only | Applied in the tentacle | 1.3 |
| A client `fingerprint` on `/v0/diagnose` kept as is | Recomputed when a `shape` is present; a mismatch is `422`, as an observation's is rejected | 2.4 |
| `EchoReasoner` says "No path/shape evidence yet" when a shape is present | One possibility per drift fragment; the `unknown` label names only the evidence that is absent | 2.4 |
| `ProcessHop` cannot mark an edge that crosses a span link | An optional `via` on edge hops ([ProcessHop](dossier-and-decision.md#processhop)) | 2.4 |
| `IngestService` takes `on_drift` in its constructor | `ingest` takes it per call ([Drift rules](#drift-rules)) | 2.4 |

Step 1.1 made `Shape`, `ShapeField`, and `ShapeFingerprint` reject unknown fields
([Privacy invariants](observations.md#privacy-invariants)). Step 1.2 replaced `ShapeCatalog`
and `ShapeRecord` with `ObservationCatalog`, removed `OriginRegistry`, and added
`sources[].id` ([Migration](observations.md#migration)). Step 1.3 replaced `dedupe_key` with
`envelope`, added the source kinds and `kind: replay`, and applies `sample_rate` in the tentacle
([Migration](tentacle.md#migration)). Step 2.1 gave `ObservationCatalog` a baseline per stream
key ([Baseline](#baseline)). Step 2.2 made `add_shape` return a `ShapeAdmission` instead of a
`bool` and gave `IngestService` its `on_drift` hook ([Drift rules](#drift-rules)).

Step 2.4 renamed `project_id` to `system` on `Incident`, `Dossier`, `DiagnoseRequest`, and the
`/v0/diagnose` response, and `/v0/diagnose` rejects a system that is not loaded. It added the
required `Incident.trigger`, so `/v0/diagnose` opens `manual` incidents and drift opens
`shape_drift` ones ([Drift incidents](#drift-incidents)). `/v0/diagnose` now recomputes the
fingerprint of a `shape` and rejects a client fingerprint that differs
([Diagnose endpoint](dossier-and-decision.md#diagnose-endpoint)). `EchoReasoner` builds drift
possibilities and its `unknown` label no longer claims a shape is absent when one is present
([EchoReasoner](dossier-and-decision.md#echoreasoner)). `ProcessHop` gained `via`, set on edge
hops only, and `ConsoleNotifier` names the incident's system and trigger. `on_drift` moved from
the `IngestService` constructor to `ingest`.

## Open questions

1. The real Slack channel id for OMG.
2. How an incident.io incident maps to a system (service, catalog entry, or custom field).
3. Where 5xx counts come from: Datadog APM queries, an OTEL export, or a Datadog monitor
   webhook.
4. The Laya/Jev API: request and response formats, and whether feedback is sent to them or
   FIRE keeps the dataset and exports it.
5. `undefined` next to `pending`: whether `partial` names the middle grade more clearly.
6. Which Kinesis environment and stream the demo reads, and from where in the stream.
7. The `removed` ratio is absolute, so it misjudges nested fields. A child of an optional object
   is never required at the default `1.0`: with `customer` present in half the samples,
   `customer.email` reaches at most `0.5`, even when every `customer` carried it. A new empty
   array reports its element paths as removed: `lines: []` carries no `lines[*]` fields, so each
   required one is reported. Whether presence should be relative to the parent, as `optional` is
   in the generalizer ([Optional keys](shape-generalizer.md#optional-keys)).
8. A field that was never `null` and arrives `null` is a type change (`old: ["string"]`,
   `new: ["null"]`). Whether `null` should count as a type change, or as an absent value that
   only the `removed` rule judges.
