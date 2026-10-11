# Ingest, drift, and feedback

**Status: phase 0 is done; phases 1 to 4 are a proposal.** This is the route from today's
engine, which only diagnoses what a caller posts to `POST /v0/diagnose`, to an engine that
watches a system on its own, detects shape drift, diagnoses incidents, and learns from
feedback.

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
  `ShapeFingerprinter` adapters and sends `Observation`s over HTTP with `X-FIRE-Token`.
- The body never sees a payload. It recomputes the fingerprint from the observation's shape
  and rejects the observation on a mismatch.
- A durable buffer between tentacle and body (SQS, Kinesis) replaces HTTP once FIRE runs
  outside a laptop. The `Observation` contract does not change.

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
- `scripts/test_payloads/` holds four `WorkUnit` samples (snake_case, internal form). The
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

### Observation

`fire/core/domain/observation.py`. One payload seen by the tentacle, without its values.
Unknown fields are rejected.

| Field | Type | Meaning |
|-------|------|---------|
| `system` | `str` | System id; must be a loaded `config/systems/<system>.yml` |
| `source` | `str` | Source id from the system YAML |
| `event_type` | `str` | The record's event type |
| `event_id` | `str` | The record's unique id; deduplication and DLQ joins use it |
| `correlation_id` | `str \| None` | Set only when the source carries one |
| `region` | `str \| None` | Region of the stream it was read from |
| `occurred_at` | `datetime` | When the system says the event happened |
| `observed_at` | `datetime` | When the tentacle read it |
| `shape` | `Shape` | From `FlatStarShapeGeneralizer` |
| `fingerprint` | `ShapeFingerprint` | From `Sha256ShapeFingerprinter` |

The stream key of an observation is `(system, source, event_type)`. Baselines, drift, and
coverage are per stream key, so a replay source and a Kinesis source never share a baseline
even when they carry the same events in different casing.

### Sources in the system YAML

Each source gets an `id` and an `envelope` that says where the tentacle finds each envelope
value in a record. Paths are dotted. `envelope.event_id` replaces `dedupe_key`.

```yaml
sources:
  - id: domain-events
    kind: kinesis
    stream_env: OMG_EVENTS_STREAM
    regions: [us-east-1, us-west-2]
    envelope:
      event_id: id
      event_type: eventType
      occurred_at: occurredAt
      payload: payload
  - id: work-unit-samples
    kind: replay
    path_env: OMG_REPLAY_PATH          # a .json, .jsonl, or a directory of them
    envelope:
      event_id: work_unit_id
      event_type: event_type
      occurred_at: metadata.timestamp
      correlation_id: metadata.correlation_id
      payload: payload
```

A record missing a required envelope value is counted as rejected per source and never sent.

### Tentacle

`python -m fire.apps.tentacle --system omg [--source work-unit-samples]`

- Port `RecordSource` (`fire/core/ports/record_source.py`): an async iterator of raw records
  with the source id and region. Adapters: `ReplayRecordSource` and `KinesisRecordSource`
  (polling `GetRecords` per shard and region; adds an AWS SDK dependency).
- Per record: read the envelope, generalize the payload, fingerprint the shape, build the
  `Observation`, and batch. Batches go to `POST /v0/observations`.
- `fingerprinting.sample_rate` is applied here. DLQ attribution (phase 3) needs every
  event's fingerprint, so OMG uses `1.0`.

### Ingest endpoint and catalog

- `POST /v0/observations` takes `{"observations": [...]}`, requires `X-FIRE-Token`, and
  returns `202` with accepted, duplicate, and rejected counts. An unknown `system` or
  `source` is rejected per observation.
- Port `ObservationCatalog` replaces `ShapeCatalog`. It stores per stream key the baseline
  (below) and, per `event_id`, the fingerprint and `observed_at` for a retention window. The
  `event_id` index makes ingest idempotent, so several tentacles or both regions are safe.
- First adapter: in memory (`fire/adapters/memory/`). A restart loses the baselines and the
  engine reports them as rebuilding. SQLite comes after the demo.

## Phase 2: drift

### Baseline

Per stream key: the number of observations folded in (`samples`), and for every
`(path, value_type)` seen, how many observations carried it. A fingerprint seen before
skips comparison.

Until `samples` reaches `drift.min_samples`, the stream's status is `baseline rebuilding`
and it raises no drift. This is the honest-degradation rule: no baseline, no drift claims.

### Drift rules

A new fingerprint on a warm stream is compared with the baseline:

| Change | Rule | Reported as |
|--------|------|-------------|
| Field added | `(path, value_type)` never seen on this stream | `added` |
| Type changed | `path` seen before, but never with this `value_type` | `type_changed` (old and new types) |
| Field removed | carried by at least `drift.required_ratio` of samples, absent now | `removed` |

Comparing fingerprints alone is not enough: a payload with an optional key present and one
without it have different fingerprints but the same form. A new fingerprint with no added,
changed, or removed field is folded into the baseline silently.

A drifted fingerprint is not folded into the baseline. It becomes one drift signal per
`(stream key, fingerprint)` with a first-seen time and an occurrence count, and it notifies
once. Acknowledging a drift so it joins the baseline is phase 4 feedback.

```yaml
drift:
  min_samples: 20
  required_ratio: 1.0
```

### Drift incidents

A drift signal opens an `Incident` with `trigger: shape_drift`. `Incident` gains a `trigger`
field (`shape_drift`, `incident_io`, `http_errors`, `manual`); `POST /v0/diagnose` becomes
`manual`. The dossier carries these fragments:

| Collector | Data |
|-----------|------|
| `shape_drift` | Stream key, the diff (added, type_changed, removed paths), occurrences, first seen |
| `baseline` | Samples, status, fingerprints known |
| `coverage` | The system's `CoverageReport` from observed event types |

`EchoReasoner` builds one possibility per drift fragment. The notifier stays console for
the demo.

### Coverage and contract

- `audit_coverage` runs with the catalog's observed event types per system, on each drift
  incident and on `GET /v0/systems/{system}/coverage`.
- Contract drift (optional for the demo): compare an observation's top-level payload keys
  with the manifest's declared fields. Casing differs, so the system YAML declares the wire
  casing (`discovery.manifest.wire_case: camel`). Only names are compared; Python type names
  do not map to `ValueType`.

### Read endpoints

`GET /v0/systems/{system}/streams` lists each stream key with its status, samples, and open
drift signals. Both read endpoints require `X-FIRE-Token`.

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

| Today | Proposed |
|-------|----------|
| `project_id` on `Incident`, `Dossier`, `DiagnoseRequest` | `system`, validated against loaded systems, so the notifier can pick the Slack channel |
| `ShapeCatalog`, `ShapeRecord` (no adapter) | `ObservationCatalog` and a baseline per stream key |
| `OriginRegistry` (no adapter) | Removed until a source needs origins |
| `sources[].dedupe_key` | `sources[].id` and `sources[].envelope`; new `kind: replay` |
| `fingerprinting.sample_rate` validated only | Applied in the tentacle |
| A client `fingerprint` on `/v0/diagnose` kept as is | Recomputed when a `shape` is present; a mismatch is rejected, as for observations |
| `EchoReasoner` says "No path/shape evidence yet" when a shape is present | Possibilities from drift fragments; label fixed |

## Open questions

1. The real Slack channel id for OMG.
2. How an incident.io incident maps to a system (service, catalog entry, or custom field).
3. Where 5xx counts come from: Datadog APM queries, an OTEL export, or a Datadog monitor
   webhook.
4. The Laya/Jev API: request and response formats, and whether feedback is sent to them or
   FIRE keeps the dataset and exports it.
5. `undefined` next to `pending`: whether `partial` names the middle grade more clearly.
6. Which Kinesis environment and stream the demo reads, and from where in the stream.
