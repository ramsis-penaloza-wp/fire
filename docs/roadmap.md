# Roadmap: watching OMG end to end

The order of work for [ingest-drift-and-feedback.md](specs/ingest-drift-and-feedback.md)
(the spec). The spec says what each part must do. This roadmap cuts it into steps, one pull
request each, and says for every step what must work, how it is tested, and against what.

Phase 0 is done (repo rules and `config/systems/omg.yml`), and so are steps 1.1, 1.2, and 1.3.
Phases 1 to 4 keep the spec's numbering.

**Scope added here.** The tentacle intercepts everything that enters and leaves OMG, not only
Kinesis domain events: inbound HTTP, inbound external events, internal domain events, outbound
partner calls, and outbound AWS calls. It keeps fingerprints per endpoint (request and
response apart) and the trace logic: which operations each entry point leads to.

**Documents.** The spec links to these proposals, one per area. Each step's pull request turns
its section into the contract of the shipped code.

| Document | Steps |
|----------|-------|
| [observations.md](specs/observations.md): channels, observation models, ingest endpoint, catalog | 1.1, 1.2 |
| [tentacle.md](specs/tentacle.md): process, sender, record sources, envelope, canary | 1.3, 1.7, 1.8 |
| [http-and-span-capture.md](specs/http-and-span-capture.md): OTLP receiver, span allowlist, operation names, inbound proxy | 1.4, 1.5, 1.6 |
| [flows-and-paths.md](specs/flows-and-paths.md): trace assembly, flows, paths, path drift | 1.9, 2.3 |
| [dev/omg/README.md](../dev/omg/README.md): OMG's local stack with FIRE attached (L2) | 1.4, 1.6 to 1.8, 1.10, 2.6 |

## Rules for every step

- One step, one pull request, branched from `main`.
- The spec section a step implements is updated in the same pull request. When code and spec
  disagree, the spec is fixed first.
- New adapters get unit tests in `tests/unit/`. New pipeline behavior gets a test in
  `tests/integration/` with the `integration` marker.
- Code that touches a raw payload passes the canary test from step 1.3: no planted value
  reaches an observation, the body, or a log line.
- `make lint` and `make test` pass, and CI is green.
- Nothing changes in the OMG repository. Local wiring lives in FIRE under `dev/omg/`.

## Test levels

| Level | Runs against | Where | Data |
|-------|--------------|-------|------|
| L0 | Inputs built inside the test | CI | Made up in the test |
| L1 | Fixtures recorded from OMG's local stack or derived from OMG artifacts: the manifest, `openapi.json`, test payloads, the capture layout | CI | OMG's synthetic test data only, reviewed before commit; never from dev or staging |
| L2 | OMG's local stack (docker compose with the `omg`, `infrastructure`, and `otel` profiles, localstack) plus FIRE's override in `dev/omg/`; needs OMG's own prerequisites (AWS SSO, ECR access, a GitHub token), see [Prerequisites](../dev/omg/README.md#prerequisites) | A laptop; marker `omg_local`, skipped unless `OMG_REPO` is set | Traffic from OMG's own `make test-*` targets and payloads, and requests posted through the proxy |
| L3 | OMG dev on AWS, read only | A laptop with AWS credentials, by hand | Real dev traffic; only shapes leave the tentacle |

Locally, OMG's clients call partner **staging** services (`config/dev.yml`), so FIRE cannot
change a partner's response at L2. Outbound drift is tested at L1 with edited fixtures.
Inbound drift is tested live at L2.

## Interception map for OMG

What the tentacle reads, checked against `order-management-gateway`. Resource names are OMG's
logical names; the local names (for example `omg-dev-domain-events`) are listed in
[tentacle.md](specs/tentacle.md#migration).

`make test-order*` call the API container directly, so they produce spans but no inbound
shapes. Inbound shapes need requests posted through the proxy (step 1.6).

| Direction | Traffic | Read from | Gives | Body |
|-----------|---------|-----------|-------|------|
| Inbound | HTTP to the OMG API (`:8020`) and the validation engine (`:8030`) | A reverse proxy in the tentacle, in front of each service | Request and response shapes per route | Full |
| Inbound | The same requests as `SERVER` spans | OTLP from OMG's collector | Route, method, status, duration, trace | None |
| Inbound | External events: `rx_updates` (Kinesis), `carrier_events` and `financials_events` (SNS) | A second Kinesis reader; a FIRE-owned queue subscribed to each topic | Event shapes per event type | Full |
| Internal | Domain events | The DynamoDB stream of `order_domain_events` (every event) and the `domain_events` Kinesis stream (86 of 125 events) | Event shapes, `work_unit_id` | Full |
| Outbound | Partner calls from processors | S3 transmission captures: Helios, Monocle, Notification, Oracle OM, Order Services, Narvar, Insurance | Request and response shapes per partner action, `work_unit_id` | Full |
| Outbound | Every partner call as a `CLIENT` span | OTLP | Host, method, path, status, trace | Only for clients that opt in (Oracle OM, Order Services, CTO), cut at 4032 bytes |
| Outbound | AWS calls (DynamoDB, SNS, SQS, S3) as `CLIENT` spans | OTLP | Service, operation, resource, status, trace | None |

These joins make the trace logic possible:

- The proxy adds a W3C `traceparent` header to each request it forwards. OMG's `SERVER` span
  continues that trace, so the request's shape joins its trace.
- OMG's listener links its span to the span that wrote the event (`writer_traceparent`), so a
  flow continues across the async boundary from the API to the processors.
- A transmission capture adds a `transmission.capture` event to the active span with the S3
  keys of the pair, so a captured body joins its span.
- `work_unit_id` joins domain events, captures, and outbox rows.

These gaps are reported as `missing`, never guessed:

- A partner call made outside a processor, or by a client without capture, has a body only if
  its span carries one. A body cut at 4032 bytes is not generalized.
- Queues OMG reads directly (`money_outcomes`) cannot be read without taking messages from
  OMG; only their spans are seen.
- The proxy works locally only; inbound bodies on AWS are open question 7.

## Phase 1: ingest plane

**Exit:** OMG's local stack, driven by its `make test-*` targets, fills the catalog with
fingerprints per inbound route, per partner action, and per event type, and with the path of
the place-order flow. Spec: [Phase 1](specs/ingest-drift-and-feedback.md#phase-1-ingest-plane).

### 1.1 Observation contracts

**Spec.** [observations.md](specs/observations.md): [Channel](specs/observations.md#channel),
[ShapeObservation](specs/observations.md#shapeobservation),
[SpanObservation](specs/observations.md#spanobservation), and
[Stream key and deduplication](specs/observations.md#stream-key-and-deduplication). They
replace the main spec's event-only
[Observation](specs/ingest-drift-and-feedback.md#observation). In short:

- `Channel`: `kind` (`http`, `event`), `direction` (`inbound`, `internal`, `outbound`),
  `name` (route template, partner operation, or event type), `part` (`request`, `response`,
  `payload`), and `status_class` (`2xx` to `5xx`, or `none` for a call that got no response;
  responses only).
- `ShapeObservation`: today's fields with `channel` instead of `event_type`,
  `observation_id` instead of `event_id`, and optional `trace_id`, `span_id`, and `work_id`
  (the unit of work a payload belongs to; OMG maps its `work_unit_id` to it).
- `SpanObservation`: `trace_id`, `span_id`, `parent_span_id`, `links`, `service`, `kind`,
  `operation`, `status_code`, `error`, `start`, and `duration_ms`. No attribute values.
- The stream key becomes `(system, source, channel)`.

**Delivers.** The models in `fire/core/domain/observation.py`, and the `integration` and
`omg_local` pytest markers.

**Done when:**

- Both observation models reject unknown fields, so a `payload`, `body`, or `attributes` key
  fails validation.
- `Channel` renders one stable key, such as
  `http:inbound:POST /v1/orders/place_order:response:2xx`.

**Test.** L0: valid and invalid models, key rendering, and a JSON round trip.

### 1.2 Ingest endpoint and catalog

**Spec.** [Ingest endpoint](specs/observations.md#ingest-endpoint),
[ObservationCatalog](specs/observations.md#observationcatalog), and
[Streams endpoint](specs/observations.md#streams-endpoint).

**Delivers:**

- `POST /v0/observations` taking `{"shapes": [...], "spans": [...]}` with `X-FIRE-Token`.
- `sources[].id` in the system YAML model, so unknown sources can be rejected.
- Port `ObservationCatalog` and an in-memory adapter in `fire/adapters/memory/`.
  `ShapeCatalog`, `ShapeRecord`, and `OriginRegistry` are removed.
- `GET /v0/systems/{system}/streams` with counts per stream key. The full status comes in 2.1.

**Done when:**

- The response counts accepted, duplicate, and rejected observations, and each rejection says
  why.
- A repeated `(system, source, observation_id)` for shapes, or `(system, trace_id, span_id)`
  for spans, is a duplicate and changes nothing.
- An unknown system or source is rejected, and so is a shape whose recomputed fingerprint
  differs from the one sent.
- A missing or wrong token gives `401`, as on `/v0/diagnose`.

**Test.** L0 for the catalog. Integration with FastAPI's `TestClient` for the endpoint.

### 1.3 Tentacle skeleton, replay source, canary

**Spec.** [tentacle.md](specs/tentacle.md): [Process](specs/tentacle.md#process),
[Record pipeline](specs/tentacle.md#record-pipeline),
[Envelope](specs/tentacle.md#envelope), and [Canary harness](specs/tentacle.md#canary-harness).
Each source gets `kind`-specific keys and an `envelope`; `dedupe_key` goes away.

**Delivers:**

- `python -m fire.apps.tentacle --system omg`, which loads `config/systems/omg.yml`.
- Port `RecordSource` and the `ReplayRecordSource` adapter (`.json`, `.jsonl`, or a
  directory), with dotted envelope paths.
- A sender that batches, retries with backoff, and never drops silently: unsent batches are
  counted and logged.
- The canary harness: it plants unique strings in every value of a payload and asserts that
  none reaches the sender, the body's catalog, or captured logs.

**Done when:**

- Replaying OMG's four `WorkUnit` samples (`scripts/test_payloads/*_work_unit.json`) creates
  four event channels with one sample each.
- A record missing an envelope value is counted as rejected and not sent.
- The canary test passes for the replay source.

**Test.** L0 for envelope paths and batching. L1 against OMG's four samples, copied as
fixtures; with `OMG_REPO` set, the test also reads them from OMG. Canary.

### 1.4 OTLP receiver

**Spec.** [OTLP receiver](specs/http-and-span-capture.md#otlp-receiver) and
[Span mapping](specs/http-and-span-capture.md#span-mapping) (source kind `otlp`), and
[a. Start OMG with FIRE's override](../dev/omg/README.md#a-start-omg-with-fires-override-step-14).

**Delivers:**

- An OTLP/HTTP JSON receiver in the tentacle (`/v1/traces`). OMG's collector sends JSON
  (`encoding: json`), so FIRE needs no protobuf dependency.
- `dev/omg/`: a compose override that mounts a collector config with one more exporter
  (`otlphttp` to the tentacle), and a README with the commands.
- Spans become `SpanObservation`s through an attribute allowlist: route, method, status,
  service, `rpc.*`, and table or topic names. Everything else is dropped, including
  `url.full`, headers, and `http.*.body`.
- A complete JSON body on a `CLIENT` span (the request body attribute, or the response body
  span event) becomes a `ShapeObservation` for that outbound channel. A truncated body
  (`...[truncated`) is counted, not generalized.

**Done when:**

- At L2, `make test-order` delivers to the tentacle the `SERVER` span of
  `POST /v1/orders/place_order` and its child DynamoDB and SNS spans, all with one
  `trace_id`.
- A canary planted in the order payload appears in no observation.
- Jaeger still receives every trace: FIRE's exporter is added, not substituted.

**Test.** L0 for the allowlist and truncation. L1 with OTLP JSON recorded from
`make test-order`. L2 live.

### 1.5 Operation names

**Spec.** [Operation names](specs/http-and-span-capture.md#operation-names).

**Delivers:**

- Inbound: `METHOD http.route`, for example `POST /v1/orders/place_order`.
- Outbound HTTP: host plus path, with id-like segments (numbers, UUIDs, ULIDs, long hex)
  replaced by `{id}`. Per-host overrides go in the system YAML.
- AWS: `service.operation resource`, for example `DynamoDB.PutItem omg-dev-order_domain`.

**Done when:**

- Two L1 recordings of the same OMG targets give the same set of operation names: ids never
  create new operations.
- Every operation name passes the canary test.

**Test.** L0 table from raw paths to names. L1 stability across two recordings.

### 1.6 Inbound proxy

**Spec.** [Inbound proxy](specs/http-and-span-capture.md#inbound-proxy) (source kind
`http_proxy` with `listen_port`, `upstream_env`, and `openapi_path`).

**Delivers:**

- A reverse proxy in the tentacle in front of the OMG API (`:8021` to `:8020`) and the
  validation engine.
- Route templates loaded from the upstream's `/openapi.json`, so a concrete path maps to its
  template. A path that matches no template is counted, not sent.
- A `traceparent` header on each forwarded request, so the shapes join the trace from 1.4.
- Request and response shapes per route and status class.

**Done when:**

- The proxy is transparent: the status, headers, and body bytes OMG returns are identical with
  and without it, compared byte for byte.
- At L2, OMG's test payloads posted through the proxy produce request and response channels
  for `POST /v1/orders/place_order`, each joined to the trace of its `SERVER` span.
- The canary test passes.

**Test.** L0 against a fake FastAPI upstream. L1 template matching against OMG's
`openapi.json`. L2 posting `test_order_payload.json`, the Springfield payloads, and the RMA
payloads from `scripts/test_payloads/` through the proxy, as in
[c. Generate traffic](../dev/omg/README.md#c-generate-traffic).

### 1.7 Transmission captures

**Spec.** The `s3_capture` kind in [Kinds](specs/tentacle.md#kinds), and
[Capture events](specs/http-and-span-capture.md#capture-events) for the join to spans.

**Delivers:**

- `S3CaptureRecordSource`: reads pairs from OMG's transmission bucket (localstack locally),
  for example `requests/{work_unit_id}/{action}_{ts}.json` with the matching response.
- Channel name `{label}:{action}`, outbound. The request part has no status class; the
  response part takes the captured response's status class. `work_id` comes from the key.
- Layouts that differ by client are declared per prefix in the system YAML. An object that
  matches no layout is counted, not guessed.

**Done when:**

- At L2, processor targets that call a capturing partner produce outbound channels for that
  partner's actions, joined to their spans through the `transmission.capture` event.
  `make test-oracle-publisher` needs `PAYLOAD=` with a work unit the Oracle publisher
  consumes, and OMG commits none; the README picks the targets.
- A capture with no response (`status_code: null`) gives a request shape and a response
  channel with status class `none`.

**Test.** L1 with capture files in the bucket layout. L2 against localstack.

### 1.8 Event sources

**Spec.** The `kinesis`, `dynamodb_stream`, and `sns_fanout` kinds in
[Kinds](specs/tentacle.md#kinds).

**Delivers:**

- `KinesisRecordSource`, polling `GetRecords` per shard and region, for `domain_events` and
  `rx_updates`.
- `DynamoDbStreamRecordSource` for `order_domain_events`, which carries every domain event,
  including the 39 that never reach Kinesis.
- `SnsFanoutRecordSource`: a FIRE-owned queue subscribed to a topic (`carrier_events`,
  `financials_events`), so OMG's own queues lose nothing.
- Deduplication by `(system, source, observation_id)`: the same event read from two regions
  of one source is one observation. Two sources carrying the same event are separate
  observations, each with its own baseline.

**Done when:**

- At L2 with Kinesis enabled (`KINESIS_ENABLED=true` when OMG's local Terraform is applied),
  `make test-order` followed by the processor targets produces internal event channels from
  both the DynamoDB stream and Kinesis, and an event read from both regional streams counts
  once per source.
- OMG's processors still receive every message: their queues are untouched.

**Test.** L0 for deduplication and the envelope of each kind. L1 records per kind. L2 against
localstack. L3 read-only on `domain_events` in dev (open question 6).

### 1.9 Trace assembly and paths

**Spec.** [flows-and-paths.md](specs/flows-and-paths.md): trace assembly, entry, completion,
paths, shapes on flows, and the paths endpoint.

**Delivers:**

- In the body, spans grouped by `trace_id` and joined through span links, so one **flow**
  runs from an entry operation across the async hops.
- A **path** per entry operation: the operations its flows reach, with parent-child edges,
  counts, and error counts.
- Shape observations attached to their span, by `span_id` or, for captures and events, by
  `work_id`.
- `GET /v0/systems/{system}/paths`.

**Done when:**

- The recorded place-order flow gives a path from `POST /v1/orders/place_order` through the
  DynamoDB write, the listener, SNS, the processors, and their partner calls.
- A flow counts as complete only after `paths.settle_s` with no new spans, because async hops
  arrive late. An incomplete flow is not folded into the path.

**Test.** L0 synthetic span trees, including links and late spans. L1 the recorded
place-order trace. L2 live.

### 1.10 Phase 1 exit run

**Delivers.** One procedure in
[d. Phase 1 exit run](../dev/omg/README.md#d-phase-1-exit-run-step-110): start OMG with the
override, start the body and the tentacle, run `make test-order`,
`make test-order-springfield`, `make test-sales-order-processor`,
`make test-fulfillment-processor`, and a capturing partner target, then post OMG's order
payloads through the proxy for the inbound shapes.

**Done when:**

- `/streams` lists inbound routes with request and response fingerprints, partner actions,
  and event types.
- `/paths` shows the place-order flow.
- A canary order, with canary strings in every customer field, leaves no canary in
  `/streams`, `/paths`, or the logs.
- `audit_coverage` runs on the observed event types. Gaps are expected.

**Test.** L2 by hand, with the results in the pull request.

## Phase 2: drift

**Exit:** the spec's [demo](specs/ingest-drift-and-feedback.md#demo), extended to endpoints
and paths. Spec: [Phase 2](specs/ingest-drift-and-feedback.md#phase-2-drift).

### 2.1 Baselines

**Spec.** [Baseline](specs/ingest-drift-and-feedback.md#baseline).

**Delivers.** Per stream key, the sample count and the presence count of every
`(path, value_type)`. The `drift` block (`min_samples`, `required_ratio`) in the system
YAML. `/streams` shows each stream's status.

**Done when:**

- A stream below `min_samples` reports `baseline rebuilding` and never raises drift.
- A fingerprint seen before skips comparison.

**Test.** L0.

### 2.2 Shape drift

**Spec.** [Drift rules](specs/ingest-drift-and-feedback.md#drift-rules).

**Delivers.** The `added`, `type_changed`, and `removed` rules. One drift signal per
`(stream key, fingerprint)` that notifies once and is not folded into the baseline.
Optional-key variation is folded silently.

**Done when:**

- An optional key present in some payloads and absent in others raises no drift.
- The same drifted fingerprint seen 100 times is one signal with a count of 100.
- The rules behave the same on event, request, and response channels.

**Test.** L0 with one case per row of the spec's table. L1 with a baseline built from
variants of OMG's `test_order_payload.json` with optional fields toggled, then a copy with a
required field removed.

### 2.3 Path drift

**Spec.** [Path drift](specs/flows-and-paths.md#path-drift). The rules, next to the shape
rules:

| Change | Rule |
|--------|------|
| `hop_added` | A completed flow reaches an operation its path never reached |
| `hop_missing` | An operation reached by at least `required_ratio` of flows is absent from a completed flow |
| `hop_failing` | An operation with no errors in the baseline returns errors |

**Done when:**

- Synthetic and recorded flows with one hop removed or added produce one signal each.
- Incomplete flows produce none.

**Test.** L0. L1 with the recorded place-order trace minus its Oracle call.

### 2.4 Drift incidents

**Spec.** [Drift incidents](specs/ingest-drift-and-feedback.md#drift-incidents) and
[Changes to existing code](specs/ingest-drift-and-feedback.md#changes-to-existing-code):
`project_id` becomes `system`, `Incident` gains `trigger` (with `path_drift` next to
`shape_drift`), the echo label is fixed, and `/v0/diagnose` recomputes a client fingerprint.
[Process path in the dossier](specs/flows-and-paths.md#process-path-in-the-dossier) maps a
flow to `ProcessPath`.

**Delivers.** A drift signal opens an incident. The dossier carries `shape_drift`,
`path_drift`, `baseline`, and `coverage` fragments, and its process path comes from the flow.
`EchoReasoner` builds one possibility per drift fragment, and the console notifier prints it.

**Done when:**

- An integration test posts a baseline batch and a drifted batch to `/v0/observations` and
  captures exactly one console notification naming the channel and the diff.
- `/v0/diagnose` works with `system`.

**Test.** Integration with `TestClient` and captured stdout.

### 2.5 Coverage

**Spec.** [Coverage and contract](specs/ingest-drift-and-feedback.md#coverage-and-contract).

**Delivers.** `GET /v0/systems/{system}/coverage` covering events (manifest against
observed), inbound routes (`openapi.json` against observed), and the partner operations
observed. Contract drift stays optional.

**Done when:**

- Against OMG's manifest, observing only what `kinesis-publisher` consumes reports 39
  declared events unseen, as the spec measured.
- Against OMG's `openapi.json`, the routes never called are listed.

**Test.** L1 against copies of OMG's manifest and `openapi.json`.

### 2.6 Demo

**Delivers.** The demo procedure in
[e. Drift demo](../dev/omg/README.md#e-drift-demo-step-26) and the L1 drift corpora.

**Done when:**

- L2, inbound: OMG's order payloads posted through the proxy, with optional fields varied,
  warm the `POST /v1/orders/place_order` request stream and raise nothing. One payload with a
  removed field and a changed type then raises one notification naming the channel, the
  removed path, and the type change.
- L1, outbound: replaying a capture fixture whose Oracle response lost a field raises one
  notification for that partner action.
- L1, path: replaying the place-order trace without its Oracle call raises one path drift
  notification.

**Test.** L2 and L1 as above, with the notifications in the pull request.

## Phase 3: triggers

Spec: [Phase 3](specs/ingest-drift-and-feedback.md#phase-3-triggers).

### 3.1 Persistent catalog

**Delivers.** A SQLite adapter for `ObservationCatalog`: baselines, paths, drift signals, and
the `work_id` index survive a restart.

**Done when:**

- After a restart, a warm stream is still warm.
- The `work_id` index drops entries older than its retention window.

**Test.** One contract test suite that runs against both the memory and the SQLite adapter.

### 3.2 Incident correlation

**Delivers.** Triggers for one system inside one window collapse into one incident whose
dossier merges their fragments.

**Done when.** Two drift signals and a 5xx burst within one window give one incident; the
same signals in separate windows give separate incidents.

**Test.** L0.

### 3.3 5xx bursts

**Spec.** The source is the tentacle's `SERVER` spans, which answers open question 3 locally.
`triggers.http_errors` (`status_min`, `threshold`, `window_s`) goes in the system YAML.

**Done when:**

- One error fewer than `threshold` within the window raises nothing; `threshold` errors raise
  one incident naming the route.
- Errors on two different routes do not add up.
- The dossier carries the failing flows' path and the fingerprints of the failing requests
  next to the route's baseline.

**Test.** L0 with synthetic spans at the window and threshold edges. L2 by making the API
fail, for example stopping localstack while posting through the proxy in a loop.

### 3.4 incident.io webhook

**Spec.** `POST /v0/webhooks/incident-io`, verified with the signing secret, mapped to a
system as open question 2 decides.

**Done when:**

- A validly signed sample opens an incident with `trigger: incident_io`.
- A bad signature gives `401`.
- An incident that maps to no system is accepted, counted, and ignored.

**Test.** L0 with incident.io's documented sample payloads signed with a test secret. L3 with
a test incident once a sandbox is available.

### 3.5 DLQ attribution

**Spec.** `ABANDONED` outbox rows read through a `dynamodb_stream` source filtered on status,
joined to observations on `work_id` (OMG's `work_unit_id`). Per fingerprint: observed and
abandoned counts.
`triggers.dlq` in the system YAML sets when a fingerprint's abandon rate opens an incident.

**Done when:**

- A fingerprint with a high abandon rate appears as a `dlq_attribution` fragment.
- An abandoned work unit whose event no source reads is reported as `missing`.

**Test.** L0 for the join and the rates. L1 with recorded outbox rows. L2 once open question
11 says how to force an `ABANDONED` task locally.

### 3.6 Slack notifier

**Spec.** Posts to `notify.slack.channel_id` of the incident's system.

**Done when.** The `chat.postMessage` request carries the system's channel id and the
narration.

**Test.** L0 with an `httpx` mock transport. L3 to a test channel. The real channel waits on
open question 1.

## Phase 4: decision ledger and feedback

Spec: [Phase 4](specs/ingest-drift-and-feedback.md#phase-4-decision-ledger-and-feedback).

### 4.1 Decision ledger

**Delivers.** Port `DecisionLedger` with memory and SQLite adapters: `decision_id`, the
incident, the dossier as the reasoner saw it, the `Decision`, and the reasoner name and
version. `GET /v0/decisions/{decision_id}`.

**Done when.** Every diagnosis stores exactly one decision, starting `pending`.

**Test.** L0 and integration.

### 4.2 Feedback endpoint

**Delivers.** `POST /v0/decisions/{decision_id}/feedback` with `success`, `failure`, or
`undefined` (open question 5), an optional `possibility_id`, and a note. The history keeps
every grade. An export of graded decisions as JSONL.

**Done when:**

- An unknown decision gives `404`; an unknown outcome gives `422`.
- A second grade replaces the current one and the history keeps both.
- The export holds the dossier, the decision, and the outcome, and passes the canary test.

**Test.** Integration.

### 4.3 Drift acknowledgement

**Delivers.** Acknowledging a drift signal folds its fingerprint, or its hop, into the
baseline, as the spec defers to this phase.

**Done when.** After acknowledgement, the same fingerprint raises nothing and later payloads
compare against the new baseline.

**Test.** L0.

### 4.4 Laya/Jev reasoner

**Delivers.** A `Reasoner` adapter over HTTP (`FIRE_SYSTEMONE_BASE_URL`), selected by
`reasoner.provider` in `config/fire.yml`. A timeout or error gives a `noul` decision with the
reasoner fragment `missing`, never an invented one.

**Done when.** Contract tests pass against a FIRE-side fake server that implements the agreed
request and response.

**Test.** L0 contract tests. L3 against the real endpoint once open question 4 is answered.

### 4.5 Feedback from Slack

**Delivers.** Three buttons on the Slack message. Slack's interactivity request, verified
with the signing secret, calls the feedback endpoint.

**Done when.** A signed button payload grades the decision; an unsigned one gives `401`.

**Test.** L0 with signed sample payloads. L3 in a test channel.

## Open questions by step

Questions 1 to 6 are the main spec's. None of them blocks phase 1 at L0 to L2, or phase 2.
Each document in the table at the top lists its own open questions about details inside its
steps.

| # | Question | Blocks |
|---|----------|--------|
| 1 | OMG's Slack channel id | 3.6, real channel only |
| 2 | How an incident.io incident maps to a system | 3.4 |
| 3 | Where 5xx counts come from on AWS (locally: `SERVER` spans) | 3.3 at L3 |
| 4 | The Laya/Jev API and where feedback goes | 4.4 |
| 5 | `undefined` or `partial` for the middle grade | 4.2 |
| 6 | Which Kinesis environment and stream L3 reads | 1.8 at L3 |
| 7 | Inbound bodies on AWS, where the proxy cannot run: an OMG middleware that puts request bodies on `SERVER` spans, as its httpx hooks already do on `CLIENT` spans, or another tap | Phase 1 at L3 |
| 8 | How FIRE receives spans on AWS: a second exporter in OMG's collector, or pulling from Datadog or New Relic | 1.4 at L3 |
| 9 | DynamoDB streams allow two readers per shard and OMG's listener is one | 1.8 at L3 |
| 10 | Who approves FIRE-owned SNS subscriptions in OMG's AWS accounts | 1.8 at L3 |
| 11 | How to force an `ABANDONED` task in OMG's local stack | 3.5 at L2 |
