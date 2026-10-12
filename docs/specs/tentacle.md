# Tentacle

The tentacle (`fire.apps.tentacle`) is the only FIRE process that touches a watched system's
raw payloads. It reads records from the sources a system YAML declares, turns each payload into
a `Shape` and a `ShapeFingerprint`, and sends `ShapeObservation`s to the body
(`fire.apps.incident_engine`). No raw payload leaves the tentacle process. This document
supersedes the [Tentacle](ingest-drift-and-feedback.md#tentacle) and
[Sources in the system YAML](ingest-drift-and-feedback.md#sources-in-the-system-yaml) sections
of the main spec and the [Sources](system-config-and-discovery.md#sources) section of the
config spec. OMG is the example throughout; no code names it.

**Status:** roadmap steps 1.3, 1.6, 1.7, and 1.8 implement it. [Process](#process),
[Sending to the body](#sending-to-the-body), [Record pipeline](#record-pipeline),
[Sources in the system YAML](#sources-in-the-system-yaml), [Envelope](#envelope),
[Privacy](#privacy), and [Canary harness](#canary-harness) are the contract of the shipped code
(step 1.3), and so are the `replay` kind and the `http_proxy` kind (step 1.6,
[Inbound proxy](http-and-span-capture.md#inbound-proxy)). The other kinds load and validate in
the system YAML, but their readers, described in [Kinds](#kinds), are a proposal until steps 1.4,
1.7, and 1.8.

| Module | Holds |
|--------|-------|
| `fire/apps/tentacle/` | `__main__.py` (the CLI), `runner.py` (startup and the run loop), `sender.py`, `counters.py`, `sources.py` (readers by kind, and `open_http_proxy`) |
| `fire/core/ports/record_source.py` | `RecordSource`, `RawRecord`, `StartPosition` |
| `fire/core/services/record_pipeline.py` | `RecordPipeline` and its reason codes |
| `fire/core/domain/envelope.py` | `Envelope` and its path models |
| `fire/adapters/sources/replay.py` | `ReplayRecordSource` |
| `fire/apps/config.py` | `TentacleConfig` and one model per source kind |

The inbound proxy's modules are listed in [Inbound proxy](http-and-span-capture.md#inbound-proxy).

## Process

```
python -m fire.apps.tentacle --system omg [--source <id>] [--start latest|oldest]
```

- It loads `.env` (`load_fire_env`), `config/fire.yml`, and `config/systems/<system>.yml`; no
  manifest. It runs every source, or the one `--source` names. A source whose env var is unset,
  or whose kind has no reader yet, is logged as skipped with that reason, unless `--source`
  names it.
- Startup errors: a config that fails to load, a `--system` that is not a system id or has no
  file, an unknown `--source`, an unset `api.token_env` or `tentacle.body_url_env`, a body URL
  that is not `http` or `https`, a named source that would be skipped, a source whose env var
  names nothing it can read (a `replay` path that is no `.json` or `.jsonl` file and no
  directory, or an `http_proxy` upstream that is not an `http` or `https` URL), an
  `http_proxy` whose listen address cannot be bound, and no source left to run. Messages name
  env vars and listen addresses, never env var values.
- A `replay` source ends when its files are read; live sources, `http_proxy` included, run
  until `SIGINT` or `SIGTERM`. `--start latest` (the default) reads what is written after
  start; `oldest` reads what a source still retains; `replay` and `http_proxy` ignore it.
  Positions live in memory, so a restart begins at `--start` and the body's dedupe absorbs the
  overlap.
- When every source has ended, or on a signal, it stops reading, sends what is pending within
  `shutdown_s`, counts what is still pending as unsent, and logs its counters. Exit code `0`:
  every record was sent, skipped, or rejected with a counted reason; `1`: some observation is
  unsent, or a source failed (logged by exception type); `2`: config or startup error,
  including bad arguments.

### Sending to the body

`config/fire.yml` gains a `tentacle` block:

| Key | Default | Meaning |
|-----|---------|---------|
| `body_url_env` | `FIRE_BODY_URL` | Env var holding the body's base URL, such as `http://127.0.0.1:8000` |
| `batch_size`, `flush_ms` | `200`, `1000` | Observations per request; longest a partial batch waits |
| `buffer_size` | `10000` | Observations held before reading pauses |
| `retry.max_attempts`, `retry.backoff_ms` | `6`, `500` | Attempts per batch; first backoff, doubled per attempt up to 30 s, with full jitter |
| `shutdown_s` | `10` | Time to drain on shutdown |

- Batches go to `{body URL}/v0/observations` with `X-FIRE-Token`
  ([observations.md](observations.md#ingest-endpoint)), with a 10 s request timeout. Each
  observation is posted whole, nulls included: the field of a JSON null has a null
  `cardinality`, and dropping it would let the body read the default `"1"` and refuse the
  shape as `fingerprint_mismatch`. A batch
  holds one source's observations, so the body's counts are that source's. A connection
  error, a timeout, `408`, `429`, or a `5xx` is retried; any other status, including `401`, is
  not.
- A batch that runs out of attempts or is refused is counted as unsent and logged as
  `batch unsent: source=<id> size=<n> status=<HTTP status or exception type> attempts=<n>`. A
  full buffer pauses reading instead of dropping. An `http_proxy` cannot pause the traffic it
  forwards, so its observation that finds the buffer full is `skipped: buffer_full`.
- A source acknowledges a record (a queue deletes the message) only after the body answered
  `202` for the batch that carries it. A skipped or rejected record is acknowledged at once,
  since no batch will carry it. A failing ack is logged by exception type and stops nothing.

Counters, per source, are logged every 60 s and at exit as one line of numbers and reason
codes: `read` (an `s3_capture` record is one pair, an `http_proxy` record one exchange),
`built`, `sent` (in batches answered `202`), `unsent`, `pending`, `rejected` and `skipped` by
the reasons below, `noted` events that build and drop nothing (an `http_proxy`'s
`invalid_traceparent` and `templates_refresh_failed`), and the body's `duplicate` and
`rejected` counts by its reasons. `sent + unsent + pending = built` at all times. Reasons are
`reason:count` pairs sorted by reason, or `-`:

```
tentacle counters: source=work-unit-samples read=21 built=4 sent=4 unsent=0 pending=0
  rejected=missing_event_id:17 skipped=- noted=- body_duplicate=0 body_rejected=-
```

## Record pipeline

Port `RecordSource` (`fire/core/ports/record_source.py`): `records()` is an async iterator of
`RawRecord`s, each holding the source id, the region it was read from (`None` when the source
has none), the decoded record (`None` when the wire form did not decode to a JSON object), and
an async `ack` the sender calls once the body accepts it; its `repr` omits the record and the
ack. Adapters live in `fire/adapters/sources/`, and `READERS` in `fire/apps/tentacle/sources.py`
maps each kind to the one that opens it. AWS adapters call `boto3` through
`asyncio.to_thread`, with endpoints and credentials from the standard AWS environment
(`AWS_ENDPOINT_URL` for localstack), never from FIRE config.

`RecordPipeline` (`fire/core/services/record_pipeline.py`) runs one source's records through
these steps; the first failing step decides.

1. **Decode** the wire form into a JSON object, as each kind says; the adapter does this. A
   record with no object: `undecodable`.
2. **Filter**, for a `dynamodb_stream` with `filter` (step 1.8). No match: `skipped: filtered`.
3. **Envelope**, in key order: `event_id`, `event_type`, `occurred_at`, `payload`, `work_id`,
   `correlation_id`. An absent or `null` required value is `missing_<key>`; an absent optional
   one leaves its field `null`. Ids must be non-empty strings, or the record is `bad_id`.
   `event_type` must match `^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$` in full, or the record is
   `bad_name`. `occurred_at` is ISO 8601, read as UTC without an offset, or with `epoch` an
   integer in that unit; anything else is `bad_timestamp`. The payload may be any JSON value
   but `null`.
4. **Sample.** Kept when the first 8 bytes of SHA-256 of `observation_id`, as an integer over
   2^64, fall below `fingerprinting.sample_rate`; else `skipped: sampled_out`. Every region and
   source makes the same choice for one event, so joins stay whole.
5. **Local dedupe.** A repeat of one of the source's last 100 000 observation ids is
   `skipped: local_duplicate`. This saves work only; the body is authoritative.
6. **Generalize** with `FlatStarShapeGeneralizer`, **fingerprint** with
   `Sha256ShapeFingerprinter`, and **build** the `ShapeObservation`: `observed_at` is the read
   time, the channel `event:{direction}:{event_type}:payload` (`s3_capture` has its own, below).
   A failure here is `invalid`: an id longer than 256 characters, a string that is not valid
   Unicode (a lone surrogate), or a payload nested too deep to generalize. The sender gets the
   observation; nothing keeps the record.

## Sources in the system YAML

`sources` is a list discriminated by `kind` (`SourceConfig` in `fire/apps/config.py`).
Unknown keys are rejected at every level, so an error's location names the kind:
`sources.0.kinesis.regions`. An unknown `kind` fails at `sources.0`.

| Key | Kinds | Meaning |
|-----|-------|---------|
| `id` | all | Unique within the system, `^[a-z0-9][a-z0-9_-]*$`; the observations' `source` |
| `kind` | all | See [Kinds](#kinds) |
| `direction` | event-carrying | `inbound` for external events, `internal` for the system's own; `outbound` is rejected |
| `envelope` | event-carrying | Where each envelope value is in a record |

The event-carrying kinds are `replay`, `kinesis`, `dynamodb_stream`, and `sns_fanout`
(`EventSourceConfig`); `s3_capture` takes neither key. A `*_env` key names an env var holding a
path or a resource name, never a credential, and each one appears in `.env.example`. Each
model's `location_env` names that key's env var.

### Envelope

| Key | Required | Becomes |
|-----|----------|---------|
| `event_id` | yes | `observation_id` |
| `event_type` | yes | The channel name |
| `occurred_at` | yes | `occurred_at` |
| `payload` | yes | The value generalized into `shape` |
| `work_id`, `correlation_id` | no | The fields of the same name |

`Envelope` (`fire/core/domain/envelope.py`) rejects unknown keys.

- A path is object keys joined by `.`, such as `metadata.timestamp`; no empty key, and no `[`,
  `]`, or `*`, so no indexes or wildcards. A key that is absent, or a parent that is not an
  object, reads as absent.
- The short form is the path; the long form is `{path: ...}` plus options. On ids, names, and
  `occurred_at`, `{path: SK, strip_prefix: "EVENT#"}` removes a literal prefix; a value without
  it, or that is not a string, counts as missing. For `occurred_at` only, `{path: ts, epoch:
  ms}` (or `s`) reads an integer, and takes no `strip_prefix`. `payload` takes only `path`.
- `envelope.event_id` replaces `dedupe_key`. One event read from two regions of one source is
  one observation; one event read by two sources is two, each with its own baseline.
- `replay`, `kinesis`, `dynamodb_stream`, and `sns_fanout` set no `trace_id` or `span_id`, so
  their shapes join flows by `work_id` ([flows-and-paths.md](flows-and-paths.md); open
  question 2). `s3_capture` sets them when it can, see below.

### Kinds

| `kind` | Keys | Record |
|--------|------|--------|
| `replay` | `path_env` | One JSON object from a file |
| `kinesis` | `stream_env`, `regions` | One record's data, decoded as JSON |
| `dynamodb_stream` | `table_env`, `regions`, `filter` (optional) | One stream record's new image, as plain JSON |
| `sns_fanout` | `topic_env`, `regions` | One SNS notification, with `Message` decoded |
| `s3_capture` | `bucket_env`, `captures` | One request and response pair |
| `http_proxy` | `listen_port`, `upstream_env`, and more in [Inbound proxy](http-and-span-capture.md#inbound-proxy) | One HTTP exchange the proxy forwarded |
| `otlp` | [OTLP receiver](http-and-span-capture.md#otlp-receiver) | |

`regions` (one or more) are read in parallel. A record's region is the one it was read from,
never one named inside the record. [Migration](#migration) shows each kind in OMG's file.
`replay` has a reader (step 1.3), and `http_proxy` an opener, `open_http_proxy` (step 1.6),
which serves instead of reading: it reports each exchange to the runner, which shapes it with
`ExchangePipeline` instead of `RecordPipeline`. The tentacle skips the other kinds, as
[Process](#process) says.

**`replay`** (`ReplayRecordSource`). `path_env` holds a `.json` file (an object or a list), a
`.jsonl` file, or a directory of them, read in name order and not recursively; other files and
subdirectories are ignored. Each object is a record. An element or line that is not an object,
and a file or line that is not JSON or not UTF-8, is one `undecodable` record. Blank `.jsonl`
lines are not records. Replay records have no region, and their ack does nothing.

**`kinesis`.** Polls `GetRecords` per shard and region, following shard splits and merges.
`stream_env` holds the stream name, the same in every region. OMG's `domain_events` records
are `DomainEvent`s whose `id` is the `work_unit_id`; kinesis-publisher runs in every region, so
one `id` arrives from both streams. `rx_updates` records carry `ts` in epoch milliseconds
(`scripts/kinesis/payloads/`).

**`dynamodb_stream`.** Per region, finds the table's latest stream with `DescribeTable`.
`INSERT` and `MODIFY` records are read from their new image; `REMOVE` records are
`skipped: remove_record`; a view type without the new image fails the source. Attribute values
become plain JSON: an `N` is an integer when its text has no `.` or exponent, else a number;
sets become arrays; `B` and `BS` stay bytes, typed `unknown`. `filter: {attribute: status,
equals: ABANDONED}` keeps records whose new image holds that string at that path; phase 3
reads OMG's outbox rows this way. A stream serves at most two readers per shard and OMG's
listener is one, so one tentacle reads each stream and region (roadmap open question 9).

OMG's `order_domain_events` items are `EventItem`s (`adapters/src/dynamo/domain/events.py`)
with `PK` `ORDER#<id>` and `SK` `EVENT#<ULID>`; the ULID is the work unit's `work_unit_id`.
The local table's stream view type is to be confirmed (its test twin uses `NEW_AND_OLD_IMAGES`).

**`sns_fanout`.** Per region, the tentacle subscribes a queue it owns to the topic, so the
system's own subscriptions lose nothing. `topic_env` holds the topic name; the ARN is built
from the region and the caller's account. The queue `fire-{system}-{source}` is created if
absent, with a policy that lets only that topic send, and subscribed without raw delivery. The
tentacle never deletes it, so a restart loses nothing ([dev/omg/README.md](../../dev/omg/README.md)
says how to remove it). A record is the notification (`MessageId`, `Timestamp`, `Message`, …)
with `Message` decoded when it is JSON. Standard topics only; a `.fifo` topic fails the source.
Subscribing writes to the system's AWS account, so at L3 this kind waits on roadmap open
question 10. A queue the system reads directly, such as OMG's `money_outcomes`, is not a
source: reading it takes messages from the system.

**`s3_capture`.** Reads the request and response pairs a system's clients write to a bucket.
`bucket_env` holds the bucket name. Each `captures` entry (one or more) gives a client's key
`prefix` (unique within the source), the `label` that starts its channel names
(`^[a-z0-9][a-z0-9_-]*$`), and its `layout`.

- Every 5 s the source lists each prefix's `responses/` and reads each new response once it is
  10 s old, so both halves have landed, with the request of the same stem. An object matching
  no layout is `no_layout`, never guessed; `index/` markers are `skipped: index_marker`.
- A pair gives `http:outbound:{label}:{action}:request` and
  `http:outbound:{label}:{action}:response:{status_class}`. `work_id` is the key's work unit and
  `occurred_at` its timestamp. The pair id `{work_id}:{action}:{ts}` replaces the envelope and
  is what step 4 samples; `observation_id` is `{pair id}:{part}`.
- The OTLP receiver indexes capture object keys to `(trace_id, span_id)`
  ([Capture events](http-and-span-capture.md#capture-events)). When a pair's key is in that
  index, both shapes carry those ids; when it is not (no `otlp` source, or the span has not
  arrived), they carry neither and join by `work_id`. The keys never leave the tentacle.
- Only `body` is generalized; `method`, `url`, `headers`, `error_type`, and `error` are never
  read past decoding. `status_code: null` gives status class `none` and an empty shape.

| `layout` | Key under the prefix | Action from | OMG clients |
|----------|----------------------|-------------|-------------|
| `work_unit_action` | `{requests,responses}/{work_unit_id}/{action}_{ts}.json` | The key | Narvar, Insurance, Monocle; Notification, whose channel is the action |
| `work_unit_order_action` | `…/{work_unit_id}/{order_id}_{action}_{ts}.json` | Object metadata `action` | Oracle OM |
| `work_unit_order` | `…/{work_unit_id}/{order_id}_{ts}.json` | Object metadata `action`; Order Services writes none | Helios, Order Services |

`{ts}` is `%Y%m%d_%H%M%S_%f` in UTC (`adapters/src/clients/transmission_capture.py`). Nothing
is taken from an `{order_id}` segment: it is a value. Oracle files `unattributed` and Helios an
execution id when no work unit is bound; step 1.7 maps those to no `work_id`.

## Privacy

Only these leave the tentacle: shapes, fingerprints, ids (observation, trace, span, work,
correlation), names (channel, operation, service), status codes, timestamps, durations, and
counts. Never values, URLs whose path or query carries values, headers, or bodies.

- The sender posts the models of [observations.md](observations.md) and nothing else. Ids and
  names come only from declared envelope paths, layout segments, or config.
- Logs hold source ids, counters, reason codes, channel keys, and exception type names. Never
  a record, a payload, an S3 key, a partition or sort key, a message, or the text of an
  exception raised while a record is in hand: a Pydantic `ValidationError` quotes its input.
- Shape paths are payload keys, so a map keyed by data puts values into paths
  ([observations.md](observations.md#open-questions), question 1). The canary harness plants
  values, not keys, and does not catch this.

### Canary harness

`Canary` in `tests/canary.py`, used through the `canary` fixture of `tests/conftest.py`. Every
source kind with a reader passes it; `tests/integration/test_tentacle_canary.py` runs it.

- `plant(value)` replaces every leaf with a unique canary: strings plain, as an email, or as a
  URI, so the string refinements run; numbers as unique integers above 10^12. Booleans and
  nulls stay, since they cannot be made unique. Keys stay.
- `WIRE_FORMS` holds one wire form per kind: it writes records as that kind reads them and
  returns the value of the kind's location env var. `replay` writes a `.json` list, one `.json`
  file per record, and a `.jsonl` file that also holds broken lines. Each kind's test plants
  every other field too: the rest of the record, DynamoDB keys, SNS attributes, a capture's
  `url`, `headers`, and `error`, and an S3 key's order id. Envelope values get valid unique ids
  (`envelope_id`), names (`envelope_name`), and timestamps (`envelope_timestamp`), and must
  appear only in their own fields: `observation_id`, `work_id`, `correlation_id`,
  `occurred_at`, and `channel.name`. The envelope values of a rejected record (`rejected`) must
  appear nowhere.
- The test runs source, pipeline, and sender against the body's app through
  `httpx.ASGITransport`, capturing every logger at `DEBUG`, stdout, and stderr, with valid,
  rejected, undecodable, and duplicate records. It fails if a canary appears in a request the
  sender posted, in the catalog's state or `/streams` (where only names may appear, in channel
  keys), or in the captured output. A kind in `READERS` without a wire form fails a test.
- `http_proxy` has no wire form: `tests/integration/test_tentacle_proxy.py` posts through the
  proxy, with canaries in the path, the query, headers, a cookie, a `Content-Type` parameter,
  an invalid `traceparent`, and both bodies, and checks the same places.

## Migration

`dedupe_key` is removed; a file that still has it fails to load with a message naming
`envelope.event_id`. Step 1.2 made `id` required ([observations.md](observations.md#migration));
step 1.3 added the rest: every kind's keys, `direction`, and `envelope`. `_template.yml`
declares one `kinesis` source in the new form, `config/systems/omg.yml` holds the listing
below, and `.env.example` gained `FIRE_BODY_URL` and every env var below. Locally they name
`omg-dev-domain-events`, `omg-dev-order_domain_events`, `rx-updates`,
`omg-carrier-events-topic-dev`, `omg-financials-topic-dev`, and
`order-management-gateway-transmission-data-dev` ([dev/omg/README.md](../../dev/omg/README.md)).
Elsewhere this document uses OMG's logical names (`domain_events`, `carrier_events`, …). OMG's
`sources`:

```yaml
sources:
  - {id: omg-api, kind: http_proxy, listen_port: 8120, upstream_env: OMG_API_UPSTREAM}
  - {id: omg-validation, kind: http_proxy, listen_port: 8130,
     upstream_env: OMG_VALIDATION_UPSTREAM}
  - id: domain-events
    kind: kinesis
    direction: internal
    stream_env: OMG_EVENTS_STREAM
    regions: [us-east-1, us-west-2]
    envelope: {event_id: id, event_type: eventType, occurred_at: occurredAt, work_id: id,
               payload: payload}
  - id: order-domain-events             # every domain event, including the 39 not on Kinesis
    kind: dynamodb_stream
    direction: internal
    table_env: OMG_EVENTS_TABLE
    regions: [us-east-1, us-west-2]
    envelope: {event_id: {path: SK, strip_prefix: "EVENT#"}, event_type: event_type,
               occurred_at: created_at, work_id: {path: SK, strip_prefix: "EVENT#"},
               payload: payload}
  - id: rx-updates
    kind: kinesis
    direction: inbound
    stream_env: OMG_RX_UPDATES_STREAM
    regions: [us-east-1, us-west-2]     # to be confirmed
    envelope: {event_id: eventId, event_type: eventName, occurred_at: {path: ts, epoch: ms},
               payload: data}
  - id: carrier-events
    kind: sns_fanout
    direction: inbound
    topic_env: OMG_CARRIER_EVENTS_TOPIC
    regions: [us-east-1, us-west-2]     # to be confirmed
    envelope: {event_id: MessageId, event_type: Message.event_type,
               occurred_at: Message.occurred_at, payload: Message}
  - id: transmissions
    kind: s3_capture
    bucket_env: OMG_TRANSMISSION_BUCKET
    captures:
      - {prefix: narvar-payloads/, label: narvar, layout: work_unit_action}
      - {prefix: insurance-payloads/, label: insurance, layout: work_unit_action}
      - {prefix: monocle-payloads/, label: monocle, layout: work_unit_action}
      - {prefix: notification-payloads/, label: notification, layout: work_unit_action}
      - {prefix: oracle-payloads/, label: oracle-om, layout: work_unit_order_action}
      - {prefix: springfield-payloads/, label: helios, layout: work_unit_order}
      - {prefix: order-services-payloads/, label: order-services, layout: work_unit_order}
  - id: work-unit-samples               # scripts/test_payloads/*_work_unit.json
    kind: replay
    direction: internal
    path_env: OMG_REPLAY_PATH
    envelope: {event_id: work_unit_id, event_type: event_type,
               occurred_at: metadata.timestamp, work_id: work_unit_id, payload: payload}
```

`financials-events` matches `carrier-events` with `OMG_FINANCIALS_EVENTS_TOPIC` and
`occurred_at: Message.published_at`. No OMG source maps `correlation_id` (open question 1).
Step 1.6 added the two `http_proxy` sources and `OMG_API_UPSTREAM` and
`OMG_VALIDATION_UPSTREAM` to `.env.example`.

## Tests per step

Levels as in the [roadmap](../roadmap.md#test-levels): L0 synthetic and L1 OMG-derived
fixtures in CI; L2 OMG's local stack, marker `omg_local`, skipped unless `OMG_REPO` is set; L3
OMG dev on AWS, read only, by hand. Each step runs the canary harness for its kinds.

- **1.3.** L0: envelope paths and rejections, sampling, batching, retry (`503` then `202`; no
  retry on `401`), unsent counts, backpressure, exit codes. L1: OMG's five `WorkUnit` samples,
  as fixtures and from `OMG_REPO` when set, give five `event:internal:<event_type>:payload`
  channels of one sample each. Integration: a second replay into the body adds only duplicates.
  The tests: `tests/unit/test_envelope.py`, `test_record_pipeline.py`, `test_replay_source.py`,
  `test_tentacle_sender.py`, `test_tentacle_runner.py`, `test_fire_config.py`, and the source
  cases of `test_system_config.py`; `tests/integration/test_tentacle_replay.py` (L1, plus an
  `omg_local` case that replays OMG's whole `scripts/test_payloads/` and checks the fixtures
  still give its fingerprints) and `test_tentacle_canary.py`. The fixtures in
  `tests/fixtures/omg/test_payloads/` are OMG's synthetic samples, copied unchanged at OMG
  commit `2303406c6`.
- **1.6.** The proxy's tests are listed in
  [HTTP and span capture](http-and-span-capture.md#tests); the runner's are the proxy cases of
  `test_tentacle_runner.py` (exchanges counted and sent, `buffer_full`, a failing proxy, an
  upstream that is no URL, a busy port) and of `test_system_config.py`.
- **1.7.** L0: each layout's keys, with no order id in a channel name, `work_id`, or
  `observation_id`; pairing; `status_code: null`; `no_layout`. L1: capture files per layout.
  L2: `make test-oracle-publisher` and `make test-fulfillment-processor` give Oracle channels.
- **1.8.** L0: decoding per kind, `REMOVE`, `filter`, one event from two regions as one
  observation and from two sources as two. L1: records per kind from localstack. L2 with
  `KINESIS_ENABLED=true`: `make test-order` and the processor targets fill `order-domain-events`
  and `domain-events`, joined by `work_id`, and OMG's queues get the same messages with and
  without the tentacle. L3: read only on `domain_events` (roadmap open question 6).

## Open questions

1. OMG's correlation ids can be business identifiers: an event item falls back to the order id,
   a financials work unit to `refund_number`. May they leave as `correlation_id`, hashed, or
   not at all?
2. OMG's event items carry `writer_traceparent`. An optional `envelope.traceparent` would set
   `trace_id` and `span_id` on `dynamodb_stream` shapes and give the work index its `work_id`
   to trace link without waiting for a capture.
3. Order Services captures carry no action in the key or metadata. Name it from the request's
   method and path (step 1.5), or ask OMG for `action` metadata?
4. Positions live in memory, so records written while the tentacle is down are missed unless
   `--start oldest` covers them. Persist checkpoints, or wait for the durable buffer?
5. Listing S3 every 5 s grows with the bucket; S3 event notifications to a FIRE-owned queue
   would need a bucket change in OMG's account.
6. A source whose records lack an event type (OMG accepts financials bodies without one) needs
   a constant channel name: a literal envelope form, or reject such records?
7. The regions of `rx_updates`, `carrier_events`, and `financials_events`, and the stream view
   type of `omg-dev-order_domain_events`.
