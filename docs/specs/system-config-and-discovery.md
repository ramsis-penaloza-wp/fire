# System config and discovery

How FIRE learns about a watched system. Two sources feed it, with one boundary between them:

- **Declared**: `config/systems/<system>.yml` holds what a person decides: where to look,
  where to notify, and which exceptions apply. It changes in a reviewed pull request.
- **Discovered**: what the system contains (its events, processors, and ingress) comes from
  the system's own manifest and from observation. It is data, not config, and nobody edits
  it by hand.

A system YAML never lists events, processors, or services. A list like that goes stale as
the system changes, and nothing notices until FIRE misses something during an incident.

| What | Where it lives | Who changes it |
|------|----------------|----------------|
| Slack target, manifest location, sources | `config/systems/<system>.yml` | A person, in a PR |
| Policy exceptions (`critical`, `ignore`) | `config/systems/<system>.yml` | A person, in a PR |
| Drift thresholds (`drift`) | `config/systems/<system>.yml` | A person, in a PR |
| Declared events, processors, ingress, payload types | `SystemInventory`, parsed from the manifest | Discovery, at startup |
| Observed events | The catalog (once the tentacle feeds it) | The tentacle |

## System YAML

`load_system_configs(directory)` (`fire/apps/config.py`) loads every `*.yml` in
`config/systems/` except files whose name starts with `_`. The engine loads them at startup
from the `systems/` directory next to `config/fire.yml`.

- The file name must equal `system`: `omg.yml` holds `system: omg`.
- `system` is lowercase letters, digits, `-`, and `_`, starting with a letter or digit.
- Unknown keys are rejected at every level, so a renamed or removed key fails instead of
  being ignored.

`config/systems/_template.yml` is a valid config and a test keeps it valid.

| Key | Required | Meaning |
|-----|----------|---------|
| `system` | yes | Stable id; equals the file name |
| `display_name` | yes | Human-readable name |
| `notify.slack.channel_id` | yes | Slack channel id, see [Slack target](#slack-target) |
| `notify.slack.channel_name` | no | Display only |
| `discovery.manifest.kind` | no | Manifest format, see [Discovery](#discovery) |
| `discovery.manifest.path_env` | with `kind` | Env var holding the manifest file path |
| `sources[]` | no | Where the tentacle reads, see [Sources](#sources) |
| `policy.critical` | no | Events whose drift always matters |
| `policy.ignore` | no | Known noise, left out of coverage gaps |
| `fingerprinting.sample_rate` | no | In `(0, 1]`, default `1.0` |
| `drift.min_samples` | no | Observations folded before a stream is warm; `1` or more, default `20`, see [Baseline](ingest-drift-and-feedback.md#baseline) |
| `drift.required_ratio` | no | Share of samples that makes a field required, for the `removed` drift rule; in `(0, 1]`, default `1.0`, see [Baseline](ingest-drift-and-feedback.md#baseline) |

### Slack target

The channel is addressed by id, not name: an id survives a channel rename, and the Slack API
posts by id.

| Field | Rule | Examples |
|-------|------|----------|
| `channel_id` | `C` (public) or `G` (private), then 8 or more uppercase letters or digits | `C0123456789`, `G0ABCDEFGH` |
| `channel_name` | Optional; `#` then 1 to 80 lowercase letters, digits, `-`, or `_` | `#omg-incidents` |

Rejected ids include a channel name (`#omg-incidents`), lowercase (`c0123…`), a direct
message (`D…`), and a user (`U…`). In Slack, the id is under the channel name → **View
channel details**, at the bottom of the **About** tab.

### Sources

The tentacle reads `sources`. The engine validates them and accepts observations only from a
declared source ([observations.md](observations.md#ingest-endpoint)).

Every source has an `id`: required, `^[a-z0-9][a-z0-9_-]*$`, and unique within the system. It
is the `source` of every observation read from it. Two systems may reuse an id.

| `kind` | Fields besides `id` |
|--------|---------------------|
| `replay` | `direction`, `envelope`, `path_env` |
| `kinesis` | `direction`, `envelope`, `stream_env`, `regions` |
| `dynamodb_stream` | `direction`, `envelope`, `table_env`, `regions`, `filter` (optional) |
| `sns_fanout` | `direction`, `envelope`, `topic_env`, `regions` |
| `s3_capture` | `bucket_env`, `captures` |
| `http_proxy` | `listen_port`, `upstream_env`, and optional `listen_host`, `openapi_path`, `openapi_refresh_s`, `max_body_bytes` |

[tentacle.md](tentacle.md#sources-in-the-system-yaml) defines each key, and
[http-and-span-capture.md](http-and-span-capture.md#inbound-proxy) the `http_proxy` ones; two
`http_proxy` sources of one system cannot share a `listen_host` and `listen_port`. `envelope.event_id`
identifies one event across regions, so a system that publishes the same event in several
regions counts it once. It replaced `dedupe_key`; a file that still has it fails to load with a
message naming `envelope.event_id`.

### Policy

`critical` and `ignore` hold exceptions only, never the full list of events. A name cannot
be in both. Each name must be declared by the manifest or observed; otherwise the coverage
audit reports it as an unknown reference, so a renamed event does not leave the config
silently stale.

## Discovery

At startup the engine runs `discover` (`fire/apps/discovery.py`) for every system and keeps
one `Discovery` per system on the container. A `Discovery` holds either a `SystemInventory`
or a `missing_reason`, never both.

| Situation | Result |
|-----------|--------|
| No `discovery.manifest` | `missing_reason`: `No manifest declared (observation only)` |
| `path_env` unset or empty | `missing_reason`: `<path_env> is not set` |
| File unreadable | `missing_reason`: `Manifest unreadable: …` |
| File is not JSON | `missing_reason`: `Manifest is not JSON: …` |
| Parser rejects the document | `missing_reason`: `Manifest rejected: …` |
| Parsed | `inventory` |

A missing manifest never stops the engine: it diagnoses with less evidence and says so. A
`kind` with no parser is a config error and fails startup.

### Manifest parsers

A parser implements the `ManifestParser` port (`fire/core/ports/manifest_parser.py`): it
turns a decoded document into a `SystemInventory` and raises `ValueError` on a document that
is not its format. It never reads files. The engine registers parsers by `kind` in
`manifest_parsers()` (`fire/apps/incident_engine/wiring.py`). Parsers are written per format,
not per system: any system that publishes the same format reuses one.

### `event_chains_graph`

`EventChainsGraphParser` (`fire/adapters/manifests/event_chains_graph.py`) reads a JSON
event graph with these top-level keys. Any of them may be absent.

| Key | Type | Becomes |
|-----|------|---------|
| `nodes` | list of `{id, kind}` | Events (`kind` `event`, the default) |
| `edges` | list of `{from, to, …}` | Events at both ends, except ends starting with `external:` |
| `consumers` | event → list of processor names | Events, and each event's `consumers` (sorted, unique) |
| `payloads` | event → `{type, fields: [{name, type}]}` | Events, and each event's `payload_type` and `payload_fields` |
| `processors` | name → `{regionPolicy}` | `processors`, sorted by name |
| `externalSources` | list of `{id, label, transport, firstEvents}` | `ingress` sorted by `id`, and events from `firstEvents` |

An event is any name the graph mentions in any of these places, because a graph may name an
event in `consumers` that it lists nowhere else. Against OMG's graph this gives 125 events,
of which 18 appear only as `consumers` keys, plus 31 processors and 5 ingress points. A
section of the wrong type or an entry without a required key raises `ValueError`.

## SystemInventory

`fire/core/domain/inventory.py`. Declarations only: names and the types the system names,
never values.

| Model | Fields |
|-------|--------|
| `SystemInventory` | `system`, `manifest_kind`, `events`, `processors`, `ingress` |
| `DeclaredEvent` | `name`, `payload_type`, `payload_fields`, `consumers` |
| `DeclaredField` | `name`, `type` (the system's own type name, such as `list[AllocatedItem]`) |
| `DeclaredProcessor` | `name`, `region_policy` |
| `DeclaredIngress` | `id`, `label`, `transport`, `first_events` |

Names are non-empty and unique within each list; a duplicate raises `ValidationError`.
Unknown fields on `SystemInventory` are rejected.

## Coverage audit

`audit_coverage(inventory, observed, *, critical, ignore)` (`fire/core/services/coverage.py`)
compares declared event names with observed ones and returns a `CoverageReport`. Every list
is sorted.

| List | Holds |
|------|-------|
| `declared_unseen` | Declared, not observed, not ignored |
| `seen_undeclared` | Observed, not declared, not ignored |
| `unknown_references` | Named in `critical` or `ignore`, but neither declared nor observed |

`is_complete` is true when all three are empty. An event in `ignore` leaves both gap lists;
an ignored name that is observed but undeclared is a valid reference.

`CoverageScope`, in the same module, holds what one system's audit needs: exactly one of
`inventory` and `missing_reason`, and the system's `policy.critical` and `policy.ignore`. A
scope with both or neither raises `ValueError`. `build_container` builds one per system from
its `Discovery` and `policy` for drift incidents
([Drift incidents](ingest-drift-and-feedback.md#drift-incidents)).

Against OMG's graph, observing only the events that `kinesis-publisher` consumes leaves 39
of 125 in `declared_unseen`: reading Kinesis alone does not cover OMG.

## Follow-ups

- The tentacle ([tentacle.md](tentacle.md)) reads `replay` sources and feeds observed events
  into the catalog, and since step 1.6 its `http_proxy` sources feed inbound request and
  response shapes; the other kinds follow in roadmap steps 1.4, 1.7, and 1.8. Since step 2.4, each
  shape drift incident runs `audit_coverage` with the system's observed event types
  ([Drift incidents](ingest-drift-and-feedback.md#drift-incidents)); the coverage endpoint is
  step 2.5.
- Manifest refresh: discovery runs once at startup; a redeploy of the watched system is not
  picked up until FIRE restarts.
- A Slack notifier that posts to `notify.slack.channel_id`. Since step 2.4, `Incident.system`
  names a loaded system, so the notifier can pick the channel; the Slack notifier itself is
  roadmap step 3.6.
- A third coverage list for events that exist but pass through no source FIRE reads; it
  needs each source to declare which events it carries.
- `fingerprinting.sample_rate` is applied by the tentacle
  ([Record pipeline](tentacle.md#record-pipeline), step 4); the body does not sample.
