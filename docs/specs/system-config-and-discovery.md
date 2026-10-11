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

The tentacle reads `sources`; the engine only validates them for now.

| `kind` | Fields |
|--------|--------|
| `kinesis` | `stream_env` (env var holding the stream name), `regions` (one or more), `dedupe_key` (optional) |

`dedupe_key` names the record field that identifies one event across regions. A system that
publishes the same event in several regions needs it, or every count doubles. In OMG it is
`id`, the field of a Kinesis `DomainEvent` that holds the work unit's `work_unit_id`.

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

Against OMG's graph, observing only the events that `kinesis-publisher` consumes leaves 39
of 125 in `declared_unseen`: reading Kinesis alone does not cover OMG.

## Follow-ups

- The tentacle: read `sources`, generalize and fingerprint at the edge, and feed observed
  events into the catalog. Until then nothing calls `audit_coverage` with real observations.
- Manifest refresh: discovery runs once at startup; a redeploy of the watched system is not
  picked up until FIRE restarts.
- A Slack notifier that posts to `notify.slack.channel_id`. Today `Incident.project_id` is
  not mapped to a `system`, so the notifier cannot pick a channel yet.
- A third coverage list for events that exist but pass through no source FIRE reads; it
  needs each source to declare which events it carries.
- `fingerprinting.sample_rate` is validated but not applied.
