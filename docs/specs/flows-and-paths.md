# Flows and paths

How the body turns spans into flows, learns the path each entry operation leads to, and detects
path drift. Spans and shapes arrive as `SpanObservation`s and `ShapeObservation`s
([observations.md](observations.md)); the tentacle captures them ([tentacle.md](tentacle.md),
[http-and-span-capture.md](http-and-span-capture.md)). Shape baselines and drift are in
[ingest-drift-and-feedback.md](ingest-drift-and-feedback.md#phase-2-drift).

**Status:** the contract of the shipped code. Roadmap step 1.9 shipped
[Trace assembly](#trace-assembly), [Entry](#entry), [Completion](#completion),
[Paths](#paths), [Shapes on flows](#shapes-on-flows), the [Paths endpoint](#paths-endpoint), and
[Process path in the dossier](#process-path-in-the-dossier); step 2.3 shipped
[Path drift](#path-drift) and [Path drift incidents](#path-drift-incidents). Step 2.4 shipped
`ProcessHop.via` ([ProcessHop](dossier-and-decision.md#processhop)), the `path_drift` trigger,
and `EchoReasoner`'s `path_drift` possibilities before them.

| Module | Holds |
|--------|-------|
| `fire/core/domain/flow.py` | `PathPolicy` (the system YAML's `paths`), `Node`, `FlowCounters`, `ShapeRef`, `FlowAssembler` (open flows and their indexes), `CompleteFlow` (entry, local roots, reach, shape attachment, process path) |
| `fire/core/domain/path.py` | `Path` (one entry's folded flows and its signals), `settle` (a complete flow's outcome), `PathDriftRule`, `PathDriftSignal`, `PathDriftFiring`, `PathDrift`, `FlowOutcome`, and the `/paths` models `PathsReport`, `PathSummary`, `NodeSummary`, `EdgeSummary` |
| `fire/core/ports/observation_catalog.py` | `complete_flows()` and `paths(system)` |
| `fire/adapters/memory/observation_catalog.py` | One `FlowAssembler` and its paths per system, under the catalog's lock, on its `clock` |
| `fire/core/services/drift_incident.py` | `DriftIncidentService.open_path_drift` |
| `fire/apps/incident_engine/app.py` | `GET /v0/systems/{system}/paths`, and the loop that completes flows every second |
| `fire/apps/incident_engine/wiring.py` | `log_path_drift`; `build_container(config_path, clock)` |

## Terms

| Term | Meaning |
|------|---------|
| Node | One `(service, operation)` |
| Flow | One unit of work: a head trace plus every trace that links into it, directly or through another linked trace |
| Entry | The node of the span that starts a flow; the roadmap's entry operation is its `operation` |
| Path | What the folded flows of one `(system, entry)` reached: nodes, edges, and counts |

A node is `(service, operation)`, not the operation alone: OMG's API and its processors call the
same AWS operations, and only the service tells those hops apart. A `ProcessHop` names the same
pair as `ref` and `operation`. Operation names come from
[http-and-span-capture.md](http-and-span-capture.md); a span its rules do not name keeps its span
name, such as OMG's `internal` `dynamo_listener.distribute_work`.

## Trace assembly

The catalog hands every span it stores to its system's `FlowAssembler`, with the catalog's clock
at acceptance; a duplicate `(system, trace_id, span_id)` is not stored, so it never reaches
assembly.

- A span joins the open flow that holds its `trace_id`. A `trace_id` no open flow holds starts
  one, unless one of the span's links names a trace the assembler knows: then the trace joins
  the flow of the first such link, or is late when that trace has completed.
- `parent_span_id` gives a `child` edge from the parent's node to the span's node, when the
  parent is in the flow.
- A link names the span that caused this one and gives a `link` edge from that span's node, when
  that span is in the flow. When it names a trace another open flow holds, the two flows merge;
  a link to a trace not yet seen waits for that trace's first span.
- A trace joins through its first link to resolve to another flow, and its flow merges into that
  one. A later link from the same trace to another flow adds nothing and is counted in
  `fan_in_links`. Links within one flow are edges.

Each flow has one head trace: the one that joined no other. A merge keeps the head of the flow
joined, so a listener's trace that links to the API's `server` span joins the API's flow, whose
head is the API's trace. The order of arrival does not matter: the listener's spans may come
first and wait.

In OMG's checkout, the listener (`lambda_base/base_listener.py`) starts
`{listener}.distribute_work` as an `internal` root linked to the span active when the event was
built (`writer_traceparent`); no Lambda instrumentor wraps it. The work distributor puts its trace
context in SNS message attributes, and each processor starts `{processor}.process_work_unit` as
the child of the listener's span (`base_processor.py`). A retry of a work unit continues the
trace of the message it retries (`apps/task_auto_retry`), so a retry after its flow completes is
late, and a retry of a flow the body never saw is an orphan: its root's parent never arrives.
Each L1 recording holds one such sf-publisher retry of an earlier order. Service names below
are from OMG's local stack; `…` is `order-management-gateway`:

```
trace A
  order-management-api      server    POST /v1/orders/place_order
  order-management-api      client      DynamoDB.PutItem omg-dev-order_domain
trace B, linked to the span of trace A that built the event
  …-work-distributor        internal  dynamo_listener.distribute_work
  …-work-distributor        producer    SNS.Publish omg-springfield-events-dev
  …-sf-publisher            internal    sf-publisher.process_work_unit
  …-sf-publisher            client        partner calls, such as Helios
```

## Entry

The entry is decided when a flow completes. A local root is a span whose parent is not in the
flow: it has no `parent_span_id`, or its parent never arrived.

| Local root of the head trace | Result |
|------------------------------|--------|
| `server` or `consumer`, whatever its parent or links, including a parent that is the proxy's id | Entry |
| `internal` with no `parent_span_id` and no link | Entry |
| `internal` whose parent or link never arrived | Orphan |
| `client` or `producer` | Orphan |

A `server` or `consumer` span takes work from outside its service, so its parent or link may name
a span FIRE never receives. The tentacle's proxy is one: it sends no span, and the `traceparent`
it injects names its own id, which becomes the `server` span's `parent_span_id`
([http-and-span-capture.md](http-and-span-capture.md#trace-context)). That `server` span is a
valid local root, not an orphan. An `internal` span's parent and links are inside the system, so
a missing one is a loss, not an entry. A `client` or `producer` root means the caller's span is
missing. OMG's scheduled jobs start `{job_name}.job` as an `internal` root
(`base_scheduled_job.py`): each run is an entry.

When several local roots of the head trace qualify, the earliest `start` is the entry, ties broken
by `span_id`. Every trace should have one local root; a flow where any trace has two is broken.

## Completion

A flow is complete when `paths.settle_s` seconds pass with no span joining it, on the body's clock
at acceptance. Shapes never extend it, and neither does a duplicate. While the body runs, it
completes settled flows every second (`COMPLETE_EVERY_S` in `app.py`) and before answering
`/paths`. A failure of that loop is logged as `flow completion failed: error=<type>`, and of a
diagnosis it starts as `diagnosis failed: incident=<id> error=<type>`; the loop goes on. Tests
inject the clock through `build_container(config_path, clock)` and move it by hand. Only
complete flows fold into paths.

```yaml
paths:
  settle_s: 60              # a flow completes after this long with no span joining it
  max_open_flows: 10000     # one more evicts the open flow joined least recently
  max_spans_per_flow: 1000  # more are dropped and the flow is truncated
```

`PathPolicy` reads this block of the system YAML. All three keys are optional, with these
defaults, and at least `1`; `settle_s` is a finite number of seconds; unknown keys are rejected.
`omg.yml` and `_template.yml` declare the defaults. Sixty seconds leaves room for a DynamoDB
stream, an SNS-to-SQS hop, and a partner call. At L2, a fresh place order's spans reached the
body over 28.9, 8.8, and 1.9 seconds in three runs, the first run the slowest
(`test_paths_omg.py`); in the two L1 recordings, the flow's spans start over 4.9 and 10.7
seconds. A complete flow ends in exactly one outcome (`FlowOutcome`), checked in this order:

| Outcome | When | Counted in |
|---------|------|------------|
| Orphan | No entry | `orphan_flows` |
| Truncated | It reached `max_spans_per_flow` | the path's `truncated_flows` |
| Broken | A trace has two local roots | the path's `broken_flows` |
| Drifted | The path is warm and a drift rule fires | the path's `drifted_flows` |
| Folded | Otherwise | the path's `flows` |

A span over `max_spans_per_flow` still joins its flow, so it extends the settle time, but it is
not kept; a merge that brings a flow over the cap keeps the spans that fit. A complete flow's
trace ids stay in a completed index for `10 × settle_s` (`COMPLETED_RETENTION`). A span of a
completed trace is late: counted in `late_spans`, dropped, and never re-opens the flow. So is
every span of a trace whose first link to resolve names a completed trace: when that trace was
already the head of an open flow, the whole flow is dropped, its spans counted in `late_spans`
and its shapes in `late_shapes`, and its traces enter the completed index. After the index
forgets a trace, a late span starts a new flow, which ends as an orphan unless it qualifies as an
entry.

When a span would open one flow more than `max_open_flows`, the open flow joined least recently is
evicted: counted in `evicted_flows`, not folded, and its traces enter the completed index, so their
later spans count as late. Open flows live in memory and a restart loses them; step 3.1 persists
paths and drift signals only.

## Paths

A path appears with the first complete flow that has its entry, whatever its outcome but orphan.
A folded flow adds to the path of `(system, entry)`:

| Element | Kept |
|---------|------|
| Path | `flows`: folded flows; `drifted_flows`, `broken_flows`, `truncated_flows` |
| Node | `kinds` seen; `flows` that reached it; `spans`; `errors`, the spans with `error: true`; `channels`, per channel key the folded flows whose shape on it attached to the node |
| Edge | Identity `(from, to, via)`, `via` being `child` or `link`; `flows`; `count`, the traversals; `errors`, the traversals whose `to` span has `error: true` |
| Flow channels | Per channel key, the folded flows it attached to without a span |

A node reached three times in one flow adds `3` to `spans` and `1` to `flows`. A path keeps names,
kinds, channel keys, and counts: no span, timestamp, duration, status code, shape, fingerprint, or
value. It is the baseline for path drift: `baseline rebuilding` until `flows` reaches
`drift.min_samples`, then `warm` (`BaselineStatus`). The recorded place-order flow gives a path
of 27 nodes and 26 edges, one of them the `link` from `POST /v1/orders/place_order` to
`dynamo_listener.distribute_work`; both recordings give the same nodes and edges.

## Shapes on flows

The catalog hands every shape it stores with a `trace_id` or a `work_id` to its system's flows,
as a `ShapeRef`: the channel key and the ids. A shape joins the open flow of its trace, but never
extends its settle time. A shape whose trace no open flow holds waits for that trace's first
span. Shapes attach when their flow completes and folds:

| Shape carries | Attaches to |
|---------------|-------------|
| `trace_id` and `span_id` | That span's node |
| `trace_id` and a `span_id` that matches no span but is the `parent_span_id` of a `server` span in that trace | That `server` span's node: the proxy's request and response shapes |
| `trace_id` and a `span_id` that matches neither | Nothing; counted in `unattached_shapes` |
| `trace_id` only | Its flow, as a flow channel |
| `work_id` only | The flow of the trace the work index names, as a flow channel |
| Neither | No flow, and no count |

The work index maps a `work_id` to the trace of the first shape that carries both that `work_id`
and a `trace_id`. A shape that waits `settle_s` for its trace's first span, or a `work_id` shape
that waits `settle_s` for the work index to name a trace, counts in `unattached_shapes`, and so do
the shapes of an orphan flow. A shape whose trace has completed counts in `late_shapes`. The
shapes of an evicted, truncated, broken, or drifted flow attach nowhere and are not counted: the
flow is. The `s3_capture` source is the first to set both
([tentacle.md](tentacle.md#kinds), step 1.7): a pair the capture index joins to the span whose
`transmission.capture` event named it carries that span's ids and the work unit of its key. Its
shapes attach to that span's node and give the work index its entry, so a later shape with the
same `work_id` alone, such as a retry's capture the index missed or a domain event of step 1.8,
attaches to the same flow.

Attaching never touches the shape's stream or baseline: a shape that attaches nowhere is still in
the catalog, and a drifted shape attaches like any other.

## Paths endpoint

`GET /v0/systems/{system}/paths` requires `X-FIRE-Token` (`401` and `503` as in
[observations.md](observations.md#ingest-endpoint)). An unknown system is `404`. Before answering,
the body completes settled flows and opens their path drift incidents. A known system with no
path has an empty `paths` list. The response is `PathsReport`, dumped by alias for the edges'
`from`. After the L1 warm-up of 20 recorded flows and one copy without its Helios call, three of
the 27 nodes and the edges between them, renumbered, where `…` stands for
`order-management-gateway`:

```json
{"system": "omg", "open_flows": 0,
 "counters": {"orphan_flows": 21, "evicted_flows": 0, "late_spans": 0, "late_shapes": 0,
              "unattached_shapes": 0, "fan_in_links": 0},
 "paths": [{
  "entry": {"service": "order-management-api", "operation": "POST /v1/orders/place_order"},
  "status": "warm", "flows": 20, "drifted_flows": 1, "broken_flows": 0, "truncated_flows": 0,
  "nodes": [{"service": "order-management-api", "operation": "DynamoDB.PutItem omg-dev-order_domain",
             "kinds": ["client"], "flows": 20, "spans": 20, "errors": 0, "channels": {}},
            {"service": "order-management-api", "operation": "POST /v1/orders/place_order",
             "kinds": ["server"], "flows": 20, "spans": 20, "errors": 0, "channels": {}},
            {"service": "…-work-distributor", "operation": "dynamo_listener.distribute_work",
             "kinds": ["internal"], "flows": 20, "spans": 20, "errors": 0, "channels": {}}],
  "edges": [{"from": 1, "to": 0, "via": "child", "flows": 20, "count": 20, "errors": 0},
            {"from": 1, "to": 2, "via": "link", "flows": 20, "count": 20, "errors": 0}],
  "flow_channels": {},
  "drift": [{"rule": "hop_missing",
             "node": {"service": "…-sf-publisher",
                      "operation": "POST helios-store.us-east-1.pvt.gateway.stage.warby.io/api/v4/estimate/place"},
             "occurrences": 1, "first_seen": "2026-10-12T01:32:07.627620Z",
             "last_seen": "2026-10-12T01:32:07.627620Z",
             "trace_ids": ["8cd2a43dca5a1664ec9e1436c0ec8133", "dd58eea0af1ee98ec2fc9e16cf39027e"]}]}]}
```

The recordings carry no shapes. Posted through the proxy, as at L2, the `server` node's
`channels` hold one count per request and response channel, such as
`{"http:inbound:POST /v1/orders/place_order:request": 1}`.

| Counter | Counts, per system since the body started |
|---------|---------------------------------------------|
| `orphan_flows` | Complete flows with no entry |
| `evicted_flows` | Open flows evicted by `max_open_flows` |
| `late_spans` | Spans of a completed trace, or of a trace whose first link to resolve named one |
| `late_shapes` | Shapes of a completed trace, or of a flow dropped as late |
| `unattached_shapes` | Shapes that waited `settle_s` for a flow, shapes of an orphan, and shapes of a folded flow whose span id matches nothing |
| `fan_in_links` | Links from a trace that had joined a flow already to another flow |

`from` and `to` are positions in `nodes`. Paths sort by entry service, then operation; nodes by
service, then operation; edges by `from`, `to`, then `via`; kinds by value; channel keys
alphabetically. `drift` lists the path's signals in full ([Path drift](#path-drift)), sorted by
`first_seen`, then rule (`hop_added`, `hop_missing`, `hop_failing`), then node, and names each node
in full, since a `hop_added` node is not in `nodes`.

## Process path in the dossier

A complete flow becomes the dossier's `ProcessPath` (`CompleteFlow.process_path`). Spans of one
node merge into one hop:

| Flow element | `ProcessHop` |
|--------------|--------------|
| Node | `ref`: service; `operation`; `span_kind`: kind of its first span; `status` |
| Edge between two services, or any `link` edge | `from_ref` and `to_ref`: services; `via`: the edge's `child` or `link`; `operation` and `span_kind` of the `to` node; `status` of the traversals |

`status` is `error` when any span counted is in error, else `ok`, then the highest `status_code`
among them when one is set: `ok 201`, `error 502`. A `child` edge within one service gives no hop,
and edges that differ only in their `from` operation are one hop. Node hops sort by the earliest
`start` of their spans, ties by service, then operation; each node's edge hops come just before
it, sorted by the earliest `start` of their `to` spans, then `from` service, then `via`. The
recorded place-order flow gives 29 hops: 27 nodes, the `link` hop from `order-management-api` to
`…-work-distributor`, and the `child` hop from `…-work-distributor` to `…-sf-publisher`.

A path drift incident takes the flow that raised its new signals
([Path drift incidents](#path-drift-incidents)); other triggers choose in their own steps. A
dossier with no complete flow has an empty `process_path` and a `process_path` fragment with
quality `missing` and summary `No complete flow`. A shape drift incident opens when its shape
arrives, before its shape's flow can complete, so every shape drift dossier still carries that
fragment ([Drift incidents](ingest-drift-and-feedback.md#drift-incidents); open question 3).

## Path drift

On a `baseline rebuilding` path, flows fold without comparison, the flow that brings it to
`drift.min_samples` included. On a warm path, each complete flow that is not an orphan, truncated,
or broken is compared with its path before it folds. The rules read the same `drift` block
(`min_samples`, `required_ratio`) as shape drift.

| Rule | Fires for a node when |
|------|-----------------------|
| `hop_added` | The flow reaches it and the path never did |
| `hop_missing` | Its `flows / path flows` is at least `drift.required_ratio`, and the flow does not reach it |
| `hop_failing` | The path reached it with `errors` at `0`, and the flow has a span of it with `error: true` |

A flow that fires any rule is not folded. Each firing is a `PathDriftSignal` keyed by
`(system, entry, rule, node)`, with `occurrences`, `first_seen` and `last_seen`, the earliest and
latest entry `start` among the flows that fired it, whatever the order they completed in, and the
first flow's trace ids, sorted. A later flow firing the same key adds an occurrence and notifies
nothing. Acknowledgement, which folds the hop into the path, is phase 4. `hop_failing` never fires
for a node that erred while the path warmed: in OMG's local stack, staging refuses sf-publisher's
Helios call in every flow, so that node can only go missing.

`settle` returns a `PathDrift` for a flow that fires at least one new signal: the system, the
entry, the entry's `start`, the flow's trace ids, its process path, and one `PathDriftFiring` per
signal it fired, new or not, in rule order, then by node. A firing holds the signal as that
occurrence left it, whether it is new, and the counts its rule read. The catalog's
`complete_flows()` returns them.

## Path drift incidents

For each `PathDrift`, the body logs one `INFO` line with `log_path_drift`
(`fire/apps/incident_engine/wiring.py`), opens one incident with
`DriftIncidentService.open_path_drift(drift)`, and runs its diagnosis (reasoner, then narrator,
then notifier): in the completion loop, or in a background task of the `/paths` request that
completed the flow. A lost processor that takes several nodes with it is one notification.

```
path drift raised: system=<system> service=<entry service> entry=<entry operation> hop_added=<n> hop_missing=<n> hop_failing=<n> new=<n>
```

| Incident field | Value |
|----------------|-------|
| `id` | A new uuid4 |
| `system` | The flow's system |
| `trigger` | `path_drift` ([Incident](dossier-and-decision.md#incident)) |
| `service` | `None` |
| `summary` | `Path drift on <entry operation>` |
| `started_at` | The entry span's `start` |

The dossier holds the flow's process path and one `path_drift` fragment per signal the flow fired,
new or not, in the firings' order; no `origin`, `service`, `shape`, or `fingerprint`, and no
missing fragment. The L1 recording without its Helios call gives:

```json
{"collector": "path_drift", "quality": "present",
 "summary": "hop_missing: POST helios-store.us-east-1.pvt.gateway.stage.warby.io/api/v4/estimate/place on POST /v1/orders/place_order",
 "data": {"rule": "hop_missing",
  "entry": {"service": "order-management-api", "operation": "POST /v1/orders/place_order"},
  "node": {"service": "order-management-gateway-sf-publisher",
           "operation": "POST helios-store.us-east-1.pvt.gateway.stage.warby.io/api/v4/estimate/place"},
  "path_flows": 20, "node_flows": 20, "node_errors": 20, "flow_spans": 0, "flow_errors": 0,
  "occurrences": 1, "first_seen": "2026-10-12T01:32:07.627620Z",
  "trace_ids": ["8cd2a43dca5a1664ec9e1436c0ec8133", "dd58eea0af1ee98ec2fc9e16cf39027e"]}}
```

`path_flows` is the path's folded flows; `node_flows` and `node_errors` are the path's counts,
`0` for `hop_added`; `flow_spans` and `flow_errors` are the drifted flow's, `0` for
`hop_missing`. Trace ids open the flow in a trace viewer and hold no value.

`EchoReasoner` builds one possibility per hop, then one per `present` `path_drift` fragment,
labelled `<rule>: <node operation> on <entry operation>`
([EchoReasoner](dossier-and-decision.md#echoreasoner)). With no missing fragment, the decision has
confidence `0.45` and is not `noul`. The recording without its Helios call notifies once:

```
[fire] notify incident=<uuid> system=omg trigger=path_drift
*FIRE ENGINE* incident `<uuid>` (`omg`)
Confidence: 45%
Possibilities:
• (3%) Failure near order-management-api/POST /v1/orders/place_order _[process_path:order-management-api/POST /v1/orders/place_order]_
…  one line per hop, 28 in all
• (3%) hop_missing: POST helios-store.us-east-1.pvt.gateway.stage.warby.io/api/v4/estimate/place on POST /v1/orders/place_order _[path_drift]_
Process path: `order-management-api/POST /v1/orders/place_order → … → order-management-gateway-sf-publisher/DynamoDB.UpdateItem omg-outbox-dev`
_Narrator: template stub (swap OpenAI-compat via FIRE_NARRATOR_*)._
```

## Tests

Step 1.9's tests are `tests/unit/test_flow_assembly.py`, `tests/unit/test_paths.py` (fold and
summary cases), cases in `tests/unit/test_observation_catalog_memory.py` and
`tests/unit/test_system_config.py`, `tests/integration/test_paths_api.py`, and
`tests/integration/test_place_order_paths.py` (the path cases), with the `/paths` canary in
`tests/integration/test_tentacle_otlp.py` and the L2 test `tests/integration/test_paths_omg.py`.
Step 2.3's are the rule and signal cases of `tests/unit/test_paths.py`, the path drift cases of
`tests/unit/test_drift_incident.py`, and the drift cases of the two integration files.
`tests/spans.py` builds the synthetic spans and the hand-moved `Clock`.

| Step | Level | Proves |
|------|-------|--------|
| 1.9 | L0 | With an injected clock and no sleeps: a synthetic tree is one flow with child edges and counts; a flow completes only once `settle_s` passes with no span joining it; a linked trace joins whether it arrives before or after its target, or its child first; a trace joins through its first link and counts later ones in `fan_in_links`; a link within one flow is an edge; a span after completion is late and changes nothing, and so is a new trace linking to a completed one; a head trace that links to a completed trace makes its flow late whole; the completed index forgets after its retention; a `client` root, a `producer` root, an `internal` root whose link never resolves, and an `internal` span whose parent never arrives are orphans; `server` (also under a proxy id), `consumer`, and causeless `internal` roots are entries; two local roots make a flow broken and the earliest qualifying root is its entry; both caps count what they drop, a merge past the span cap included; shapes attach by span, by the proxy's id, by trace, or by `work_id`, wait for their trace, never extend settle, and are counted unattached or late; the `ProcessPath` mapping, with `via` on edge hops and merged edges; `PathPolicy` bounds |
| 1.9 | L0 | The outcomes in order; a folded flow's nodes, edges, and channels; summary sorting and the `from` alias; the catalog folds a settled flow, a duplicate does not extend settle, each system has its own flows and policy, stored shapes with a trace join their flow, an empty report, copies; `paths` in the system YAML, `omg.yml`, and the template |
| 1.9 | Integration | `/paths` needs the token and a loaded system; a flow shows once it settles, with the proxy's shapes on the `server` node, in the exact format; open flows raise nothing; the running app completes flows on its own, and a failing completion or diagnosis is logged by type and the loop goes on |
| 1.9 | L1 | Each place-order recording, mapped to `SpanObservation`s by the tentacle's own pipeline, gives one path from `POST /v1/orders/place_order` through the DynamoDB write, the listener by its link, SNS, the processor, and its Helios call, and one orphan; two recordings give the same nodes and edges; the recording's canaries reach no `/paths` output |
| 1.9 | L2 | `omg_local`: a fresh order with canaries, posted through the proxy, shows in `/paths` with that path and the proxy's shapes on its `server` node, its spans reach the body within `settle_s`, and no canary reaches `/paths`, the tentacle's posts, or the logs |
| 2.3 | L0 | One case per rule; nothing below `min_samples`, the flow that warms the path included; `required_ratio` at its boundary; rules in rule order, then by node; a drifted flow is not folded; 100 drifted flows are one signal with 100 occurrences and the earliest and latest starts; a flow with one new signal carries its old ones; signal sorting; copies; orphan, truncated, and broken flows raise nothing on a warm path; the drift carries the flow's process path; `log_path_drift`; the signal model rejects bad fields; the incident's fields, and a dossier with the process path and one fragment per signal with its exact data |
| 2.3 | Integration | Two new signals from one flow give one notification, and a repeat gives none; the dossier holds the hops and both fragments and no missing one |
| 2.3 | L1 | Against 20 warm-up copies of the recordings: without its Helios call, one `hop_missing` and one notification, and a second copy only adds an occurrence; with a partner call added, `hop_added`; with the payments call failing, `hop_failing`; without sf-publisher, one incident with a `hop_missing` per node it took; the Helios dossier counts the path's errors and the flow's 28 hops; flows still open raise nothing |

## Open questions

1. Fan-in: a batch span linking to several flows joins only the first. Whether to split per link.
2. Which sources set both `work_id` and `trace_id`, so `work_id` shapes attach. Since step 1.7
   joined captures do; OMG's event rows also store `writer_traceparent`, which could set both on
   `dynamodb_stream` shapes ([tentacle.md](tentacle.md#open-questions), question 2).
3. Whether a shape drift incident waits up to `settle_s` for its shape's flow, for a process path.
4. Rules not covered: a new edge between known nodes, a channel at a new node, and an error rate
   (`hop_failing` never fires for a node that errored once during warm-up).
5. Noisy entries, such as health checks: whether `paths` gets an ignore list, as `policy` does.
6. Comparing observed paths with the event chains OMG's manifest declares (`paths`).
7. `settle_s` on AWS, where SQS batching and cold starts add delay. Locally the place-order flow
   reaches sf-publisher's Helios call and no Oracle call, so the 2.3 L1 case removes the Helios
   call instead.
8. `EchoReasoner` makes every hop a possibility, so a path drift on the recorded flow lists 28
   hops before its one drift possibility, each at 3%. Whether the stub should leave hops out when
   a drift fragment is present, before the Laya/Jev reasoner (step 4.4) replaces it.
