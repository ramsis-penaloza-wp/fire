# Flows and paths

How the body turns spans into flows, learns the path each entry operation leads to, and detects
path drift. Spans and shapes arrive as `SpanObservation`s and `ShapeObservation`s
([observations.md](observations.md)); the tentacle captures them ([tentacle.md](tentacle.md),
[http-and-span-capture.md](http-and-span-capture.md)). Shape baselines and drift are in
[ingest-drift-and-feedback.md](ingest-drift-and-feedback.md#phase-2-drift).

**Status: proposal.** Roadmap steps 1.9 and 2.3 implement it; each step's pull request turns the
matching section into the contract of the shipped code.

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
[http-and-span-capture.md](http-and-span-capture.md); for spans its rules do not name, this doc
assumes the span name, such as OMG's `internal` `dynamo_listener.distribute_work`.

## Trace assembly

The body assembles flows per system from accepted spans. A duplicate
`(system, trace_id, span_id)` never reaches assembly.

- A span joins the open flow that holds its `trace_id`. A `trace_id` no open flow holds starts one.
- `parent_span_id` gives a `child` edge from the parent's node to the span's node.
- A link names the span that caused this one and gives a `link` edge from that span's node. When
  it names a trace another open flow holds, the two flows merge; a link to a trace not yet seen
  waits for that trace's first span.
- A trace joins through its first link to resolve. A later link from the same trace to another
  flow adds nothing and is counted in `fan_in_links`. Links within one flow are edges.

In OMG's checkout, the listener (`lambda_base/base_listener.py`) starts
`{listener}.distribute_work` as an `internal` root linked to the span active when the event was
built (`writer_traceparent`); no Lambda instrumentor wraps it. The work distributor puts its trace
context in SNS message attributes, and each processor starts `{processor}.process_work_unit` as
its child (`base_processor.py`). The retry waker (`apps/task_auto_retry`) continues the trace of
the message it wakes, so a retry after its flow completes is late. Service names below are from
OMG's local stack; `…` is `order-management-gateway`:

```
trace A
  order-management-api      server    POST /v1/orders/place_order
  order-management-api      client      DynamoDB.PutItem order_domain
trace B, linked to the span of trace A that built the event
  …-work-distributor        internal  dynamo_listener.distribute_work
  …-work-distributor                    SNS publish, context in message attributes
  …-sales_order_processor   internal      sales_order_processor.process_work_unit
  …-sales_order_processor   client          partner calls
```

## Entry

The entry is decided when a flow completes. A local root is a span whose parent is not in the
flow: it has no `parent_span_id`, or its parent never arrived. The head trace is the one trace of
the flow that joined no other.

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
valid local root, not an orphan. An `internal` span's parent and links are inside the system, so a missing one is a loss,
not an entry. A `client` or `producer` root means the caller's span is missing. OMG's scheduled
jobs start `{job_name}.job` as an `internal` root (`base_scheduled_job.py`): each run is an entry.

When several local roots of the head trace qualify, the earliest `start` is the entry, ties broken
by `span_id`. Every trace should have one local root; a flow where any trace has two is broken.

## Completion

A flow is complete when `paths.settle_s` seconds pass with no span joining it, on the body's clock
at acceptance. Shapes never extend it. The body completes settled flows at least once a second and
before answering `/paths`; tests inject the clock. Only complete flows fold into paths.

```yaml
paths:
  settle_s: 60              # a flow completes after this long with no span joining it
  max_open_flows: 10000     # one more evicts the open flow joined least recently
  max_spans_per_flow: 1000  # more are dropped and the flow is truncated
```

All three are optional, with these defaults, and at least `1`. Sixty seconds leaves room for a
DynamoDB stream, an SNS-to-SQS hop, and a partner call; step 1.9 measures it at L2. A complete
flow ends in exactly one outcome, checked in this order:

| Outcome | When | Counted in |
|---------|------|------------|
| Orphan | No entry | `orphan_flows` |
| Truncated | It reached `max_spans_per_flow` | the path's `truncated_flows` |
| Broken | A trace has two local roots | the path's `broken_flows` |
| Drifted | The path is warm and a drift rule fires | the path's `drifted_flows` |
| Folded | Otherwise | the path's `flows` |

A complete flow's trace ids stay in a completed index for `10 × settle_s`. A span of a completed
trace, or of a trace whose first resolved link names one, is late: counted in `late_spans`,
dropped, and never re-opens the flow. After the index forgets a trace, a late span starts a new
flow, which ends as an orphan unless it qualifies as an entry.

When a span would open one flow more than `max_open_flows`, the open flow joined least recently is
evicted: counted in `evicted_flows`, not folded, and its traces enter the completed index, so their
later spans count as late. Open flows live in memory and a restart loses them; step 3.1 persists
paths and drift signals only.

## Paths

A folded flow adds to the path of `(system, entry)`:

| Element | Kept |
|---------|------|
| Path | `flows`: folded flows |
| Node | `kinds` seen; `flows` that reached it; `spans`; `errors`, the spans with `error: true`; `channels` |
| Edge | Identity `(from, to, via)`, `via` being `child` or `link`; `flows`; `count`, the traversals; `errors`, the traversals whose `to` span has `error: true` |
| Flow channels | Per channel key, the folded flows it attached to without a span |

A node reached three times in one flow adds `3` to `spans` and `1` to `flows`. A path keeps names,
kinds, channel keys, and counts: no span, timestamp, duration, status code, shape, fingerprint, or
value. It is the baseline for path drift: `baseline rebuilding` until `flows` reaches
`drift.min_samples`, then `warm`.

## Shapes on flows

A `ShapeObservation` joins a flow as a span does, but never extends its settle time. It attaches
when the flow completes:

| Shape carries | Attaches to |
|---------------|-------------|
| `trace_id` and `span_id` | That span's node |
| `trace_id` and a `span_id` that matches no span but is the `parent_span_id` of a `server` span in that trace | That `server` span's node: the proxy's request and response shapes |
| `trace_id` and a `span_id` that matches neither | Nothing; counted in `unattached_shapes` |
| `trace_id` only | Its flow, as a flow channel |
| `work_id` only | The open flow the work index names, as a flow channel |
| Neither | No flow, and no count |

The work index maps a `work_id` to the flow of any observation that carries both that `work_id`
and a `trace_id`. A `work_id` shape that finds no flow within `settle_s`, or whose flow is an
orphan, counts in `unattached_shapes`; one whose flow has completed counts in `late_shapes`. In
OMG, the `transmission.capture` span event names the capture pair, whose S3 keys hold the
`work_unit_id`.

Attaching never touches the shape's stream or baseline: a shape that attaches nowhere is still in
the catalog.

## Paths endpoint

`GET /v0/systems/{system}/paths` requires `X-FIRE-Token` (`401` and `503` as in
[observations.md](observations.md#ingest-endpoint)). An unknown system is `404`; a known system
with no folded flows has an empty `paths` list. Abbreviated example, where `…` stands for
`order-management-gateway`:

```json
{"system": "omg", "open_flows": 2,
 "counters": {"orphan_flows": 1, "evicted_flows": 0, "late_spans": 3, "late_shapes": 0,
              "unattached_shapes": 4, "fan_in_links": 0},
 "paths": [{
  "entry": {"service": "order-management-api", "operation": "POST /v1/orders/place_order"},
  "status": "warm", "flows": 24, "drifted_flows": 1, "broken_flows": 0, "truncated_flows": 0,
  "nodes": [{"service": "order-management-api", "operation": "DynamoDB.PutItem order_domain",
             "kinds": ["client"], "flows": 24, "spans": 24, "errors": 0, "channels": {}},
            {"service": "order-management-api", "operation": "POST /v1/orders/place_order",
             "kinds": ["server"], "flows": 24, "spans": 24, "errors": 0,
             "channels": {"http:inbound:POST /v1/orders/place_order:request": 24}},
            {"service": "…-work-distributor", "operation": "dynamo_listener.distribute_work",
             "kinds": ["internal"], "flows": 24, "spans": 24, "errors": 0, "channels": {}}],
  "edges": [{"from": 1, "to": 0, "via": "child", "flows": 24, "count": 24, "errors": 0},
            {"from": 1, "to": 2, "via": "link", "flows": 24, "count": 24, "errors": 0}],
  "flow_channels": {"event:internal:order.created:payload": 24},
  "drift": [{"rule": "hop_missing", "occurrences": 1, "first_seen": "2026-10-11T16:40:02Z",
             "node": {"service": "…-sales_order_processor",
                      "operation": "sales_order_processor.process_work_unit"}}]}]}
```

`from` and `to` are positions in `nodes`. Paths sort by entry service, then operation; nodes by
service, then operation; edges by `from`, `to`, then `via`; channel keys alphabetically. `drift`
lists the path's signals and names each node in full, since a `hop_added` node is not in `nodes`.
Step 2.3 adds `status`, `drifted_flows`, and `drift`.

## Process path in the dossier

A complete flow becomes the dossier's `ProcessPath`. Spans of one node merge into one hop:

| Flow element | `ProcessHop` |
|--------------|--------------|
| Node | `ref`: service; `operation`; `span_kind`: kind of its first span; `status` |
| Edge between two services, or any `link` edge | `from_ref` and `to_ref`: services; `operation` and `span_kind` of the `to` node; `status` of the traversals |

`status` is `error` when any span counted is in error, else `ok`, then the highest `status_code`
among them when one is set: `ok 201`, `error 502`. A `child` edge within one service gives no hop.
Hops sort by the earliest `start` of the spans they stand for; an edge sorts just before its `to`
node; ties sort by service, then operation. `ProcessHop` cannot mark a link, so step 2.4 adds an
optional `via` (`child`, `link`) to edge hops and updates
[dossier-and-decision.md](dossier-and-decision.md).

A path drift incident takes the flow that raised its new signals; other triggers choose in their
own steps. A dossier with no complete flow has an empty `process_path` and a `process_path`
fragment with quality `missing` and summary `No complete flow`.

## Path drift

On a `baseline rebuilding` path, flows fold without comparison. On a warm path, each complete flow
that is not an orphan, truncated, or broken is compared with its path before it folds. The rules
read the same `drift` block (`min_samples`, `required_ratio`) as shape drift.

| Rule | Fires for a node when |
|------|-----------------------|
| `hop_added` | The flow reaches it and the path never did |
| `hop_missing` | Its `flows` is at least `drift.required_ratio` × the path's `flows`, and the flow does not reach it |
| `hop_failing` | The path reached it with `errors` at `0`, and the flow has a span of it with `error: true` |

A flow that fires any rule is not folded. Each firing is a drift signal keyed by
`(system, entry, rule, node)` with `first_seen`, `last_seen`, `occurrences`, and the first flow's
trace ids. A later flow firing the same key adds an occurrence and notifies nothing.
Acknowledgement, which folds the hop into the path, is phase 4.

A flow that fires at least one new signal opens one incident with `trigger: path_drift`, a new
trigger next to `shape_drift`. Its dossier holds one `path_drift` fragment per signal the flow
fired, new or not, so a lost processor that takes several nodes with it is one notification:

```json
{"collector": "path_drift", "quality": "present",
 "summary": "hop_missing: sales_order_processor.process_work_unit on POST /v1/orders/place_order",
 "data": {"rule": "hop_missing",
  "entry": {"service": "order-management-api", "operation": "POST /v1/orders/place_order"},
  "node": {"service": "order-management-gateway-sales_order_processor",
           "operation": "sales_order_processor.process_work_unit"},
  "path_flows": 24, "node_flows": 24, "node_errors": 0, "flow_spans": 0, "flow_errors": 0,
  "occurrences": 1, "first_seen": "2026-10-11T16:40:02Z",
  "trace_ids": ["4bf92f3577b34da6a3ce929d0e0e4736"]}}
```

`node_flows` and `node_errors` are the path's counts, `0` for `hop_added`; `flow_spans` and
`flow_errors` are the drifted flow's. Trace ids open the flow in a trace viewer and hold no value.

## Tests

| Step | Level | Proves |
|------|-------|--------|
| 1.9 | L0 | With an injected clock and no sleeps: a synthetic tree folds into one path with child edges and counts; a linked trace joins whether it arrives before or after its target; a span after `settle_s` is late and changes nothing; a `client` root, an `internal` root whose link never resolves, and an `internal` span whose parent never arrives are orphans; two local roots make a flow broken; both caps count what they drop; duplicates do not extend settle; a `server` span whose parent is a proxy id is the entry and gets the proxy's shapes; shapes attach by span, by trace, or by `work_id`, or are counted; the `ProcessPath` mapping |
| 1.9 | L1 | The place-order recording from step 1.4, mapped to `SpanObservation`s, gives one path from `POST /v1/orders/place_order` through the DynamoDB write, the listener by its link, SNS, the processors, and their partner calls; two recordings give the same nodes and edges |
| 1.9 | L2 | `omg_local`: after `make test-order` and the processor targets, `/paths` shows that path, and a canary order leaves no canary in it |
| 2.3 | L0 | One case per rule; nothing below `min_samples`; `required_ratio` at its boundary; a drifted flow is not folded; 100 drifted flows are one signal with 100 occurrences; open, orphan, truncated, and broken flows raise nothing; several new signals from one flow open one incident |
| 2.3 | L1 | The recording minus its Oracle call fires `hop_missing` for each node removed and opens one incident; with a partner span added, `hop_added`; with an error on a partner span, `hop_failing` |

## Open questions

1. Fan-in: a batch span linking to several flows joins only the first. Whether to split per link.
2. Which sources set both `work_id` and `trace_id`, so `work_id` shapes attach. OMG's event rows
   store `writer_traceparent`; [tentacle.md](tentacle.md) decides.
3. Whether a shape drift incident waits up to `settle_s` for its shape's flow, for a process path.
4. Rules not covered: a new edge between known nodes, a channel at a new node, and an error rate
   (`hop_failing` never fires for a node that errored once during warm-up).
5. Noisy entries, such as health checks: whether `paths` gets an ignore list, as `policy` does.
6. Comparing observed paths with the event chains OMG's manifest declares (`paths`).
7. `settle_s` on AWS, where SQS batching and cold starts add delay, and whether OMG's local
   place-order flow reaches the Oracle call the 2.3 L1 case removes.
