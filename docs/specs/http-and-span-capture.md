# HTTP and span capture

How the tentacle sees a watched system's HTTP traffic and the operations behind it. Source kind
`otlp` turns each span of the system's traces into a `SpanObservation`, and each complete body
on a `client` span into a `ShapeObservation`. Source kind `http_proxy` sits in front of an HTTP
service and turns its request and response bodies into `ShapeObservation`s. Both run in the
tentacle, so no payload, URL, or header leaves it. Models are in
[observations.md](observations.md); the process, counters, and canary harness in
[tentacle.md](tentacle.md); flows in [flows-and-paths.md](flows-and-paths.md). OMG is the
example.

**Status:** [Inbound proxy](#inbound-proxy) is the contract of the shipped code (step 1.6), and
so are the template rules of [Per-host templates](#per-host-templates) as the proxy applies
them to OpenAPI templates. The rest is a proposal: steps 1.4 and 1.5 turn their sections into
contracts.

## OTLP receiver

The watched system's OpenTelemetry collector gets one more exporter, `otlphttp` with
`encoding: json`, appended to its traces pipeline next to the exporters already there; none is
removed or replaced. With JSON, FIRE parses OTLP with the standard library and needs no
protobuf dependency. OMG's `docker/local/otel-collector-config.yml` exports traces to
`[otlp, debug]` (Jaeger at `warby-jaeger:4317`); `dev/omg/` mounts a copy with this change
([dev/omg/README.md](../../dev/omg/README.md)).

```yaml
exporters:
  otlphttp/fire:
    endpoint: http://host.docker.internal:4318   # the tentacle; /v1/traces is appended
    encoding: json
service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp, debug, otlphttp/fire]
```

| Key | Required | Meaning |
|-----|----------|---------|
| `id`, `kind` | yes | Common keys ([tentacle.md](tentacle.md)); `kind: otlp`. No `envelope` |
| `listen_port` | yes | Receiver port; OMG uses `4318`, which its compose file does not publish |
| `listen_host` | no | Bind address, default `127.0.0.1` |
| `capture_event` | no | A span event that points at archived bodies, see [Capture events](#capture-events) |

- `POST /v1/traces` takes `application/json`, plain or `Content-Encoding: gzip`, in the OTLP
  JSON encoding: hex ids, integer or named enums, nanosecond timestamps as strings.
- Protobuf gets `415`, so a misconfigured exporter fails visibly in the collector's log. A
  body that is not an `ExportTraceServiceRequest` gets `400`, which the collector does not
  retry. A full sender buffer gives `503` with `Retry-After`, so the collector's queue holds
  the spans. Otherwise `200` with `{}`, or `{"partialSuccess": {"rejectedSpans": N}}`.

## Span mapping

| `SpanObservation` field | From |
|-------------------------|------|
| `system`, `source` | The tentacle's system and this source's `id` |
| `trace_id`, `span_id` | `traceId`, `spanId` |
| `parent_span_id` | `parentSpanId`; empty or absent gives `null` |
| `links` | `{trace_id, span_id}` per entry of `links`; attributes, `traceState`, and flags dropped |
| `service` | Resource attribute `service.name` |
| `kind` | 1 `internal`, 2 `server`, 3 `client`, 4 `producer`, 5 `consumer`; 0 gives `internal` |
| `operation` | [Operation names](#operation-names) |
| `status_code` | `http.response.status_code`, else `http.status_code`; 100 to 599, else `null` |
| `error` | `status.code` is 2 (`ERROR`), or `status_code` is 500 or more |
| `start`, `duration_ms` | `startTimeUnixNano` as UTC; `(endTimeUnixNano - startTimeUnixNano) / 1e6` |

`status.message` is never read: it can hold exception text. A link with an invalid id is
dropped and counted; the span is kept. OMG's listener span links to the span that wrote the
event (`writer_traceparent`, `adapters/src/clients/aws/lambda_base/base_listener.py`). A span
has no `observation_id`; a batch the collector resends is a duplicate by `(trace_id, span_id)`.

A span is `rejected` and not sent for `invalid_ids` (missing, wrong length, not hex, or all
zeros), `invalid_times` (missing, or end before start), `no_service`, or `no_operation` (none
can be built, or over 256 characters). Nothing of a rejected span is logged.

### Attribute allowlist

OMG emits the old HTTP names: it does not set `OTEL_SEMCONV_STABILITY_OPT_IN`, so its 0.66b0
instrumentors use the default conventions, and an httpx `CLIENT` span carries only
`http.method`, `http.url`, and `http.status_code` from them. When both names are present, the
stable one is read.

| Attribute | Spans | Becomes |
|-----------|-------|---------|
| `service.name` (resource) | all | `service` |
| `http.request.method`, `http.method` | `server`, `client` | Method in `operation` |
| `http.route` | `server` | Route in `operation` |
| `http.response.status_code`, `http.status_code` | all | `status_code` |
| `server.address`, `server.port` | `client` | Host in `operation` |
| `url.full`, `http.url` | `client` | Parsed in memory into host, port, and path; the rest, query included, is discarded |
| `rpc.system`, `rpc.service`, `rpc.method` | all | AWS or RPC `operation` |
| `aws.dynamodb.table_names`, `aws.sns.topic.arn`, `aws.s3.bucket` | AWS | Resource |
| `messaging.destination.name`, `messaging.destination` | AWS SQS | Resource |
| `http.request.body`; event `http.response.body` | `client` | Shapes, see [Bodies on client spans](#bodies-on-client-spans) |
| The `capture_event` attributes | all | The capture index |

Everything else is dropped without being copied, logged, or named in a count. That includes
every `http.request.header.*` and `http.response.header.*` (OMG's httpx hooks set them on every
`CLIENT` span), `http.request.body.size`, `http.response.body.size`, `http.target`,
`http.host`, `http.user_agent`, `url.query`, `net.*`, `aws.queue_url` and `messaging.url`
(account ids), `aws.request_id`, `messaging.message_id`, `cloud.region`, `db.*`, other `aws.*`,
OMG's `omg.*` (including `omg.work_unit.id`) and `lambda.*`, `exception` events, and every
other span event. For SNS, botocore puts the full topic ARN in `messaging.destination`; that
attribute is read for SQS only.

### Bodies on client spans

OMG's httpx hooks (`adapters/src/middleware/instrumentation/httpx_hooks.py`) set the request
body as the span attribute `http.request.body` and the response body as a span event named
`http.response.body`, only when the client opts in through `capture_body_paths`
(`adapters/src/clients/base_client.py`): Oracle OM `*`, Order Services `/v1/omg/*`, CTO
`/v1/*`. A body over 4032 bytes (`_HTTPX_BODY_MAX_BYTES`) is cut and gets
`...[truncated N bytes]` appended; a body that is not UTF-8 becomes hex. Checked in order:

| Body | Result |
|------|--------|
| Ends with `...[truncated `, digits, ` bytes]` | `skipped: body_truncated` |
| Empty | `skipped: body_empty` |
| Not JSON, such as OMG's hex fallback or a form body | `skipped: body_not_json` |
| JSON, not an object or array | `skipped: body_not_container` |
| A JSON object or array | Generalized by `FlatStarShapeGeneralizer`, fingerprinted, sent |

| `ShapeObservation` field | Value |
|--------------------------|-------|
| `channel` | `http`, `outbound`, the span's `operation`, `request` or `response`, `status_class` from `status_code` |
| `observation_id` | `{trace_id}:{span_id}:{part}` |
| `occurred_at` | Span start for the request, span end for the response |
| `trace_id`, `span_id` | The span's; `region`, `work_id`, and `correlation_id` are `null` |

A span with a request shape and no `status_code` also gives an empty shape on the `none`
response channel ([observations.md](observations.md)). The body string is dropped after
generalization. Bodies on other kinds are ignored. Sampling uses the tentacle's hash rule on
`{trace_id}:{span_id}`, so a request and its response are kept or dropped together; spans are
never sampled, because paths need every span.

### Capture events

OMG's transmission captures (`adapters/src/clients/transmission_capture.py`) add the span event
`transmission.capture` through `set_span_event`, which prefixes every attribute with `omg.`, so
the keys are `omg.transmission.request_uri`, `omg.transmission.response_uri`, and
`omg.transmission.status_code`. The URIs are S3 keys holding a `work_unit_id`, an action, and
for some clients an order id; they never leave the tentacle. For OMG's response middlewares the
event lands on the `CLIENT` span's parent: the `CLIENT` span has already ended
(`httpx_hooks.py`, module docstring).

```yaml
capture_event:
  name: transmission.capture
  request_uri: omg.transmission.request_uri
  response_uri: omg.transmission.response_uri
  ttl_s: 900                                   # default
```

The receiver maps each URI to the span's `trace_id` and `span_id` in memory for `ttl_s`.
Proposed for step 1.7: the `s3_capture` source looks up each object it reads and, on a hit,
sets both ids on its shapes, which then carry `work_id` and `trace_id` together for the work
index of [flows-and-paths.md](flows-and-paths.md). The span observation carries nothing from
the event.

## Operation names

An operation name is built only from names chosen by code or infrastructure: methods, route
templates, hosts, path literals, AWS service and operation names, and resource names. The first
rule that applies wins.

| # | Span | `operation` |
|---|------|-------------|
| 1 | `rpc.system` is `aws-api` | `{rpc.service}.{rpc.method} {resource}`, or without the resource |
| 2 | Another `rpc.system` | `{rpc.service}/{rpc.method}` |
| 3 | `server` with an HTTP method | `{METHOD} {http.route}`, or `{METHOD} {unrouted}` with no route |
| 4 | `client` with an HTTP method and a host | `{METHOD} {host}{path}` |
| 5 | Any other | The span name, each segment between spaces and `/` passed through the id rules |

`METHOD` is the method in uppercase when it is one of the nine HTTP methods, else `HTTP`, as
FastAPI's instrumentor names spans. A request matching no route gets a `SERVER` span with no
`http.route`; rule 3 keeps it visible without naming its path.

For rule 4, `host` is `server.address`, else the URL's host, lowercased, plus `:{port}` when
the port is given and is neither 80 nor 443; an IP literal becomes `{ip}`. `path` is the URL
path, `/` when empty. A host declared in `operations.http` takes its entry's alias and
templates; any other host keeps its name and each path segment goes through the id rules.

### Id rules

A path or span-name segment is percent-decoded and becomes `{id}` when any rule matches. A
segment already of the form `{name}` is kept.

| Rule | Matches | Example |
|------|---------|---------|
| Digits | One or more ASCII digits | `42`, `300000123456789` |
| UUID | Canonical 8-4-4-4-12 hex, either case, as the generalizer checks it | `0f8fad5b-d9cb-469f-a165-70867728950e` |
| ULID | 26 Crockford base32 characters (no `I`, `L`, `O`, `U`; either case), the first `0` to `7` | `01J9ZKX4W3V8Q2T6M5N7P0R1S2` |
| Hex | 16 or more hex digits | `5f2b9c0d1e3a4b6c` |
| Mixed | 8 or more characters, at least one a digit | `SO-1234567`, `11.13.18.05` |
| Unsafe | A character outside `A-Z a-z 0-9 - . _ ~` | `jane@example.com`, `x=1` |
| Long | More than 64 characters | A long slug |

`v1`, `api-v2`, `oauth2`, `deadbeef`, and `order-items` pass every rule. The rules catch values
in id form only; a person's name as a path segment passes them, and declaring the host closes
its paths.

### Per-host templates

```yaml
operations:
  http:
    - hosts: ["*.partner.example.com"]   # exact names, or "*." plus a suffix
      alias: partner                     # replaces the host in names; required
      paths:
        - /api/{version}/orders
        - /api/{version}/orders/{order_id}
```

- Entries are tried in order; the first whose `hosts` matches wins.
- A template segment is a literal or `{name}` (lowercase letters, digits, `_`). `{name}`
  matches one non-empty segment and stays `{name}` in the operation. Among matching templates,
  the first literal where they differ, reading left to right, beats a placeholder.
- A path matching no template is `{METHOD} {alias}/{unlisted}`, counted `operation_unlisted`; a
  declared host never falls back to the id rules.
- Unknown keys, duplicate templates, and an alias outside `[a-z0-9.-]` fail validation.

### AWS resources

From botocore instrumentation 0.66b0, the version OMG pins. A resource is an infrastructure
name and keeps the name the system gives it, environment included.

| `rpc.service` | Resource |
|---------------|----------|
| `DynamoDB` | `aws.dynamodb.table_names`; several (batch calls) sorted and joined with `,` |
| `SNS` | `aws.sns.topic.arn` after its last `:`. `Publish` spans are `producer` spans |
| `SQS` | `messaging.destination.name`, else `messaging.destination`: the queue name |
| `S3` | `aws.s3.bucket`; 0.66b0 does not set it, so OMG's S3 calls have none |
| Others | None |

### Examples

Partner hosts are illustrative; OMG's table and topic are from `config/dev.yml`.

| Inputs | `operation` |
|--------|-------------|
| `server`, `POST`, `http.route` `/v1/orders/place_order` | `POST /v1/orders/place_order` |
| `server`, `GET`, no route | `GET {unrouted}` |
| `client`, `GET https://api.partner.example.com/api/v2/orders/12345?email=a@b.c` | `GET partner/api/{version}/orders/{order_id}` |
| `client`, `GET https://api.partner.example.com/api/v2/refunds` | `GET partner/{unlisted}` |
| `client`, `GET https://other.example.net/v1/customers/SO-1234567/items` | `GET other.example.net/v1/customers/{id}/items` |
| `client`, `POST http://10.0.3.7:9000/hooks/0f8fad5b-d9cb-469f-a165-70867728950e` | `POST {ip}:9000/hooks/{id}` |
| `DynamoDB`, `PutItem`, `[omg-dev-order_domain]` | `DynamoDB.PutItem omg-dev-order_domain` |
| `SNS`, `Publish`, `arn:aws:sns:us-east-1:000000000000:omg-order-events-dev` | `SNS.Publish omg-order-events-dev` |
| `S3`, `PutObject`, no bucket | `S3.PutObject` |
| `internal`, `POST /v1/orders/place_order http send` (ASGI's send span) | Unchanged |
| `internal`, `retry 01J9ZKX4W3V8Q2T6M5N7P0R1S2` | `retry {id}` |

## Inbound proxy

Source kind `http_proxy` puts a reverse proxy in the tentacle in front of one HTTP service.
Clients send to the proxy's port instead of the service's. The proxy forwards each request
unchanged, copies both bodies aside, and reports each exchange to the runner, which shapes it
and sends the shapes to the body with the rest of the tentacle's observations.

| Module | Holds |
|--------|-------|
| `fire/adapters/sources/http_proxy.py` | `HttpProxy`: a uvicorn server (`h11`) in front, `httpcore` to the upstream; `listen` |
| `fire/core/ports/exchange_source.py` | `ExchangeSource`, `ExchangeSink`, `HttpExchange`, `HttpMessage`, `ExchangeSkipReason`, `ExchangeNote` |
| `fire/core/services/exchange_pipeline.py` | `ExchangePipeline` and `PartSkipReason` |
| `fire/core/domain/route_template.py` | `RouteTemplate`, `RouteTable` |
| `fire/adapters/manifests/openapi.py` | `openapi_routes`: the routes of an OpenAPI 3 document |
| `fire/apps/tentacle/sources.py` | `open_http_proxy`: checks the upstream URL and binds the listen address |
| `fire/apps/tentacle/runner.py` | `PlannedProxy`, and the sink that shapes and queues each exchange |

| Key | Required | Meaning |
|-----|----------|---------|
| `id`, `kind` | yes | Common keys; `kind: http_proxy`. No `direction` or `envelope`: channels are `inbound` |
| `listen_port` | yes | Port the proxy listens on, `1` to `65535`; a `listen_host` and port pair is unique within a system |
| `listen_host` | no | Bind address, default `127.0.0.1` |
| `upstream_env` | yes | Env var holding the upstream base URL, `http` or `https`; a path prefix in it is kept |
| `openapi_path` | no | The upstream's OpenAPI document, starting with `/`, default `/openapi.json` |
| `openapi_refresh_s` | no | Template refresh period, default `300` |
| `max_body_bytes` | no | Largest body copied for shaping, default `1048576` |

At startup ([Process](tentacle.md#process)), an unset `upstream_env` skips the source; an
upstream that is not an `http` or `https` URL, or a listen address that cannot be bound, is a
startup error naming the env var or the address. The proxy then serves until `SIGINT` or
`SIGTERM`. On stop it accepts no new connection, finishes the exchanges in flight within 5
seconds, and their shapes drain with the tentacle's other observations. An exchange still
running then is cut: `502` with `x-fire-proxy: upstream-aborted` if nothing was sent yet, and
`upstream_aborted`.

```yaml
sources:
  - {id: omg-api, kind: http_proxy, listen_port: 8021, upstream_env: OMG_API_UPSTREAM}
  - {id: omg-validation, kind: http_proxy, listen_port: 8031,
     upstream_env: OMG_VALIDATION_UPSTREAM}
```

Locally the upstreams are `http://localhost:8020` and `http://localhost:8030`
(`item-rx-validation-service`); both variables go in `.env.example`. Both services serve
FastAPI's default `/openapi.json` (OpenAPI 3.0.2). `install_omg_schema`
(`apps/order_management_api/src/openapi.py`) renames component schemas and leaves `paths` as
FastAPI builds them.

### Route templates

- At startup the proxy fetches `openapi_path` from the upstream directly, every 5 seconds
  until it succeeds; until then requests are forwarded and `skipped: templates_unavailable`.
  It refetches every `openapi_refresh_s` to pick up a reloaded upstream's routes; a failed
  refresh keeps the last set and is counted `noted: templates_refresh_failed`.
- A template is a `paths` key with its operations (`get`, `put`, `post`, `delete`, `options`,
  `head`, `patch`, `trace`). It starts with `/`, has at most 240 characters, and each segment
  is a literal (no braces, whitespace, or C0 or C1 control characters) or a whole-segment `{name}`
  (`name` a letter or `_`, then letters, digits, `_`, `.`, or `-`). A key that is not a
  template, or whose item is not an object, is dropped and counted in the log line
  `templates loaded: source=<id> templates=<n> dropped=<n>`.
- A request matches on its method, exactly, and its percent-decoded path, under the
  [template rules](#per-host-templates): a placeholder matches one non-empty segment, the first
  literal where two matching templates differ beats a placeholder, and between equal ones the
  first in the document wins. `HEAD` matches only a declared `head` operation, not a `get`. A
  `servers` prefix is not stripped; OMG's local `root_path` is empty.
- The channel name is `{METHOD} {template}`, the same string as the `SERVER` span's operation.
- A request matching no template is forwarded and `skipped: unmatched_route`; the count never
  names the path.

### Trace context

- No `traceparent`: the proxy adds `00-{trace_id}-{span_id}-01` with fresh random ids, as the
  last header; the sampled flag makes the upstream record its `SERVER` span.
- One valid `traceparent` is forwarded unchanged, and the proxy reads its trace id and parent
  id. Valid is W3C Trace Context level 1: `{version}-{trace id}-{parent id}-{flags}` in
  lowercase hex, version not `ff`, nothing after the flags for version `00`, and neither id all
  zeros. An invalid one, or more than one header, is forwarded unchanged and counted
  `noted: invalid_traceparent`, and the shapes carry no trace ids.

The exchange's shapes carry that `trace_id`, and as `span_id` the header's parent id, which is
the `parent_span_id` of the upstream's `SERVER` span. No span with that id is ever sent. OMG's
FastAPI services keep the SDK's default propagators, which read W3C `traceparent`.

### Shapes

`ExchangePipeline` shapes each part of an exchange whose route matched. A body is shaped when
its `Content-Type` is `application/json` or ends in `+json`, its `Content-Encoding` is absent,
`identity`, `gzip`, or `deflate`, it is complete within `max_body_bytes`, and it parses as a
JSON object or array. Checked in order, per part; the first failing check decides:

| Part | Result |
|------|--------|
| A response whose status is not `200` to `599` | `skipped: status_unclassified` |
| An empty body | `skipped: no_body` |
| No `Content-Type`, or a media type other than `application/json` or `*/*+json`, parameters ignored | `skipped: body_not_json` |
| A `Content-Encoding` other than `identity`, `gzip`, `x-gzip`, or `deflate` (zlib or raw), or more than one coding | `skipped: body_encoding_unsupported` |
| More than `max_body_bytes` as sent, or once decoded | `skipped: body_too_large` |
| Not decodable, not JSON, or a JSON value that is not an object or array | `skipped: body_not_json` |
| Generalizing, fingerprinting, or building the observation fails | `skipped: shaping_failed` |
| A JSON object or array | Generalized by `FlatStarShapeGeneralizer`, fingerprinted, sent |

The decoded copy and the parsed value are dropped after generalization.

| Field | Request | Response |
|-------|---------|----------|
| `channel` | `http:inbound:{METHOD} {template}:request` | `http:inbound:{METHOD} {template}:response:{status_class}` |
| `observation_id` | `{exchange_id}:request` | `{exchange_id}:response` |
| `occurred_at` | Request headers received | Response headers received |

`exchange_id` is 32 random hex characters per exchange: a client can reuse a `traceparent`, so
trace ids do not identify an exchange. Sampling hashes `exchange_id` with the tentacle's rule,
so a request and its response are kept or dropped together; a dropped exchange counts once as
`skipped: sampled_out`. `trace_id` and `span_id` are those of [Trace context](#trace-context);
`region`, `work_id`, and `correlation_id` are `null`.

### Transparency

Status, headers, and body bytes a client receives are identical with and without the proxy,
with the `Host` header held fixed.

- Bodies stream through as they arrive. The proxy copies at most `max_body_bytes` of each aside
  and never waits for a whole body before forwarding. Forwarded bytes are never decoded or
  re-encoded; shaping decodes the copy.
- The request target is the path and query bytes as received, never normalized: dot segments
  and percent escapes pass unchanged, which is why the upstream client is `httpcore` and not
  `httpx`. A `?` with an empty query is not forwarded, since ASGI does not keep it.
- It adds `traceparent` when absent and nothing else: no `Via`, no `X-Forwarded-*`. `Host` is
  forwarded as received, or set to the upstream's for an HTTP/1.0 request without one.
  Hop-by-hop headers follow RFC 9110 (`Connection`, the headers it names, `Keep-Alive`,
  `Proxy-Connection`, `TE`, `Trailer`, `Transfer-Encoding`, `Upgrade`) and are removed both
  ways; end-to-end headers pass unchanged and in order. A `Connection` token never removes
  `Host` or `Content-Length`, and `Content-Length` is dropped when `Transfer-Encoding` frames the
  message (RFC 9112, section 6.3). A request with `Content-Length` keeps it, a chunked request
  is re-chunked, and a request with neither has no body.
- The proxy's own server adds no `Date` or `Server` header and writes no access log; it adds
  `Connection: close` to a response only when the client sent it.
- Shaping runs after the response is sent, in a callback the response never waits for. A
  failure of the sink is logged as `shaping failed: source=<id> error=<type>`; a failure inside
  `ExchangePipeline` is counted `shaping_failed`.
- Upstream unreachable (refused, DNS failure, or no connection within 5 seconds): `502` with an
  empty body and `x-fire-proxy: upstream-unreachable`, and no observation
  (`upstream_unreachable`). Without the proxy the client would get a connection error; the
  header tells this `502` from the upstream's. Any other upstream failure before the response
  headers gives `502` with `x-fire-proxy: upstream-aborted` (`upstream_aborted`), such as an
  upstream that closes a keep-alive connection without `Connection: close` just as the next
  request goes out on it. The proxy never retries: the upstream may already have run the
  request, and a direct client would see the failure too.
- Upstream fails mid-response: the status is already sent, so the proxy closes the client
  connection (`upstream_aborted`). A client that disconnects cancels the upstream request
  (`client_aborted`). The proxy sets no read timeout of its own.
- An `Upgrade` to `websocket` is out of scope: `501` with an empty body
  (`upgrade_unsupported`). Any other upgrade, such as `h2c`, is a hop-by-hop header like the
  rest: removed, and the request forwarded as HTTP/1.1, as a server may ignore an upgrade.

### Counts and logs

Counted on the source's [counters line](tentacle.md#sending-to-the-body). `read` counts
exchanges; each exchange is either shaped or skipped once for the whole exchange.

| Count | When |
|-------|------|
| `skipped: templates_unavailable`, `unmatched_route` | Forwarded, with no template to name its channel |
| `skipped: upstream_unreachable`, `upstream_aborted`, `client_aborted`, `upgrade_unsupported` | As in [Transparency](#transparency); no observation |
| `skipped: sampled_out` | Once per exchange sampled out |
| `skipped:` a reason of [Shapes](#shapes) | Once per part not shaped |
| `skipped: buffer_full` | An observation that found the tentacle's buffer full: the proxy cannot pause the traffic it forwards |
| `noted: invalid_traceparent`, `templates_refresh_failed` | Counted; nothing is dropped |

Its log lines: `templates loaded: source=<id> templates=<n> dropped=<n>` on the first load and
whenever a count changes, `templates unavailable: source=<id> error=<type or status=<code>>`
once until the first load, `templates refresh failed: source=<id> error=<…>`,
`shaping failed: source=<id> error=<type>`, `exchange failed: source=<id> error=<type>` for an
unexpected error in the proxy itself, and at `DEBUG`
`upstream failed: source=<id> stage=<connect|before_response|mid_response> error=<type>`.
uvicorn's own lines carry no request data: its startup lines, and `ASGI callable returned
without completing response.` after an upstream abort. Libraries that would log request data
are held back: uvicorn's access log is off on every connection (uvicorn turns it back on per
connection whenever `uvicorn.access` has a handler), its `TRACE` message logger is never
installed, and the `httpcore` logger is raised to `INFO`, since its `DEBUG` lines carry
response headers. The OpenAPI fetch goes through the same `httpcore` pool, not `httpx`, which
logs every URL at `INFO`.

## Privacy

The canary rule and the logging rules of [tentacle.md](tentacle.md#privacy) hold on every path
here: a value planted in a body, URL, header, span attribute, span event, or link reaches no
observation, count, or log line. Span mapping emits only `SpanObservation` fields. Client-span
bodies and the proxy emit only `ShapeObservation`s: channel, ids, timestamps, shape, and
fingerprint. Capture events emit nothing; their index stays in memory. The proxy's added
`traceparent` holds only random ids and goes only to the upstream. Every path emits counts
per source and reason.

Strings copied into observations come from code or infrastructure: `service.name`, span names
(rule 5), `http.route`, OpenAPI templates, hosts, path literals, `rpc.*`, and resource names.
The harness plants values everywhere else. A span name built from a payload value would pass
rule 5; OMG builds its names from code (`{processor_name}.process_work_unit`), and the L1
canary run on recorded spans would catch a system that does not.

## Tests

| Step | Level | Proves |
|------|-------|--------|
| 1.4 | L0 | Every field and allowlist row; canaries in dropped attributes, the query, headers, both bodies, `status.message`, exception events, and link attributes reach no observation or log; each body outcome and rejection; gzip; `415`, `400`, `503`; a resent batch gives the same ids |
| 1.4 | L1 | OTLP JSON recorded from `make test-order`: the `SERVER` span of `POST /v1/orders/place_order` and its DynamoDB and SNS children share one `trace_id` |
| 1.4 | L2 | Live `make test-order` delivers those spans; Jaeger receives every trace, also with the tentacle stopped |
| 1.5 | L0 | The examples; each id rule alone with near misses (15 hex digits, 25 base32 characters); the strings that pass every rule; template specificity; `{unlisted}`; `operations` validation |
| 1.5 | L1 | Two recordings of the same OMG targets give the same operation names; canary ids in path positions never appear |
| 1.6 | L0 | `tests/unit/test_http_proxy.py`, a fake FastAPI upstream: byte-for-byte transparency for JSON, non-JSON, gzip, empty, chunked, streamed, and oversized bodies, a chunked request, and `HEAD`, also with a sink that raises; `502` both ways; `traceparent` added, kept, and kept when invalid or repeated; hop-by-hop headers; unmatched routes, templates loaded late, a failed refresh; `501`; client and upstream aborts; canaries in path, query, headers, and bodies. `test_exchange_pipeline.py`: each row of [Shapes](#shapes), sampling, the fields. `test_route_template.py`: parsing and specificity |
| 1.6 | L1 | `test_openapi_routes.py` against `tests/fixtures/omg/openapi/` (the `paths` and operation ids of both services' `/openapi.json`): a concrete path per route maps to its template; a literal beats a parameter. `tests/integration/test_tentacle_proxy.py`: OMG's order fixture through the proxy to a fake upstream and into the body gives request and response channels, warm-up variants then a drifted copy notify once, and the canary |
| 1.6 | L2 | `tests/integration/test_tentacle_proxy_omg.py`: OMG's order payloads posted through the proxy give request and response channels for `POST /v1/orders/place_order` whose span id is the parent of OMG's `SERVER` span in Jaeger, and `/openapi.json` crosses the proxy unchanged. `test_openapi_routes.py` checks the fixtures still match the live documents |

L3 waits on open questions 1 and 2.

## Open questions

1. How FIRE receives spans on AWS: a second exporter in the collector OMG runs there, or a pull
   from Datadog or New Relic (roadmap question 8). The allowlist and names stay; the transport
   changes.
2. Inbound bodies on AWS, where the proxy cannot run: an OMG middleware that puts bodies on
   `SERVER` spans, as its httpx hooks do on `CLIENT` spans, or another tap (roadmap question
   7). Such bodies would follow the client-span rules as inbound shapes.
3. `make test-order` runs in Docker and calls `http://warby-omg-order-management-api:8000`
   (`scripts/call_order_api.py`), bypassing a proxy on the host. Either `dev/omg/` routes that
   hostname through the proxy, or FIRE posts OMG's payloads itself; step 1.10 assumes the
   first.
4. Which call a capture belongs to: OMG records `transmission.capture` on the `CLIENT` span's
   parent, so a processor span with several partner calls holds several captures.
5. Whether the receiver and the proxy need a token once they listen beyond the loopback.
