# FIRE against OMG's local stack

`dev/omg/` wires FIRE to the local stack of OMG (`order-management-gateway`). It is test level
L2 in the [roadmap](../../docs/roadmap.md#test-levels): OMG runs in docker compose with
LocalStack, driven by its own `make test-*` targets and payloads, while the body and the
tentacle run on the host. Nothing in the OMG repository changes: FIRE adds one compose override,
passed with an extra `-f`, and its own collector config.

**Status: partly planned.** The body runs, and so does the tentacle with its `replay` source
(step 1.3) and its inbound proxies (step 1.6), so the inbound part of the drift demo (section e)
runs today. Roadmap steps 1.4, 1.7, 1.8, 1.10, and 2.6 add the other files and commands below.

Related: [ingest spec](../../docs/specs/ingest-drift-and-feedback.md),
[tentacle](../../docs/specs/tentacle.md), [HTTP and span capture](../../docs/specs/http-and-span-capture.md),
[flows and paths](../../docs/specs/flows-and-paths.md), [observations](../../docs/specs/observations.md).

## Prerequisites

```bash
export OMG_REPO=/path/to/order-management-gateway
export FIRE_REPO=/path/to/fire
```

OMG, per its `README.md`, `AGENTS.md`, and `.docs/runbooks/local-setup.md`:

- Python 3.14, uv, Docker with Compose, direnv, and the AWS CLI with SSO.
- `direnv allow` in `$OMG_REPO` (sets `AWS_PROFILE`, `AWS_REGION`), then
  `aws sso login --profile <your-profile>`. `make dev/init` fails its preflight without SSO.
- `GITHUB_TOKEN` exported, with WarbyParker org read access. OMG's targets write it to the
  gitignored `.github-token.txt` for image builds; `terraform init` uses it for private modules.
- ECR access to account `844647875270` for the private `wp-terraform` image. OMG runs
  `make aws/ecr/login` before every `make docker/up/*`.
- `uv sync --all-packages --dev` in `$OMG_REPO`.

FIRE: Python 3.13+, uv, `jq`, `curl`, and `make install`. Copy `.env.example` to `.env` and set
`FIRE_API_TOKEN`. The curl checks below read it from the shell: `set -a; . "$FIRE_REPO/.env"; set +a`.

## Files in this directory (steps 1.4 and 2.6, none created yet; step 1.6 needs none)

| File | Step | Holds |
|------|------|-------|
| `compose.fire.yml` | 1.4 | Override that mounts FIRE's collector config into OMG's `otel-collector` |
| `otel-collector-config.yml` | 1.4 | OMG's collector config plus one exporter to the tentacle |
| `corpora/` | 2.6 | Drift corpora: `events/` (WorkUnits), `outbound/` (Oracle captures), `path/` (place-order OTLP traces) |

`compose.fire.yml`:

```yaml
services:
  otel-collector:
    volumes:
      - ${FIRE_REPO:?set FIRE_REPO}/dev/omg/otel-collector-config.yml:/otel-collector-config.yaml:ro
    extra_hosts:
      - host.docker.internal:host-gateway
```

- Compose merges `volumes` across files by container path, so this entry replaces OMG's
  `./docker/local/otel-collector-config.yml` mount; OMG's `--config=/otel-collector-config.yaml`
  command still applies. Relative paths in every `-f` file resolve against the first file's
  directory (OMG's root), so the source goes through `${FIRE_REPO}`. Both were checked with
  `docker compose config` (Compose v5.1.2).
- `extra_hosts` makes `host.docker.internal` resolve on Linux; Docker Desktop provides it.

`otel-collector-config.yml` is OMG's `docker/local/otel-collector-config.yml` plus
`otlphttp/fire` in the traces pipeline, next to `otlp` (Jaeger) and `debug`. The exporter
appends `/v1/traces` to `endpoint`.

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:55680"
      http:
        endpoint: "0.0.0.0:55681"
exporters:
  otlp:
    endpoint: warby-jaeger:4317
    tls:
      insecure: true
  prometheus:
    endpoint: "warby-otel-collector:8889"
  debug:
    verbosity: detailed
    sampling_initial: 5
    sampling_thereafter: 200
  otlphttp/fire:
    endpoint: http://host.docker.internal:4318
    encoding: json
    compression: none
service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp, debug, otlphttp/fire]
    metrics:
      receivers: [otlp]
      exporters: [prometheus, debug]
```

TODO(1.4): fix the tentacle's OTLP listen port; this README assumes `4318`.

## a. Start OMG with FIRE's override (step 1.4)

Start OMG its own way. `make dev/init` brings up LocalStack, applies Terraform (the processors
are Lambdas in LocalStack), and starts the `otel` and `omg` profiles. Its daily start is
`make docker/up/tools`, `make tf/local/init tfupgrade=true`, `make tf/local/apply`, plus
`make docker/up/omg`.

```bash
cd "$OMG_REPO"
make dev/init
KINESIS_ENABLED=true make tf/local/apply   # optional, step 1.8
```

The terraform service reads `TF_VAR_kinesis_enabled=${KINESIS_ENABLED:-false}`, so the flag
counts at `tf/local/apply`. It adds the `omg-dev-domain-events` and `rx-updates` streams.

Then recreate only the collector. OMG's targets pass `DOCKER_COMPOSE_FILES` =
`-f docker-compose.yml -f ./docker-compose.local.yml`; FIRE appends its file. Run from
`$OMG_REPO`, because OMG's compose files mount `${PWD}/apps`.

```bash
cd "$OMG_REPO"
FIRE_COMPOSE=(-f docker-compose.yml -f docker-compose.local.yml -f "$FIRE_REPO/dev/omg/compose.fire.yml")
docker compose "${FIRE_COMPOSE[@]}" --profile otel up -d otel-collector
docker inspect warby-omg-otel-collector \
  --format '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{println}}{{end}}'
# expect: <FIRE_REPO>/dev/omg/otel-collector-config.yml -> /otel-collector-config.yaml
docker logs warby-omg-otel-collector 2>&1 | grep 'Everything is ready'
curl -s http://localhost:8020/healthcheck && curl -s http://localhost:8030/healthcheck
```

## b. Start the body and the tentacle

Body (exists today), on `127.0.0.1:8000` unless `FIRE_HOST` or `FIRE_PORT` say otherwise:

```bash
cd "$FIRE_REPO"
export OMG_MANIFEST_PATH="$OMG_REPO/scripts/event_chains/event_chains_graph.json"
PYTHONUNBUFFERED=1 make run 2>&1 | tee /tmp/fire-body.log
curl -s http://127.0.0.1:8000/health   # in another terminal: {"status":"ok"}
```

The console notifier prints each notification to stdout, which Python buffers when it is a pipe
or a file: without `PYTHONUNBUFFERED=1`, notifications appear only when the body stops.
`make run` shows uvicorn's lines and the notifications, not FIRE's own `INFO` log lines.

Tentacle (step 1.3; each source works once its step lands). It sends to `FIRE_BODY_URL`
(`http://127.0.0.1:8000` in `.env.example`) with `FIRE_API_TOKEN`. Sources whose variable is
unset, or whose kind has no reader yet, are skipped with a log line. The source variables are
fixed in `config/systems/omg.yml`.

The inbound proxies (step 1.6) need OMG's API and validation engine up (section a without the
collector is enough). `omg-api` listens on `:8120` in front of `:8020`, and `omg-validation`
on `:8130` in front of `:8030`. Each fetches its upstream's `/openapi.json` and logs
`templates loaded: source=omg-api templates=<n> dropped=0`; the other sources are skipped:

```bash
cd "$FIRE_REPO"
OMG_API_UPSTREAM=http://localhost:8020 OMG_VALIDATION_UPSTREAM=http://localhost:8030 \
  uv run python -m fire.apps.tentacle --system omg 2>&1 | tee /tmp/fire-tentacle.log
```

The replay alone needs neither OMG's stack nor AWS. It reads OMG's `WorkUnit` samples, logs
`rejected=missing_event_id:<n>` for the API bodies in the same directory, and exits `0` once
everything is sent; `/streams` then lists five `event:internal:…:payload` channels:

```bash
cd "$FIRE_REPO"
OMG_REPLAY_PATH="$OMG_REPO/scripts/test_payloads" \
  uv run python -m fire.apps.tentacle --system omg --source work-unit-samples
```

Every source at once, for the later steps:

```bash
cd "$FIRE_REPO"
export OMG_MANIFEST_PATH="$OMG_REPO/scripts/event_chains/event_chains_graph.json"
export OMG_REPLAY_PATH="$OMG_REPO/scripts/test_payloads"   # 1.3; non-WorkUnit files count as rejected
export OMG_API_UPSTREAM=http://localhost:8020               # 1.6; the proxy listens on :8120
export OMG_VALIDATION_UPSTREAM=http://localhost:8030        # 1.6; the proxy listens on :8130
export OMG_TRANSMISSION_BUCKET=order-management-gateway-transmission-data-dev   # 1.7
export OMG_EVENTS_STREAM=omg-dev-domain-events              # 1.8; needs KINESIS_ENABLED=true
export OMG_RX_UPDATES_STREAM=rx-updates                     # 1.8; needs KINESIS_ENABLED=true
export OMG_EVENTS_TABLE=omg-dev-order_domain_events         # 1.8
export OMG_CARRIER_EVENTS_TOPIC=omg-carrier-events-topic-dev     # 1.8
export OMG_FINANCIALS_EVENTS_TOPIC=omg-financials-topic-dev      # 1.8
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test         # 1.7, 1.8: LocalStack
uv run python -m fire.apps.tentacle --system omg 2>&1 | tee /tmp/fire-tentacle.log
lsof -nP -iTCP:8120 -iTCP:8130 -iTCP:4318 -sTCP:LISTEN     # in another terminal
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8120/healthcheck   # 200
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8130/healthcheck   # 200
```

## c. Generate traffic

OMG's targets run in its `python-builder` container and call `warby-omg-order-management-api:8000`
or LocalStack directly. They bypass the proxy: they give spans and captures, never inbound
request or response shapes. Run them from `$OMG_REPO`.

| Target | Exercises |
|--------|-----------|
| `make list-payloads` | Lists `scripts/test_payloads/*.json` |
| `make test-order` | `POST /v1/orders/place_order` with `test_order_payload.json` |
| `make test-order-springfield` | Same with `test_springfield_order_payload.json`, an order placed into Springfield (Helios) |
| `make test-order-custom PAYLOAD=<file>` | Same with any file in `scripts/test_payloads/` |
| `make test-sales-order-processor` | `sales_order_processor` Lambda with the `order.charged` WorkUnit; `batchItemFailures` without a matching order |
| `make test-fulfillment-processor` | `order-management-gateway-fulfillment-processor` with the `fulfillment.receive` WorkUnit |
| `make test-oracle-publisher PAYLOAD=<file>` | `order-management-gateway-oracle-publisher`; `PAYLOAD` is required |

Through the proxy (step 1.6). OMG reads its token from the `x-service-token` header. Locally
`config/dev.yml` sets `api_auth.require_auth: false`, so the header is optional; it keeps the
request in the form real callers send. OMG answers `201` on success.

```bash
cd "$OMG_REPO"
OMG_TOKEN=$(make -s jwt/generate/api SERVICE=neon | sed -n 's/^x-service-token: //p')
for p in test_order_payload.json test_springfield_order_payload.json \
    test_rma_place_order_payload.json test_rma_exchange_lower_value.json; do
  curl -sS -o /dev/null -w "$p %{http_code}\n" http://localhost:8120/v1/orders/place_order \
    -H 'Content-Type: application/json' -H "x-service-token: $OMG_TOKEN" \
    --data-binary @"scripts/test_payloads/$p"
done
H="X-FIRE-Token: $FIRE_API_TOKEN"
curl -s -H "$H" http://127.0.0.1:8000/v0/systems/omg/streams | jq '.streams[].key'
```

`/streams` lists `http:inbound:POST /v1/orders/place_order:request` and one `:response:`
stream per status class OMG answered. The proxy adds a `traceparent` to each request, so OMG's
`SERVER` span in Jaeger (<http://localhost:16686>) is a child of the span id the shapes carry.
The L2 test checks both, with the four payloads above, by querying Jaeger's API; it runs its
own proxy on a free port, so it needs no tentacle or body running:

```bash
cd "$FIRE_REPO"
OMG_REPO="$OMG_REPO" uv run pytest tests/integration/test_tentacle_proxy_omg.py \
  tests/unit/test_openapi_routes.py -m omg_local
```

## d. Phase 1 exit run (step 1.10)

With a and b running, run these, then the proxy loop from c:

```bash
cd "$OMG_REPO"
make test-order && make test-order-springfield
make test-sales-order-processor && make test-fulfillment-processor
# make test-oracle-publisher PAYLOAD=...   (no committed WorkUnit fits yet, see the TODO below)
H="X-FIRE-Token: $FIRE_API_TOKEN"
curl -s -H "$H" http://127.0.0.1:8000/v0/systems/omg/streams | jq .
curl -s -H "$H" http://127.0.0.1:8000/v0/systems/omg/paths | jq .
```

- TODO(1.10): choose a WorkUnit for `test-oracle-publisher`. None of OMG's five committed
  samples (`fulfillment.allocated`, `fulfillment.receive`, `order.charged`,
  `payments.ar_refund_ack`, `payments.oracle_refund_initiated`) is an event `oracle-publisher`
  consumes in the manifest.
- `/streams` lists `http:inbound:POST /v1/orders/place_order:request` and `:response:2xx`,
  outbound Oracle actions, and event types.
- `/paths` has `POST /v1/orders/place_order` reaching the DynamoDB write, the listener, SNS,
  the processors, and their partner calls.
- TODO(1.10): say how `audit_coverage` runs on observed event types before `/coverage` (2.5).

Canary order, with unique strings in every customer field and in the order ids:

```bash
cd "$OMG_REPO"
C=firecanary$(date +%s)
jq --arg c "$C" '.referenceOrderId = $c | .checkoutId = $c
  | .customer |= (.firstName = "\($c)fn" | .lastName = "\($c)ln"
      | .email = "\($c)@example.com" | .telephone = "\($c)tel")
  | .shippingAddress |= (.firstName = "\($c)sfn" | .lastName = "\($c)sln"
      | .streetAddress = "\($c)st" | .extendedAddress = "\($c)ext" | .locality = "\($c)city")' \
  scripts/test_payloads/test_order_payload.json > /tmp/fire-canary-order.json
curl -sS -o /dev/null -w '%{http_code}\n' http://localhost:8120/v1/orders/place_order \
  -H 'Content-Type: application/json' -H "x-service-token: $OMG_TOKEN" \
  --data-binary @/tmp/fire-canary-order.json
curl -s -H "$H" http://127.0.0.1:8000/v0/systems/omg/streams | grep -c "$C"   # 0
curl -s -H "$H" http://127.0.0.1:8000/v0/systems/omg/paths | grep -c "$C"     # 0
grep -c "$C" /tmp/fire-body.log /tmp/fire-tentacle.log                         # 0 each
```

If OMG rejects the order, the request still crossed the proxy and the check holds. OMG's own
`debug` exporter and Jaeger may show the canary; that is OMG's output, not FIRE's.

## e. Drift demo (step 2.6)

The inbound part needs sections a (OMG's API, no collector) and b (the body, started with
`PYTHONUNBUFFERED=1`, and the tentacle's proxies), on a freshly started body. Warm the request
stream past `drift.min_samples` (20 in `omg.yml`), toggling `adminId`, `adminName`, and
`partyId`, which `PlaceOrderRequest` declares optional:

```bash
cd "$OMG_REPO"
for i in $(seq 1 24); do
  jq --arg id "firewarm$(date +%s)$i" --argjson i "$i" '.referenceOrderId = $id | .checkoutId = $id
    | if $i % 2 == 0 then del(.adminId, .adminName) else . end
    | if $i % 3 == 0 then del(.partyId) else . end' scripts/test_payloads/test_order_payload.json \
  | curl -sS -o /dev/null -w '%{http_code} ' http://localhost:8120/v1/orders/place_order \
      -H 'Content-Type: application/json' -H "x-service-token: $OMG_TOKEN" --data-binary @-
done; echo
H="X-FIRE-Token: $FIRE_API_TOKEN"
curl -s -H "$H" http://127.0.0.1:8000/v0/systems/omg/streams \
  | jq -c '.streams[] | select(.key | contains("place_order")) | {key, status, samples, drift: (.drift | length)}'
```

OMG answers `201 201 422`, repeated: a business rule rejects an order without `partyId`
(`Order requires a party_id`). The request stream folds all 24, so `/streams` shows
`…place_order:request` `warm` with 24 samples and no drift signal, `:response:2xx` and
`:response:4xx` in `baseline rebuilding`, and the body printed no notification. Then remove the
required `timezone` and send `referenceOrderId` as an integer:

```bash
jq 'del(.timezone) | .referenceOrderId = 12345 | .checkoutId = "firedrift"' \
  scripts/test_payloads/test_order_payload.json \
  | curl -sS -o /dev/null -w '%{http_code}\n' http://localhost:8120/v1/orders/place_order \
      -H 'Content-Type: application/json' -H "x-service-token: $OMG_TOKEN" --data-binary @-
sleep 2; grep -c '^\[fire\] notify' /tmp/fire-body.log   # 1; the tentacle flushes every second
```

OMG answers `422`. `/tmp/fire-body.log` shows exactly one notification, at once:

```text
[fire] notify incident=<uuid> system=omg trigger=shape_drift
*FIRE ENGINE* incident `<uuid>` (`omg`)
Confidence: 20% — `noul` (do not treat as settled cause)
Possibilities:
• (50%) Shape drift on http:inbound:POST /v1/orders/place_order:request: referenceOrderId changed string -> integer; timezone (string) removed _[shape_drift]_
• (50%) Insufficient evidence (MISSING fragments) _[process_path]_
Shape hash: `<schema_hash>`
_Narrator: template stub (swap OpenAI-compat via FIRE_NARRATOR_*)._
```

`/streams` lists the signal, with both paths, under the request stream's `drift`. Posting the
same payload again adds an occurrence to the signal, not a second notification. Its `422` goes
to the `:response:4xx` stream, still rebuilding, and raises nothing. Warm up once before the
drifted payload: after three loops the `party_id` errors alone warm `:response:4xx`, and the
drifted payload's `422`, shaped differently, would notify for that channel too.

L1 replays need no OMG stack. TODO(2.6): the replay commands for `corpora/outbound/` (an Oracle
response that lost a field: one notification for that partner action) and `corpora/path/` (the
place-order trace without its Oracle call: one path drift notification).

## f. Stop and clean up

```bash
# Ctrl-C the tentacle and the body, then:
cd "$OMG_REPO"
make docker/up/otel   # back on OMG's config; the docker inspect from a shows its file
make docker/stop      # or make docker/down to remove the containers
rm -f /tmp/fire-body.log /tmp/fire-tentacle.log /tmp/fire-canary-order.json
```

## Caveats

- OMG's clients call partner staging services locally (`config/dev.yml`), and product reads hit
  the stage catalog table. Every target above reaches staging; outbound drift is tested at L1.
- Local Kinesis is off unless `KINESIS_ENABLED=true` at `tf/local/apply`.
- FIRE's exporter is added, never substituted: Jaeger keeps every trace, and step 1.4 checks it.
- OMG's `make docker/up/otel`, `docker/up/infrastructure`, and `docker/up` recreate the
  collector without the override; rerun the command in a.
- What differs on AWS (L3: inbound bodies, span delivery, stream readers) is out of scope.

## Troubleshooting

- **The collector cannot reach the tentacle.** Its logs show `otlphttp/fire` export errors. Test
  from OMG's network with `docker run --rm --network warbyparker --add-host host.docker.internal:host-gateway curlimages/curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'Content-Type: application/json' -d '{}' http://host.docker.internal:4318/v1/traces`.
  On Linux the tentacle must listen on `0.0.0.0`, not `127.0.0.1`.
- **A tentacle port is busy.** The tentacle exits `2` with `cannot listen on 127.0.0.1:8120`;
  `lsof -nP -iTCP:8120 -iTCP:8130 -iTCP:4318 -sTCP:LISTEN` names the process. Without `sudo`,
  `lsof` misses sockets held by `launchd`; `netstat -anv -p tcp | grep LISTEN` shows them. On
  macOS, `launchd` can hold `localhost:8021` for `com.apple.ftp-proxy`, which is why the proxies
  listen on `:8120` and `:8130`.
- **No inbound streams.** The counters line shows `skipped=templates_unavailable:<n>` while the
  proxy cannot fetch its upstream's `/openapi.json` (the log says `templates unavailable` once,
  then it retries every 5 s), and `unmatched_route:<n>` for paths OMG does not declare. A
  `502` with `x-fire-proxy: upstream-unreachable` means OMG's port is not answering.
- **401 from FIRE.** Body and caller hold different `FIRE_API_TOKEN` values; an exported
  variable wins over `.env`. A `503` means the body has no token.
- **500 from `place_order` with `NoSuchBucket` `federation` in the API's log.** OMG's compose
  sets `AWS_ENDPOINT_URL` for the whole API container, so botocore sends the SSO credential
  call (`GetRoleCredentials`, needed for the stage catalog read) to LocalStack, and
  `aws sso login` alone does not help. Without editing OMG, log in, then recreate only the API
  with an override outside both repos that sends SSO calls back to AWS:

  ```bash
  aws sso login --profile <your-profile>
  cat > ~/omg-sso.override.yml <<'EOF'
  services:
    order-management-api:
      environment:
        AWS_ENDPOINT_URL_SSO: https://portal.sso.us-east-1.amazonaws.com
        AWS_ENDPOINT_URL_SSO_OIDC: https://oidc.us-east-1.amazonaws.com
  EOF
  cd "$OMG_REPO"
  docker compose -f docker-compose.yml -f docker-compose.local.yml -f ~/omg-sso.override.yml \
    --profile omg up -d --no-deps order-management-api
  ```

  `make docker/up/omg` recreates the API without the override; run the last command again
  after it. When the SSO session expires, log in again. Until fixed, the `500` still crosses
  the proxy: it gives a `response:5xx` stream, and its trace still joins. After each such
  `500`, OMG closes the keep-alive
  connection a few milliseconds later without `Connection: close`, so the next request sent on
  it is lost: a direct client gets a connection error, and through the proxy it gets `502` with
  `x-fire-proxy: upstream-aborted` and counts `upstream_aborted`. The proxy does not retry,
  since OMG may already have run the request; the L2 test posts again.
- **401 or 403 from OMG.** Local auth is off, so a `401` means the API started without
  `docker-compose.local.yml`, which mounts `config/dev.yml`. Partner `403`s and ECR errors mean
  expired SSO: `aws sso login --profile <your-profile>`, then `make secrets/sync`.
- **No Kinesis streams.** If `docker exec warby-omg-localstack aws --endpoint-url http://localhost:4566 --region us-east-1 kinesis list-streams`
  is empty, run `KINESIS_ENABLED=true make tf/local/apply`.
