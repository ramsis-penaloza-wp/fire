```
  ███████╗██╗██████╗ ███████╗     ███████╗███╗   ██╗ ██████╗ ██╗███╗   ██╗███████╗
  ██╔════╝██║██╔══██╗██╔════╝     ██╔════╝████╗  ██║██╔════╝ ██║████╗  ██║██╔════╝
  █████╗  ██║██████╔╝█████╗       █████╗  ██╔██╗ ██║██║  ███╗██║██╔██╗ ██║█████╗
  ██╔══╝  ██║██╔══██╗██╔══╝       ██╔══╝  ██║╚██╗██║██║   ██║██║██║╚██╗██║██╔══╝
  ██║     ██║██║  ██║███████╗     ███████╗██║ ╚████║╚██████╔╝██║██║ ╚████║███████╗
  ╚═╝     ╚═╝╚═╝  ╚═╝╚══════╝     ╚══════╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝╚═╝  ╚═══╝╚══════╝
```

**FIRE ENGINE — Failure Investigation & Root Explanation**

AI-assisted incident response that reasons over **payload shape** (never raw field
values) and the **process path** a request walked — which services, spans, and
changes it hit — then ranks **why it may have failed**.

Ingest → correlate → assemble a dossier (shape + path + context) → System One
reasoner → notifier. Target: calibrated possibility sets in under 30 seconds.

Panoptes stays untouched; this repo is the clean slate for that thesis.

## Purpose

Existing IR tools are weak at connecting “what the payload *looked like*” with
“which path through the system it took.” FIRE ENGINE is built for that gap:

1. **Ingest** structured trace/object streams from watched transaction systems,
   storing **fingerprints / shapes**, not secret values.
2. **Assemble** an incident dossier: shape drift, process path (span/service
   chain), change timeline, dependency health, visibility gaps, runbooks.
3. **Reason** with a **System One** model (default **Laya**, Jev-compatible) over
   that dossier only — shape + path → **dynamic calibrated possibilities**, never
   raw PII/payload bodies and never a free-form LLM essay as the happy path.
4. **Explain** the ranked possibilities (and optional remediation hints) to the
   on-call channel.

## Architecture

Hexagonal, with a strict dependency rule (detail in `docs/architecture.md` when
added):

- `fire/core/` — domain, ports, and services. **Zero knowledge of concrete
  watched systems and zero infra deps** (only `pydantic` / hashing).
- `fire/adapters/` — concrete port implementations (ingest, collectors, stores,
  reasoners, notifiers). Imports `core`.
- `fire/apps/` — runnable apps (incident engine). Imports both.
- `config/systems/` — one YAML per watched system; all per-system variability
  lives here, never in core.
- `config/runbooks/` — markdown runbooks indexed into the knowledge store.
- `infrastructure/` — Terraform (later).

```
apps  --imports-->  adapters  --imports-->  core        (never the other way)
```

### Pipeline (target)

Alert / ingest returns **202 at T=0**; diagnosis runs in background:

1. **Correlate** — collapse storm alerts into one incident window.
2. **Assemble dossier** — collectors fan out concurrently with per-tier budgets;
   missing evidence is declared `MISSING`, never invented.
3. **Reason** — System One (`choice` / `score` / `noul`) over shape + process
   path (+ changes, deps, runbooks) → calibrated possibility distribution.
4. **Notify** — console locally; Slack (or equivalent) when wired.

| Tier | Meaning | Default budget |
|------|---------|----------------|
| `TIER_0` | Internal reads (baseline, change ledger, visibility, deps) | ~300 ms |
| `TIER_1` | External pulls (vendor APIs / deep window capture) | ~3000 ms |
| `TIER_2` | Bonus context (runbook search / RAG) | ~1500 ms |

### Reasoner contract (non-negotiable)

- Input: **shape-only** payload evidence + **process path** (services/spans/changes).
  No raw field values.
- Engine: **System One**, not a full generative LLM on the happy path.
  - Default: **Laya** (open-weight, local; Jev-compatible wire `/v1/systemone`).
  - Alt: **Jev** hosted.
  - Primitives: `choice` / `score` / `noul` over the dossier.
  - Output: **dynamic calibrated possibilities** (distribution + confidence), not
    prose root-cause. Option sets for `choice` can be built per incident from the
    path/hypotheses.
  - `noul` is the privacy gate when evidence is insufficient or unsafe to score.
- Cascade: Laya first → low confidence → human and/or optional generative LLM
  escalation. Generative LLM is **not** the default reasoner.
- Local echo/heuristic stubs may stand in for demos behind the same port.

## Current state

Greenfield repo: private GitHub + this README. Next: minimal scaffold
(`pyproject`, package `fire`, empty ports for shape/path → System One reasoner),
then ingest + dossier + Laya/Jev adapter, then notifier (+ optional LLM escalate).

No copy of Panoptes source into this tree — ideas and layering only.
No published accuracy claims; no “FIRE benchmark” numbers in docs until measured.

## Development (planned)

Requires Python 3.13+, [uv](https://docs.astral.sh/uv/), Docker optional.

```bash
make dev/install        # uv sync
make lint               # ruff
make test               # pytest
make docker/up          # local engine on :8000
```

```bash
uv run python -m fire.apps.incident_engine
```

## HTTP API (planned)

Auth is **required by default** on mutating endpoints (`X-FIRE-Token` shared secret).
Missing/invalid token → `401`. No fail-open “secret optional” mode outside an
explicit local-dev override.

- `POST /webhooks/alerts` — generic alert webhook; 202 + background pipeline.
- `POST /webhooks/alerts/{source}` — vendor webhook (normalize + route).
- `POST /ingest/{system}` — continuous-plane tap (shape fingerprints only).
- `GET /health` — liveness (unauthenticated).

## Hard boundaries

- Work on `main` / local working tree unless an explicit branch action is asked.
- No ship (commit/push) without Approval Desk yes.
- Do not treat `$HOME` as a git root; clone lives under `Desktop/Proyects/fire`.
- Panoptes and WP production remotes are out of scope for edits from this project.
