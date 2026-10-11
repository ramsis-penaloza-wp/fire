"""FastAPI edge: every /v0 route requires X-FIRE-Token; ingest and diagnose answer 202."""

from __future__ import annotations

import hmac
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from fire.apps.env import load_fire_env
from fire.apps.incident_engine.wiring import Container, build_container, log_drift
from fire.core.domain.dossier import (
    Dossier,
    EvidenceFragment,
    EvidenceQuality,
    ProcessHop,
    ProcessPath,
    Shape,
    ShapeFingerprint,
)
from fire.core.domain.drift import DriftSignal
from fire.core.domain.incident import Incident, Trigger
from fire.core.domain.observation import ShapeObservation
from fire.core.domain.origin import Origin

logger = logging.getLogger(__name__)

_container: Container | None = None


def get_container() -> Container:
    if _container is None:
        raise RuntimeError("container not initialized")
    return _container


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _container
    # Process env wins over .env (override=False). Must run before auth/wiring
    # read os.environ.
    load_fire_env()
    _container = build_container()
    yield
    _container = None


app = FastAPI(title="FIRE ENGINE", version="0.1.0", lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def _validation_error(_request: Request, exc: RequestValidationError) -> JSONResponse:
    """A 422 lists each error's location and type, never the input that failed."""
    detail = [{"loc": list(error["loc"]), "type": error["type"]} for error in exc.errors()]
    return JSONResponse(status_code=422, content={"detail": detail})


class ObservationBatch(BaseModel):
    """Body of `POST /v0/observations`. Items are checked one by one by the ingest service."""

    model_config = ConfigDict(extra="forbid")

    shapes: list[Any] = Field(default_factory=list)
    spans: list[Any] = Field(default_factory=list)


class DiagnoseRequest(BaseModel):
    """Edge ingest — system-scoped; Origin + nested shape; hops by reference."""

    system: str
    service: str | None = None
    summary: str | None = None
    origin: Origin | None = None
    shape: Shape | None = None
    fingerprint: ShapeFingerprint | None = None
    process_hops: list[ProcessHop] = Field(default_factory=list)


def _require_token(x_fire_token: str | None, token_env: str) -> None:
    expected = os.environ.get(token_env)
    if not expected:
        raise HTTPException(
            status_code=503,
            detail=f"server misconfigured: {token_env} is not set",
        )
    # Compare bytes: compare_digest raises TypeError on non-ASCII str inputs.
    provided = (x_fire_token or "").encode()
    if not hmac.compare_digest(provided, expected.encode()):
        raise HTTPException(status_code=401, detail="invalid or missing X-FIRE-Token")


async def _run_pipeline(incident: Incident, dossier: Dossier) -> None:
    await get_container().diagnosis.run(incident, dossier)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/v0/observations",
    status_code=202,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": ObservationBatch.model_json_schema()}},
        }
    },
)
async def ingest_observations(
    request: Request,
    background_tasks: BackgroundTasks,
    x_fire_token: str | None = Header(default=None, alias="X-FIRE-Token"),
) -> dict[str, Any]:
    container = get_container()
    _require_token(x_fire_token, container.config.api.token_env)
    # Parsed here rather than declared as a parameter: FastAPI validates parameters before the
    # handler runs, and the token must be checked before the body.
    try:
        batch = ObservationBatch.model_validate_json(await request.body())
    except ValidationError as error:
        raise RequestValidationError(
            [
                {"loc": ("body", *detail["loc"]), "type": detail["type"]}
                for detail in error.errors(include_input=False, include_context=False)
            ]
        ) from None

    def open_drift_incident(observation: ShapeObservation, signal: DriftSignal) -> None:
        log_drift(observation, signal)
        incident, dossier = container.drift_incidents.open_shape_drift(observation, signal)
        background_tasks.add_task(_run_pipeline, incident, dossier)

    report = container.ingest.ingest(batch.shapes, batch.spans, on_drift=open_drift_incident)
    logger.info(
        "observations ingested: shapes accepted=%d duplicate=%d rejected=%d, "
        "spans accepted=%d duplicate=%d rejected=%d",
        report.shapes.accepted,
        report.shapes.duplicate,
        len(report.shapes.rejected),
        report.spans.accepted,
        report.spans.duplicate,
        len(report.spans.rejected),
    )
    return report.model_dump(mode="json", exclude_none=True)


@app.get("/v0/systems/{system}/streams")
async def streams(
    system: str,
    x_fire_token: str | None = Header(default=None, alias="X-FIRE-Token"),
) -> dict[str, Any]:
    container = get_container()
    _require_token(x_fire_token, container.config.api.token_env)
    if system not in container.systems:
        raise HTTPException(status_code=404, detail="unknown system")
    return {
        "system": system,
        "streams": [
            {
                "source": stream.source,
                "key": stream.key,
                "status": stream.status,
                "samples": stream.samples,
                "observations": stream.observations,
                "fingerprints": [
                    fingerprint.model_dump(mode="json") for fingerprint in stream.fingerprints
                ],
                "drift": [signal.model_dump(mode="json") for signal in stream.drift],
            }
            for stream in container.catalog.streams(system)
        ],
    }


@app.post("/v0/diagnose", status_code=202)
async def diagnose(
    body: DiagnoseRequest,
    background_tasks: BackgroundTasks,
    x_fire_token: str | None = Header(default=None, alias="X-FIRE-Token"),
) -> dict[str, Any]:
    container = get_container()
    _require_token(x_fire_token, container.config.api.token_env)
    if body.system not in container.systems:
        raise RequestValidationError([{"loc": ("body", "system"), "type": "unknown_system"}])
    shape = body.shape
    fingerprint = body.fingerprint
    if shape is not None:
        computed = container.fingerprinter.fingerprint(shape)
        if fingerprint is not None and fingerprint != computed:
            raise RequestValidationError(
                [{"loc": ("body", "fingerprint"), "type": "fingerprint_mismatch"}]
            )
        fingerprint = computed

    incident_id = str(uuid.uuid4())
    incident = Incident(
        id=incident_id,
        system=body.system,
        trigger=Trigger.MANUAL,
        service=body.service,
        summary=body.summary,
    )
    origin = body.origin
    fragments: list[EvidenceFragment] = []
    if origin is None:
        fragments.append(
            EvidenceFragment(
                collector="origin",
                quality=EvidenceQuality.MISSING,
                summary="No Origin provided",
            )
        )
    if shape is None and fingerprint is None:
        fragments.append(
            EvidenceFragment(
                collector="shape",
                quality=EvidenceQuality.MISSING,
                summary="No shape or fingerprint provided",
            )
        )
    dossier = Dossier(
        incident_id=incident_id,
        system=body.system,
        service=body.service,
        origin=origin,
        shape=shape,
        fingerprint=fingerprint,
        process_path=ProcessPath(hops=list(body.process_hops)),
        fragments=fragments,
    )
    background_tasks.add_task(_run_pipeline, incident, dossier)
    return {
        "status": "accepted",
        "incident_id": incident_id,
        "system": body.system,
        "service": body.service,
        "origin_id": origin.origin_id if origin else None,
    }
