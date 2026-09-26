"""FastAPI edge: auth required on mutating routes; 202 + background diagnosis."""

from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from fire.apps.env import load_fire_env
from fire.apps.incident_engine.wiring import Container, build_container
from fire.core.domain.dossier import (
    Dossier,
    EvidenceFragment,
    EvidenceQuality,
    ProcessHop,
    ProcessPath,
    ShapeFingerprint,
)
from fire.core.domain.incident import Incident

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


class DiagnoseRequest(BaseModel):
    system: str
    summary: str | None = None
    schema_hash: str | None = Field(
        default=None, description="Shape fingerprint hash — not raw payload."
    )
    field_paths: list[str] = Field(default_factory=list)
    process_hops: list[ProcessHop] = Field(default_factory=list)


def _require_token(x_fire_token: str | None, token_env: str) -> None:
    expected = os.environ.get(token_env)
    if not expected:
        raise HTTPException(
            status_code=503,
            detail=f"server misconfigured: {token_env} is not set",
        )
    if not x_fire_token or x_fire_token != expected:
        raise HTTPException(status_code=401, detail="invalid or missing X-FIRE-Token")


async def _run_pipeline(incident: Incident, dossier: Dossier) -> None:
    await get_container().diagnosis.run(incident, dossier)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v0/diagnose", status_code=202)
async def diagnose(
    body: DiagnoseRequest,
    background_tasks: BackgroundTasks,
    x_fire_token: str | None = Header(default=None, alias="X-FIRE-Token"),
) -> dict[str, Any]:
    container = get_container()
    _require_token(x_fire_token, container.config.api.token_env)

    incident_id = str(uuid.uuid4())
    incident = Incident(id=incident_id, system=body.system, summary=body.summary)
    shape = None
    if body.schema_hash:
        shape = ShapeFingerprint(
            schema_hash=body.schema_hash,
            field_paths=list(body.field_paths),
        )
    missing: list[EvidenceFragment] = []
    if shape is None:
        missing.append(
            EvidenceFragment(
                collector="shape",
                quality=EvidenceQuality.MISSING,
                summary="No shape fingerprint provided",
            )
        )
    dossier = Dossier(
        incident_id=incident_id,
        system=body.system,
        shape=shape,
        process_path=ProcessPath(hops=list(body.process_hops)),
        missing=missing,
    )
    background_tasks.add_task(_run_pipeline, incident, dossier)
    return {"status": "accepted", "incident_id": incident_id, "system": body.system}
