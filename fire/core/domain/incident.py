"""Incident identity: the system it belongs to and what opened it. No payload fields."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class Trigger(StrEnum):
    """What opened an incident."""

    SHAPE_DRIFT = "shape_drift"
    PATH_DRIFT = "path_drift"
    INCIDENT_IO = "incident_io"
    HTTP_ERRORS = "http_errors"
    MANUAL = "manual"


class Incident(BaseModel):
    """One incident of one loaded system — no payload fields."""

    id: str
    system: str
    trigger: Trigger
    service: str | None = None
    summary: str | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
