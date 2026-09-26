"""Incident identity accepted at the edge (webhook/ingest)."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field


class Incident(BaseModel):
    """Multi-project incident identity — no payload fields."""

    id: str
    project_id: str
    service: str | None = None
    summary: str | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
