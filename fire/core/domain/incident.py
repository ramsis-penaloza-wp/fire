"""Incident identity accepted at the edge (webhook/ingest)."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field


class Incident(BaseModel):
    id: str
    system: str
    summary: str | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
