"""System One decision: calibrated possibilities, not prose."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Possibility(BaseModel):
    """One dynamic hypothesis option (usually derived from the process path)."""

    id: str
    label: str
    probability: float = Field(ge=0.0, le=1.0)
    rationale_refs: list[str] = Field(
        default_factory=list,
        description="Fragment/collector ids cited — not free-form invention.",
    )


class Decision(BaseModel):
    """Output of the Reasoner (System One). Narrator consumes this; does not invent it."""

    incident_id: str
    possibilities: list[Possibility] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    noul: bool = Field(
        default=False,
        description="Privacy/insufficient-evidence gate: do not narrate as settled cause.",
    )
    primitive: str = Field(
        default="choice",
        description="System One primitive used: choice | score | noul.",
    )
