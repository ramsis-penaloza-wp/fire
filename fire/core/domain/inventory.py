"""What a watched system declares about itself: events, processors, ingress. Never values.

Discovered from the system's own manifest, never written by hand in config.
Contract: docs/specs/system-config-and-discovery.md.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

Name = Annotated[str, Field(min_length=1)]


class DeclaredField(BaseModel):
    """One field of a declared payload type: its name and the type the system names."""

    name: Name
    type: str


class DeclaredEvent(BaseModel):
    name: Name
    payload_type: str | None = None
    payload_fields: list[DeclaredField] = Field(default_factory=list)
    consumers: list[str] = Field(default_factory=list)


class DeclaredProcessor(BaseModel):
    name: Name
    region_policy: str | None = None


class DeclaredIngress(BaseModel):
    """An entry point from outside the system and the events it emits first."""

    id: Name
    label: str | None = None
    transport: str | None = None
    first_events: list[str] = Field(default_factory=list)


class SystemInventory(BaseModel):
    """Everything one manifest declares. Names are unique within each list."""

    model_config = ConfigDict(extra="forbid")

    system: Name
    manifest_kind: Name
    events: list[DeclaredEvent] = Field(default_factory=list)
    processors: list[DeclaredProcessor] = Field(default_factory=list)
    ingress: list[DeclaredIngress] = Field(default_factory=list)

    @model_validator(mode="after")
    def _require_unique_names(self) -> SystemInventory:
        for label, names in (
            ("event", [event.name for event in self.events]),
            ("processor", [processor.name for processor in self.processors]),
            ("ingress", [ingress.id for ingress in self.ingress]),
        ):
            duplicates = sorted({name for name in names if names.count(name) > 1})
            if duplicates:
                raise ValueError(f"duplicate {label} names: {', '.join(duplicates)}")
        return self

    def event_names(self) -> set[str]:
        return {event.name for event in self.events}


class CoverageReport(BaseModel):
    """Declared versus observed events, and config references to neither. Lists are sorted."""

    system: str
    declared_unseen: list[str] = Field(default_factory=list)
    seen_undeclared: list[str] = Field(default_factory=list)
    unknown_references: list[str] = Field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return not (self.declared_unseen or self.seen_undeclared or self.unknown_references)
