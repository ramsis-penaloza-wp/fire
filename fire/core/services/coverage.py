"""Coverage audit: what a system declares versus what FIRE observes."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from fire.core.domain.inventory import CoverageReport, SystemInventory


@dataclass(frozen=True)
class CoverageScope:
    """What one system's coverage audit needs: its inventory or why it has none, and its policy."""

    inventory: SystemInventory | None
    missing_reason: str | None = None
    critical: tuple[str, ...] = ()
    ignore: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (self.inventory is None) == (self.missing_reason is None):
            raise ValueError("a coverage scope needs exactly one of inventory and missing_reason")


def audit_coverage(
    inventory: SystemInventory,
    observed: Iterable[str],
    *,
    critical: Iterable[str] = (),
    ignore: Iterable[str] = (),
) -> CoverageReport:
    """Compare declared event names with observed ones.

    Ignored events are left out of both gap lists. A `critical` or `ignore` reference that is
    neither declared nor observed is reported, so config cannot go stale silently.
    """
    declared = inventory.event_names()
    seen = set(observed)
    ignored = set(ignore)
    references = set(critical) | ignored
    return CoverageReport(
        system=inventory.system,
        declared_unseen=sorted(declared - seen - ignored),
        seen_undeclared=sorted(seen - declared - ignored),
        unknown_references=sorted(references - declared - seen),
    )
