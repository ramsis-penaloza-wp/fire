"""Coverage audit: what a system declares versus what FIRE observes."""

from __future__ import annotations

from collections.abc import Iterable

from fire.core.domain.inventory import CoverageReport, SystemInventory


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
