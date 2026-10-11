"""SystemInventory invariants and the declared-versus-observed coverage audit."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fire.core.domain.inventory import (
    DeclaredEvent,
    DeclaredIngress,
    DeclaredProcessor,
    SystemInventory,
)
from fire.core.services.coverage import audit_coverage


def _inventory(*names: str) -> SystemInventory:
    return SystemInventory(
        system="example",
        manifest_kind="event_chains_graph",
        events=[DeclaredEvent(name=name) for name in names],
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "field, items",
    [
        ("events", [DeclaredEvent(name="a"), DeclaredEvent(name="a")]),
        ("processors", [DeclaredProcessor(name="p"), DeclaredProcessor(name="p")]),
        ("ingress", [DeclaredIngress(id="i"), DeclaredIngress(id="i")]),
    ],
)
def test_inventory_rejects_duplicate_names(field: str, items: list) -> None:
    with pytest.raises(ValidationError, match="duplicate"):
        SystemInventory(system="example", manifest_kind="k", **{field: items})


@pytest.mark.unit
def test_inventory_rejects_empty_names() -> None:
    with pytest.raises(ValidationError):
        DeclaredEvent(name="")


@pytest.mark.unit
def test_full_coverage_is_complete() -> None:
    report = audit_coverage(_inventory("a", "b"), ["b", "a", "a"])
    assert report.is_complete
    assert report.system == "example"


@pytest.mark.unit
def test_reports_declared_unseen_and_seen_undeclared_sorted() -> None:
    report = audit_coverage(_inventory("c", "a", "b"), ["b", "z", "y"])
    assert report.declared_unseen == ["a", "c"]
    assert report.seen_undeclared == ["y", "z"]
    assert not report.is_complete


@pytest.mark.unit
def test_ignored_events_leave_both_gap_lists() -> None:
    report = audit_coverage(_inventory("a", "noisy"), ["a", "extra"], ignore=["noisy", "extra"])
    assert report.is_complete


@pytest.mark.unit
def test_references_to_unknown_names_are_reported() -> None:
    report = audit_coverage(
        _inventory("a"), ["seen"], critical=["a", "gone"], ignore=["seen", "renamed"]
    )
    assert report.unknown_references == ["gone", "renamed"]


@pytest.mark.unit
def test_nothing_observed_lists_every_declared_event() -> None:
    report = audit_coverage(_inventory("b", "a"), [])
    assert report.declared_unseen == ["a", "b"]
