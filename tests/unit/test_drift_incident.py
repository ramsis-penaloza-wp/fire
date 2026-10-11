"""DriftIncidentService: a new shape drift signal opens one incident and its dossier."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from fire.adapters.memory.observation_catalog import MemoryObservationCatalog
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.core.domain.baseline import DriftPolicy
from fire.core.domain.dossier import (
    EvidenceFragment,
    EvidenceQuality,
    ProcessPath,
    Shape,
    ShapeField,
    ValueType,
)
from fire.core.domain.drift import Admission, DriftSignal, FieldRef, ShapeDiff, TypeChange
from fire.core.domain.incident import Trigger
from fire.core.domain.inventory import DeclaredEvent, SystemInventory
from fire.core.domain.observation import Channel, ShapeObservation
from fire.core.services.coverage import CoverageScope
from fire.core.services.drift_incident import DriftIncidentService

SYSTEM = "shop"
SOURCE = "order-events"
CREATED = {"kind": "event", "direction": "internal", "name": "order.created", "part": "payload"}
PAID = {**CREATED, "name": "order.paid"}
HTTP = {"kind": "http", "direction": "inbound", "name": "POST /orders", "part": "request"}
STREAM = "event:internal:order.created:payload"
START = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
FINGERPRINTER = Sha256ShapeFingerprinter()
INCIDENT_ID = "incident-1"


def _shape_of(*fields: tuple[str, ValueType]) -> Shape:
    return Shape(
        fields=[ShapeField(path=path, value_type=value_type) for path, value_type in fields]
    )


BASELINE = _shape_of(("amount", ValueType.INTEGER), ("order_ref", ValueType.STRING))
WITH_NOTE = _shape_of(
    ("amount", ValueType.INTEGER), ("note", ValueType.STRING), ("order_ref", ValueType.STRING)
)
# Against the baseline: coupon added, order_ref an integer, amount removed.
DRIFTED = _shape_of(("coupon", ValueType.STRING), ("order_ref", ValueType.INTEGER))
NO_SCOPE: dict[str, CoverageScope] = {}


def _observation(
    observation_id: str,
    shape: Shape,
    *,
    source: str = SOURCE,
    channel: dict[str, Any] = CREATED,
    second: int = 0,
) -> ShapeObservation:
    at = START + timedelta(seconds=second)
    return ShapeObservation(
        system=SYSTEM,
        source=source,
        channel=Channel.model_validate(channel),
        observation_id=observation_id,
        occurred_at=at,
        observed_at=at,
        shape=shape,
        fingerprint=FINGERPRINTER.fingerprint(shape),
    )


def _inventory(*names: str) -> SystemInventory:
    return SystemInventory(
        system=SYSTEM,
        manifest_kind="event_chains_graph",
        events=[DeclaredEvent(name=name) for name in names],
    )


@pytest.fixture
def catalog() -> MemoryObservationCatalog:
    return MemoryObservationCatalog({SYSTEM: DriftPolicy(min_samples=2)})


@pytest.fixture
def drift(catalog: MemoryObservationCatalog) -> tuple[ShapeObservation, DriftSignal]:
    """Warms order.created on order-events beside three other streams, then drifts it.

    audit-log carries the same channel, and sorts first, so only its source tells it apart.
    """
    for observation in (
        _observation("base-0", BASELINE),
        _observation("base-1", WITH_NOTE, second=1),
        _observation("audit-0", BASELINE, source="audit-log"),
        _observation("paid-0", BASELINE, channel=PAID),
        _observation("http-0", BASELINE, source="api-proxy", channel=HTTP),
    ):
        catalog.add_shape(observation)
    drifted = _observation("drift-0", DRIFTED, second=10)
    admission = catalog.add_shape(drifted)
    assert admission.outcome is Admission.DRIFT_RAISED
    assert admission.signal is not None
    return drifted, admission.signal


def _service(
    catalog: MemoryObservationCatalog, coverage: dict[str, CoverageScope]
) -> DriftIncidentService:
    return DriftIncidentService(catalog, coverage, new_id=lambda: INCIDENT_ID)


def _fragment(fragments: list[EvidenceFragment], collector: str) -> EvidenceFragment:
    [fragment] = [fragment for fragment in fragments if fragment.collector == collector]
    return fragment


@pytest.mark.unit
def test_incident_is_a_shape_drift_on_the_stream_from_first_seen(
    catalog: MemoryObservationCatalog, drift: tuple[ShapeObservation, DriftSignal]
) -> None:
    observation, signal = drift

    incident, _dossier = _service(catalog, NO_SCOPE).open_shape_drift(observation, signal)

    assert incident.id == INCIDENT_ID
    assert incident.system == SYSTEM
    assert incident.trigger is Trigger.SHAPE_DRIFT
    assert incident.service is None
    assert incident.summary == f"Shape drift on {STREAM}"
    assert incident.started_at == signal.first_seen == START + timedelta(seconds=10)


@pytest.mark.unit
def test_dossier_carries_the_drifted_shape_and_four_fragments_in_order(
    catalog: MemoryObservationCatalog, drift: tuple[ShapeObservation, DriftSignal]
) -> None:
    observation, signal = drift

    _incident, dossier = _service(catalog, NO_SCOPE).open_shape_drift(observation, signal)

    assert dossier.incident_id == INCIDENT_ID
    assert dossier.system == SYSTEM
    assert (dossier.service, dossier.origin) == (None, None)
    assert dossier.shape == DRIFTED
    assert dossier.fingerprint == FINGERPRINTER.fingerprint(DRIFTED)
    assert dossier.process_path == ProcessPath()
    assert [(f.collector, f.quality) for f in dossier.fragments] == [
        ("shape_drift", EvidenceQuality.PRESENT),
        ("baseline", EvidenceQuality.PRESENT),
        ("coverage", EvidenceQuality.MISSING),
        ("process_path", EvidenceQuality.MISSING),
    ]


@pytest.mark.unit
def test_shape_drift_fragment_names_the_stream_and_its_diff(
    catalog: MemoryObservationCatalog, drift: tuple[ShapeObservation, DriftSignal]
) -> None:
    observation, signal = drift
    assert signal.diff == ShapeDiff(
        added=[FieldRef(path="coupon", value_type=ValueType.STRING)],
        type_changed=[
            TypeChange(path="order_ref", old=[ValueType.STRING], new=[ValueType.INTEGER])
        ],
        removed=[FieldRef(path="amount", value_type=ValueType.INTEGER)],
    )

    _incident, dossier = _service(catalog, NO_SCOPE).open_shape_drift(observation, signal)

    assert _fragment(dossier.fragments, "shape_drift") == EvidenceFragment(
        collector="shape_drift",
        quality=EvidenceQuality.PRESENT,
        summary=f"Shape drift on {STREAM}: added=1 type_changed=1 removed=1",
        data={
            "source": SOURCE,
            "stream": STREAM,
            "schema_hash": FINGERPRINTER.fingerprint(DRIFTED).schema_hash,
            "diff": {
                "added": [{"path": "coupon", "value_type": "string"}],
                "type_changed": [{"path": "order_ref", "old": ["string"], "new": ["integer"]}],
                "removed": [{"path": "amount", "value_type": "integer"}],
            },
            "occurrences": 1,
            "first_seen": "2026-10-11T16:30:10Z",
        },
    )


@pytest.mark.unit
def test_baseline_fragment_counts_only_folded_fingerprints_of_its_own_stream(
    catalog: MemoryObservationCatalog, drift: tuple[ShapeObservation, DriftSignal]
) -> None:
    observation, signal = drift

    _incident, dossier = _service(catalog, NO_SCOPE).open_shape_drift(observation, signal)

    assert _fragment(dossier.fragments, "baseline") == EvidenceFragment(
        collector="baseline",
        quality=EvidenceQuality.PRESENT,
        summary="warm: 2 samples, 2 fingerprints known",
        data={
            "source": SOURCE,
            "stream": STREAM,
            "status": "warm",
            "samples": 2,
            "fingerprints": 2,
            "observations": 3,
            "drift_signals": 1,
        },
    )


@pytest.mark.unit
def test_coverage_audits_event_channel_names_only(
    catalog: MemoryObservationCatalog, drift: tuple[ShapeObservation, DriftSignal]
) -> None:
    observation, signal = drift
    scope = CoverageScope(inventory=_inventory("order.created", "order.shipped"))

    _incident, dossier = _service(catalog, {SYSTEM: scope}).open_shape_drift(observation, signal)

    assert _fragment(dossier.fragments, "coverage") == EvidenceFragment(
        collector="coverage",
        quality=EvidenceQuality.PRESENT,
        summary="1 declared events unseen, 1 seen undeclared, 0 unknown references",
        data={
            "system": SYSTEM,
            "declared_unseen": ["order.shipped"],
            "seen_undeclared": ["order.paid"],
            "unknown_references": [],
        },
    )


@pytest.mark.unit
def test_coverage_honors_the_scopes_critical_and_ignore(
    catalog: MemoryObservationCatalog, drift: tuple[ShapeObservation, DriftSignal]
) -> None:
    observation, signal = drift
    scope = CoverageScope(
        inventory=_inventory("order.created", "order.shipped"),
        critical=("order.created", "order.gone"),
        ignore=("order.paid",),
    )

    _incident, dossier = _service(catalog, {SYSTEM: scope}).open_shape_drift(observation, signal)

    coverage = _fragment(dossier.fragments, "coverage")
    assert coverage.quality is EvidenceQuality.PRESENT
    assert coverage.summary == "1 declared events unseen, 0 seen undeclared, 1 unknown references"
    assert coverage.data == {
        "system": SYSTEM,
        "declared_unseen": ["order.shipped"],
        "seen_undeclared": [],
        "unknown_references": ["order.gone"],
    }


@pytest.mark.unit
def test_coverage_without_an_inventory_is_missing_with_the_scopes_reason(
    catalog: MemoryObservationCatalog, drift: tuple[ShapeObservation, DriftSignal]
) -> None:
    observation, signal = drift
    scope = CoverageScope(inventory=None, missing_reason="SHOP_MANIFEST_PATH is not set")

    _incident, dossier = _service(catalog, {SYSTEM: scope}).open_shape_drift(observation, signal)

    assert _fragment(dossier.fragments, "coverage") == EvidenceFragment(
        collector="coverage",
        quality=EvidenceQuality.MISSING,
        summary="SHOP_MANIFEST_PATH is not set",
    )


@pytest.mark.unit
def test_coverage_without_a_scope_for_the_system_is_missing(
    catalog: MemoryObservationCatalog, drift: tuple[ShapeObservation, DriftSignal]
) -> None:
    observation, signal = drift
    other = {"billing": CoverageScope(inventory=_inventory("invoice.created"))}

    _incident, dossier = _service(catalog, other).open_shape_drift(observation, signal)

    assert _fragment(dossier.fragments, "coverage") == EvidenceFragment(
        collector="coverage",
        quality=EvidenceQuality.MISSING,
        summary="No coverage scope for this system",
    )


@pytest.mark.unit
def test_process_path_is_missing_without_a_complete_flow(
    catalog: MemoryObservationCatalog, drift: tuple[ShapeObservation, DriftSignal]
) -> None:
    observation, signal = drift

    _incident, dossier = _service(catalog, NO_SCOPE).open_shape_drift(observation, signal)

    assert dossier.fragments[-1] == EvidenceFragment(
        collector="process_path",
        quality=EvidenceQuality.MISSING,
        summary="No complete flow",
    )


@pytest.mark.unit
def test_a_stream_the_catalog_does_not_hold_has_a_missing_baseline(
    catalog: MemoryObservationCatalog,
) -> None:
    observation = _observation("drift-0", DRIFTED)
    signal = DriftSignal(
        schema_hash=observation.fingerprint.schema_hash,
        diff=ShapeDiff(added=[FieldRef(path="coupon", value_type=ValueType.STRING)]),
        occurrences=1,
        first_seen=observation.observed_at,
        last_seen=observation.observed_at,
    )
    scope = CoverageScope(inventory=_inventory("order.created"))

    _incident, dossier = _service(catalog, {SYSTEM: scope}).open_shape_drift(observation, signal)

    assert dossier.fragments[1] == EvidenceFragment(
        collector="baseline",
        quality=EvidenceQuality.MISSING,
        summary="No baseline for this stream",
        data={"source": SOURCE, "stream": STREAM},
    )
    assert dossier.fragments[2].data["declared_unseen"] == ["order.created"]
