"""IngestService: per-item checks in order, the first failing check decides."""

from __future__ import annotations

from typing import Any

import pytest

from fire.adapters.memory.observation_catalog import MemoryObservationCatalog
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.core.domain.baseline import DriftPolicy
from fire.core.domain.dossier import Shape, ValueType
from fire.core.domain.drift import DriftSignal, FieldRef, ShapeDiff
from fire.core.domain.ingest import IngestCounts, Rejection, RejectionReason
from fire.core.domain.observation import ShapeObservation, SpanObservation
from fire.core.services.ingest import IngestService

SOURCES = {"shop": frozenset({"order-events", "traces"}), "billing": frozenset()}
SHAPE = {"fields": [{"path": "order_id", "value_type": "string"}]}
FINGERPRINT = Sha256ShapeFingerprinter().fingerprint(Shape.model_validate(SHAPE)).model_dump()

Call = tuple[ShapeObservation, DriftSignal]


def _shape(**overrides: Any) -> dict[str, Any]:
    item: dict[str, Any] = {
        "system": "shop",
        "source": "order-events",
        "channel": {
            "kind": "event",
            "direction": "internal",
            "name": "order.created",
            "part": "payload",
        },
        "observation_id": "obs-1",
        "occurred_at": "2026-10-11T16:30:00Z",
        "observed_at": "2026-10-11T16:30:01Z",
        "shape": SHAPE,
        "fingerprint": FINGERPRINT,
    }
    item.update(overrides)
    return item


def _span(**overrides: Any) -> dict[str, Any]:
    item: dict[str, Any] = {
        "system": "shop",
        "source": "traces",
        "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
        "span_id": "00f067aa0ba902b7",
        "service": "orders-api",
        "kind": "server",
        "operation": "POST /v1/orders",
        "error": False,
        "start": "2026-10-11T16:30:00Z",
        "duration_ms": 12.5,
    }
    item.update(overrides)
    return item


def _form(*paths: str) -> dict[str, Any]:
    shape = {"fields": [{"path": path, "value_type": "string"} for path in paths]}
    fingerprint = Sha256ShapeFingerprinter().fingerprint(Shape.model_validate(shape))
    return {"shape": shape, "fingerprint": fingerprint.model_dump()}


WARMUP = [_shape(observation_id=f"warm-{index}") for index in range(2)]
COUPON = _form("order_id", "coupon")


@pytest.fixture
def catalog() -> MemoryObservationCatalog:
    return MemoryObservationCatalog()


@pytest.fixture
def service(catalog: MemoryObservationCatalog) -> IngestService:
    return IngestService(catalog, Sha256ShapeFingerprinter(), SOURCES)


@pytest.fixture
def warm_catalog() -> MemoryObservationCatalog:
    return MemoryObservationCatalog({"shop": DriftPolicy(min_samples=len(WARMUP))})


@pytest.fixture
def calls() -> list[Call]:
    return []


@pytest.fixture
def hooked(warm_catalog: MemoryObservationCatalog, calls: list[Call]) -> IngestService:
    return IngestService(
        warm_catalog,
        Sha256ShapeFingerprinter(),
        SOURCES,
        on_drift=lambda observation, signal: calls.append((observation, signal)),
    )


def _rejected(reason: RejectionReason, index: int = 0) -> IngestCounts:
    return IngestCounts(rejected=[Rejection(index=index, reason=reason)])


MISMATCH = {**FINGERPRINT, "schema_hash": "0" * 64}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"system": "unknown", "fingerprint": MISMATCH}, RejectionReason.UNKNOWN_SYSTEM),
        ({"source": "unknown", "fingerprint": MISMATCH}, RejectionReason.UNKNOWN_SOURCE),
        ({"system": "billing", "source": "order-events"}, RejectionReason.UNKNOWN_SOURCE),
        ({"fingerprint": MISMATCH}, RejectionReason.FINGERPRINT_MISMATCH),
        ({"fingerprint": {**FINGERPRINT, "notes": "x"}}, RejectionReason.FINGERPRINT_MISMATCH),
        (
            {"fingerprint": {**FINGERPRINT, "normalized_paths": []}},
            RejectionReason.FINGERPRINT_MISMATCH,
        ),
    ],
    ids=[
        "system-before-fingerprint",
        "source-before-fingerprint",
        "source-of-another-system",
        "schema-hash",
        "notes",
        "normalized-paths",
    ],
)
def test_first_failing_shape_check_decides(
    service: IngestService,
    catalog: MemoryObservationCatalog,
    overrides: dict[str, Any],
    reason: RejectionReason,
) -> None:
    report = service.ingest([_shape(**overrides)], [])
    assert report.shapes == _rejected(reason)
    assert catalog.streams("shop") == []


@pytest.mark.unit
def test_fingerprint_mismatch_is_decided_before_duplicate(service: IngestService) -> None:
    report = service.ingest([_shape(), _shape(fingerprint=MISMATCH)], [])
    assert report.shapes == IngestCounts(
        accepted=1,
        rejected=[Rejection(index=1, reason=RejectionReason.FINGERPRINT_MISMATCH)],
    )


@pytest.mark.unit
def test_a_rejected_item_holds_no_dedupe_key(service: IngestService) -> None:
    assert service.ingest([_shape(fingerprint=MISMATCH)], []).shapes.accepted == 0
    assert service.ingest([_shape()], []).shapes == IngestCounts(accepted=1)


@pytest.mark.unit
def test_two_equal_items_in_one_batch_are_accepted_then_duplicate(
    service: IngestService,
) -> None:
    report = service.ingest([_shape(), _shape()], [_span(), _span()])
    assert report.shapes == IngestCounts(accepted=1, duplicate=1)
    assert report.spans == IngestCounts(accepted=1, duplicate=1)


@pytest.mark.unit
def test_invalid_item_lists_dotted_field_locations(service: IngestService) -> None:
    item = _shape(
        channel={"kind": "http", "direction": "inbound", "name": "GET /x", "part": "response"},
        shape={"fields": [{"path": "a", "value": "planted"}]},
    )
    report = service.ingest([item], [])
    assert report.shapes.rejected == [
        Rejection(
            index=0,
            reason=RejectionReason.INVALID,
            fields=["channel.status_class", "shape.fields.0.value"],
        )
    ]


@pytest.mark.unit
@pytest.mark.parametrize("item", ["text", 7, None, ["a"]], ids=["str", "int", "null", "list"])
def test_an_item_that_is_not_an_object_is_invalid_without_fields(
    service: IngestService, item: object
) -> None:
    report = service.ingest([item], [item])
    expected = [Rejection(index=0, reason=RejectionReason.INVALID, fields=[])]
    assert report.shapes.rejected == expected
    assert report.spans.rejected == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"system": "unknown"}, RejectionReason.UNKNOWN_SYSTEM),
        ({"source": "order-table"}, RejectionReason.UNKNOWN_SOURCE),
    ],
    ids=["unknown-system", "unknown-source"],
)
def test_span_checks(
    service: IngestService, overrides: dict[str, Any], reason: RejectionReason
) -> None:
    assert service.ingest([], [_span(**overrides)]).spans == _rejected(reason)


@pytest.mark.unit
def test_indexes_are_per_list(service: IngestService) -> None:
    report = service.ingest(
        [_shape(), _shape(observation_id="obs-2", system="unknown")],
        [_span(source="unknown"), _span()],
    )
    assert report.shapes == IngestCounts(
        accepted=1, rejected=[Rejection(index=1, reason=RejectionReason.UNKNOWN_SYSTEM)]
    )
    assert report.spans == IngestCounts(
        accepted=1, rejected=[Rejection(index=0, reason=RejectionReason.UNKNOWN_SOURCE)]
    )


@pytest.mark.unit
def test_rejection_fields_only_on_invalid() -> None:
    with pytest.raises(ValueError, match="fields"):
        Rejection(index=0, reason=RejectionReason.UNKNOWN_SOURCE, fields=["source"])
    with pytest.raises(ValueError, match="fields"):
        Rejection(index=0, reason=RejectionReason.INVALID)


# Drift hook


@pytest.mark.unit
def test_drift_hook_is_called_once_with_the_observation_that_raised_the_signal(
    hooked: IngestService, calls: list[Call]
) -> None:
    batch = [
        *WARMUP,
        _shape(observation_id="obs-2", **COUPON),
        _shape(observation_id="obs-3", **COUPON),
        _shape(observation_id="obs-4"),
        _shape(observation_id="obs-5", **COUPON),
    ]

    report = hooked.ingest(batch, [])

    assert report.shapes == IngestCounts(accepted=6)
    [(observation, signal)] = calls
    assert observation == ShapeObservation.model_validate(batch[2])
    assert signal == DriftSignal(
        schema_hash=COUPON["fingerprint"]["schema_hash"],
        diff=ShapeDiff(added=[FieldRef(path="coupon", value_type=ValueType.STRING)]),
        occurrences=1,
        first_seen=observation.observed_at,
        last_seen=observation.observed_at,
    )


@pytest.mark.unit
def test_a_later_batch_with_a_known_drift_calls_nothing(
    hooked: IngestService, calls: list[Call]
) -> None:
    hooked.ingest([*WARMUP, _shape(observation_id="obs-2", **COUPON)], [])
    assert len(calls) == 1

    report = hooked.ingest(
        [_shape(observation_id="obs-3", **COUPON), _shape(observation_id="obs-4", **COUPON)], []
    )

    assert report.shapes == IngestCounts(accepted=2)
    assert len(calls) == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "paths", [("coupon", "gift"), ("gift", "coupon")], ids=["coupon-first", "gift-first"]
)
def test_two_new_signals_in_one_batch_call_the_hook_in_batch_order(
    hooked: IngestService, calls: list[Call], paths: tuple[str, str]
) -> None:
    first, second = paths
    batch = [
        *WARMUP,
        _shape(observation_id=f"obs-{first}", **_form("order_id", first)),
        _shape(observation_id=f"obs-{first}-again", **_form("order_id", first)),
        _shape(observation_id=f"obs-{second}", **_form("order_id", second)),
    ]

    hooked.ingest(batch, [])

    assert [observation.observation_id for observation, _ in calls] == [
        f"obs-{first}",
        f"obs-{second}",
    ]
    assert [signal.diff for _, signal in calls] == [
        ShapeDiff(added=[FieldRef(path=path, value_type=ValueType.STRING)]) for path in paths
    ]


@pytest.mark.unit
def test_drift_hook_runs_after_the_whole_batch_is_counted(
    warm_catalog: MemoryObservationCatalog,
) -> None:
    seen: list[tuple[int, int, bool]] = []

    def hook(observation: ShapeObservation, signal: DriftSignal) -> None:
        [stream] = warm_catalog.streams("shop")
        span_stored = not warm_catalog.add_span(SpanObservation.model_validate(_span()))
        seen.append((stream.observations, stream.drift[0].occurrences, span_stored))

    service = IngestService(warm_catalog, Sha256ShapeFingerprinter(), SOURCES, on_drift=hook)
    service.ingest(
        [
            *WARMUP,
            _shape(observation_id="obs-2", **COUPON),
            _shape(observation_id="obs-3"),
            _shape(observation_id="obs-4", **COUPON),
        ],
        [_span()],
    )

    assert seen == [(5, 2, True)]


@pytest.mark.unit
def test_drifted_items_count_as_accepted_and_their_resends_as_duplicate(
    hooked: IngestService, calls: list[Call]
) -> None:
    drifted = _shape(observation_id="obs-2", **COUPON)
    report = hooked.ingest([*WARMUP, drifted, drifted], [])
    assert report.shapes == IngestCounts(accepted=3, duplicate=1)
    assert len(calls) == 1


@pytest.mark.unit
def test_a_rejected_drifted_item_calls_nothing(hooked: IngestService, calls: list[Call]) -> None:
    drifted = _shape(observation_id="obs-2", **{**COUPON, "fingerprint": MISMATCH})
    report = hooked.ingest([*WARMUP, drifted], [])
    assert report.shapes == IngestCounts(
        accepted=2, rejected=[Rejection(index=2, reason=RejectionReason.FINGERPRINT_MISMATCH)]
    )
    assert calls == []


@pytest.mark.unit
def test_drift_without_a_hook_is_still_recorded(warm_catalog: MemoryObservationCatalog) -> None:
    service = IngestService(warm_catalog, Sha256ShapeFingerprinter(), SOURCES)
    report = service.ingest([*WARMUP, _shape(observation_id="obs-2", **COUPON)], [])
    assert report.shapes == IngestCounts(accepted=3)
    [stream] = warm_catalog.streams("shop")
    assert [signal.occurrences for signal in stream.drift] == [1]
