"""IngestService: per-item checks in order, the first failing check decides."""

from __future__ import annotations

from typing import Any

import pytest

from fire.adapters.memory.observation_catalog import MemoryObservationCatalog
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.core.domain.dossier import Shape
from fire.core.domain.ingest import IngestCounts, Rejection, RejectionReason
from fire.core.services.ingest import IngestService

SOURCES = {"shop": frozenset({"order-events", "traces"}), "billing": frozenset()}
SHAPE = {"fields": [{"path": "order_id", "value_type": "string"}]}
FINGERPRINT = Sha256ShapeFingerprinter().fingerprint(Shape.model_validate(SHAPE)).model_dump()


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


@pytest.fixture
def catalog() -> MemoryObservationCatalog:
    return MemoryObservationCatalog()


@pytest.fixture
def service(catalog: MemoryObservationCatalog) -> IngestService:
    return IngestService(catalog, Sha256ShapeFingerprinter(), SOURCES)


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
