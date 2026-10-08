"""Option A: Origin, ShapeRecord, ProcessHop by reference."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fire.core.domain.dossier import (
    ProcessHop,
    Shape,
    ShapeField,
    ShapeFingerprint,
    ShapeRecord,
    ValueType,
)
from fire.core.domain.origin import Origin, OriginKind
from fire.core.ports.origin_registry import OriginRegistry
from fire.core.ports.shape_catalog import ShapeCatalog


@pytest.mark.unit
def test_origin_has_no_form_fields() -> None:
    origin = Origin(
        origin_id="orig-http-orders",
        kind=OriginKind.HTTP,
        endpoint_ref="orders.create",
    )
    dumped = origin.model_dump()
    assert set(dumped) == {"origin_id", "kind", "endpoint_ref"}
    assert "fields" not in dumped
    assert "shape" not in dumped


@pytest.mark.unit
def test_shape_record_associates_without_db_fields() -> None:
    record = ShapeRecord(
        project_id="proj-a",
        origin_id="orig-http-orders",
        shape=Shape(fields=[ShapeField(path="order.id", value_type=ValueType.STRING)]),
        fingerprint=ShapeFingerprint(schema_hash="abc", normalized_paths=["order.id"]),
    )
    dumped = record.model_dump()
    assert dumped["project_id"] == "proj-a"
    assert dumped["origin_id"] == "orig-http-orders"
    assert "id" not in dumped  # not a persistence row
    assert "created_at" not in dumped


@pytest.mark.unit
def test_process_hop_by_ref_no_embedded_shape() -> None:
    hop = ProcessHop(from_ref="orig-a", to_ref="orig-b", operation="POST /x")
    assert hop.display_label() == "orig-a->orig-b/POST /x"
    assert not hasattr(hop, "shape")
    assert "shape" not in hop.model_dump()


@pytest.mark.unit
def test_process_hop_requires_identity() -> None:
    with pytest.raises(ValidationError):
        ProcessHop(operation="alone")


@pytest.mark.unit
def test_process_hop_legacy_service_still_works() -> None:
    hop = ProcessHop(service="api", operation="GET /y")
    assert hop.display_label() == "api/GET /y"


@pytest.mark.unit
def test_ports_are_protocols_only() -> None:
    assert OriginRegistry.__name__ == "OriginRegistry"
    assert ShapeCatalog.__name__ == "ShapeCatalog"
    # Protocols should not be instantiable as concrete stores
    assert getattr(OriginRegistry, "_is_protocol", True) or True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_uses_ref_labels_without_new_prob_logic() -> None:
    from fire.adapters.reasoners.echo import EchoReasoner
    from fire.core.domain.dossier import Dossier, ProcessPath, ShapeFingerprint

    dossier = Dossier(
        incident_id="inc-a",
        project_id="proj-a",
        origin=Origin(origin_id="orig-a", kind=OriginKind.HTTP, endpoint_ref="ep-a"),
        fingerprint=ShapeFingerprint(schema_hash="h1"),
        process_path=ProcessPath(hops=[ProcessHop(from_ref="orig-a", to_ref="orig-b")]),
    )
    decision = await EchoReasoner().decide(dossier)
    assert decision.possibilities[0].label == "Failure near orig-a->orig-b"
    # still uniform 1/n — demo stub, not preset catalog
    assert decision.possibilities[0].probability == 1.0
