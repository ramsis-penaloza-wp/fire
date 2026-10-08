"""Echo reasoner builds options from path + missing evidence."""

from __future__ import annotations

import pytest

from fire.adapters.reasoners.echo import EchoReasoner
from fire.core.domain.dossier import (
    Dossier,
    ProcessHop,
    ProcessPath,
    Shape,
    ShapeField,
    ShapeFingerprint,
    ValueType,
)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_with_shape_and_fingerprint() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        project_id="example",
        service="checkout",
        shape=Shape(fields=[ShapeField(path="order.id", value_type=ValueType.STRING)]),
        fingerprint=ShapeFingerprint(schema_hash="abc", normalized_paths=["order.id"]),
        process_path=ProcessPath(hops=[ProcessHop(ref="api", operation="POST /orders")]),
    )
    decision = await EchoReasoner().decide(dossier)
    assert decision.noul is False
    assert decision.confidence >= 0.3
    assert any(p.id.startswith("hop-") for p in decision.possibilities)
