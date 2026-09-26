"""Echo reasoner builds possibilities from process path."""

from __future__ import annotations

import pytest

from fire.adapters.reasoners.echo import EchoReasoner
from fire.core.domain.dossier import (
    Dossier,
    ProcessHop,
    ProcessPath,
    ShapeFingerprint,
)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_reasoner_from_path() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        system="example",
        shape=ShapeFingerprint(schema_hash="abc", field_paths=["order.id"]),
        process_path=ProcessPath(
            hops=[
                ProcessHop(service="gateway", operation="POST /orders"),
                ProcessHop(service="billing", operation="charge"),
            ]
        ),
    )
    decision = await EchoReasoner().decide(dossier)
    assert decision.incident_id == "inc-1"
    assert len(decision.possibilities) == 2
    assert abs(sum(p.probability for p in decision.possibilities) - 1.0) < 1e-6
    assert decision.noul is False
