"""Echo reasoner builds options from path + missing evidence."""

from __future__ import annotations

import pytest

from fire.adapters.reasoners.echo import EchoReasoner
from fire.core.domain.dossier import (
    Dossier,
    EvidenceFragment,
    EvidenceQuality,
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_missing_fragment_adds_insufficient_evidence_and_sets_noul() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        project_id="example",
        fingerprint=ShapeFingerprint(schema_hash="abc"),
        process_path=ProcessPath(hops=[ProcessHop(ref="api")]),
        fragments=[
            EvidenceFragment(
                collector="origin", quality=EvidenceQuality.MISSING, summary="No Origin provided"
            )
        ],
    )
    decision = await EchoReasoner().decide(dossier)
    insufficient = [p for p in decision.possibilities if p.id == "insufficient-evidence"]
    assert [p.rationale_refs for p in insufficient] == [["origin"]]
    assert decision.confidence == 0.2
    assert decision.noul is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_present_fragments_are_not_missing_evidence() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        project_id="example",
        fingerprint=ShapeFingerprint(schema_hash="abc"),
        process_path=ProcessPath(hops=[ProcessHop(ref="api")]),
        fragments=[
            EvidenceFragment(
                collector="shape", quality=EvidenceQuality.PRESENT, summary="Shape provided"
            )
        ],
    )
    decision = await EchoReasoner().decide(dossier)
    assert [p.id for p in decision.possibilities] == ["hop-0"]
    assert decision.confidence == 0.45
    assert decision.noul is False
