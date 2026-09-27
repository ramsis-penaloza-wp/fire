"""Deterministic local Reasoner for demos — not a generative LLM."""

from __future__ import annotations

from fire.core.domain.decision import Decision, Possibility
from fire.core.domain.dossier import Dossier, EvidenceQuality


class EchoReasoner:
    """Builds dynamic options from process-path hops + missing evidence."""

    async def decide(self, dossier: Dossier) -> Decision:
        possibilities: list[Possibility] = []
        for index, hop in enumerate(dossier.process_path.hops):
            possibilities.append(
                Possibility(
                    id=f"hop-{index}",
                    label=f"Failure near {hop.service}"
                    + (f"/{hop.operation}" if hop.operation else ""),
                    probability=0.0,
                    rationale_refs=[f"process_path:{hop.service}"],
                )
            )
        if dossier.missing:
            possibilities.append(
                Possibility(
                    id="insufficient-evidence",
                    label="Insufficient evidence (MISSING fragments)",
                    probability=0.0,
                    rationale_refs=[f.collector for f in dossier.missing],
                )
            )
        if not possibilities:
            possibilities.append(
                Possibility(
                    id="unknown",
                    label="No path/shape evidence yet",
                    probability=1.0,
                    rationale_refs=[],
                )
            )

        n = len(possibilities)
        for possibility in possibilities:
            possibility.probability = round(1.0 / n, 4)

        missing_heavy = any(f.quality is EvidenceQuality.MISSING for f in dossier.missing)
        has_structure = dossier.shape is not None or dossier.fingerprint is not None
        confidence = 0.2 if missing_heavy or not has_structure else 0.45
        noul = confidence < 0.3 or not has_structure

        return Decision(
            incident_id=dossier.incident_id,
            possibilities=possibilities,
            confidence=confidence,
            noul=noul,
            primitive="noul" if noul else "choice",
        )
