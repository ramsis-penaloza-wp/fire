"""Template Narrator — formats Decision without calling an LLM."""

from __future__ import annotations

from fire.core.domain.decision import Decision
from fire.core.domain.dossier import Dossier
from fire.core.domain.narration import Narration


class TemplateNarrator:
    async def narrate(self, dossier: Dossier, decision: Decision) -> Narration:
        lines = [
            f"*FIRE ENGINE* incident `{decision.incident_id}` (`{dossier.system}`)",
            f"Confidence: {decision.confidence:.0%}"
            + (" — `noul` (do not treat as settled cause)" if decision.noul else ""),
            "Possibilities:",
        ]
        for possibility in decision.possibilities:
            refs = ", ".join(possibility.rationale_refs) or "—"
            lines.append(
                f"• ({possibility.probability:.0%}) {possibility.label} _[{refs}]_"
            )
        path = " → ".join(
            hop.service + (f":{hop.operation}" if hop.operation else "")
            for hop in dossier.process_path.hops
        )
        if path:
            lines.append(f"Process path: `{path}`")
        if dossier.shape is not None:
            lines.append(f"Shape hash: `{dossier.shape.schema_hash}`")
        lines.append("_Narrator: template stub (swap OpenAI-compat via FIRE_NARRATOR_*)._")
        return Narration(
            incident_id=decision.incident_id,
            channel_text="\n".join(lines),
            used_possibility_ids=[p.id for p in decision.possibilities],
        )
