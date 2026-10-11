"""Deterministic local Reasoner for demos — not a generative LLM."""

from __future__ import annotations

from pydantic import BaseModel, ValidationError, model_validator

from fire.core.domain.decision import Decision, Possibility, Primitive
from fire.core.domain.dossier import Dossier, EvidenceFragment, EvidenceQuality
from fire.core.domain.drift import ShapeDiff


class _ShapeDriftData(BaseModel):
    stream: str
    diff: ShapeDiff

    @model_validator(mode="after")
    def _diff_names_a_change(self) -> _ShapeDriftData:
        if self.diff.is_empty:
            raise ValueError("a shape drift diff names at least one change")
        return self

    def label(self) -> str:
        changes = [
            *(f"{field.path} ({field.value_type}) added" for field in self.diff.added),
            *(
                f"{change.path} changed {'|'.join(change.old)} -> {'|'.join(change.new)}"
                for change in self.diff.type_changed
            ),
            *(f"{field.path} ({field.value_type}) removed" for field in self.diff.removed),
        ]
        return f"Shape drift on {self.stream}: " + "; ".join(changes)


class _Operation(BaseModel):
    operation: str


class _PathDriftData(BaseModel):
    rule: str
    entry: _Operation
    node: _Operation

    def label(self) -> str:
        return f"{self.rule}: {self.node.operation} on {self.entry.operation}"


_DRIFT_DATA: dict[str, type[_ShapeDriftData | _PathDriftData]] = {
    "shape_drift": _ShapeDriftData,
    "path_drift": _PathDriftData,
}


def _drift_label(fragment: EvidenceFragment) -> str:
    try:
        data = _DRIFT_DATA[fragment.collector].model_validate(fragment.data)
    except ValidationError:
        return fragment.summary
    return data.label()


class EchoReasoner:
    """Builds options from process-path hops, drift fragments, and missing evidence.

    Possibilities, in order: one per hop; one per present `shape_drift` or `path_drift`
    fragment, labelled from its `data` (its `summary` when `data` is malformed);
    `insufficient-evidence` when any fragment is missing; `unknown` when there is none of
    these. Probabilities split evenly. The decision is `noul` when any fragment is missing or
    the dossier has no shape, fingerprint, or drift possibility.
    """

    async def decide(self, dossier: Dossier) -> Decision:
        possibilities: list[Possibility] = []
        for index, hop in enumerate(dossier.process_path.hops):
            label = hop.display_label()
            possibilities.append(
                Possibility(
                    id=f"hop-{index}",
                    label=f"Failure near {label}",
                    probability=0.0,
                    rationale_refs=[f"process_path:{label}"],
                )
            )
        drift = [
            f
            for f in dossier.fragments
            if f.collector in _DRIFT_DATA and f.quality is EvidenceQuality.PRESENT
        ]
        for index, fragment in enumerate(drift):
            possibilities.append(
                Possibility(
                    id=f"drift-{index}",
                    label=_drift_label(fragment),
                    probability=0.0,
                    rationale_refs=[fragment.collector],
                )
            )
        missing = dossier.missing
        if missing:
            possibilities.append(
                Possibility(
                    id="insufficient-evidence",
                    label="Insufficient evidence (MISSING fragments)",
                    probability=0.0,
                    rationale_refs=[f.collector for f in missing],
                )
            )
        has_structure = dossier.shape is not None or dossier.fingerprint is not None
        if not possibilities:
            possibilities.append(
                Possibility(
                    id="unknown",
                    label=(
                        "No process path or drift evidence yet"
                        if has_structure
                        else "No process path, shape, or drift evidence yet"
                    ),
                    probability=1.0,
                    rationale_refs=[],
                )
            )

        probability = 1.0 / len(possibilities)
        for possibility in possibilities:
            possibility.probability = probability

        has_evidence = has_structure or bool(drift)
        confidence = 0.2 if missing or not has_evidence else 0.45
        noul = confidence < 0.3 or not has_evidence

        return Decision(
            incident_id=dossier.incident_id,
            possibilities=possibilities,
            confidence=confidence,
            noul=noul,
            primitive=Primitive.NOUL if noul else Primitive.CHOICE,
        )
