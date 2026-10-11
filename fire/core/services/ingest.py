"""Ingest: check each observation of a batch, store those that pass, and count every outcome.

Contract: docs/specs/observations.md#ingest-endpoint.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence

from pydantic import BaseModel, ValidationError

from fire.core.domain.drift import Admission, DriftSignal
from fire.core.domain.ingest import IngestCounts, IngestReport, Rejection, RejectionReason
from fire.core.domain.observation import ShapeObservation, SpanObservation
from fire.core.ports.observation_catalog import ObservationCatalog
from fire.core.ports.shape_fingerprinter import ShapeFingerprinter

# Called once per new drift signal with the observation that raised it.
DriftHook = Callable[[ShapeObservation, DriftSignal], None]


class IngestService:
    """Checks items one by one, shapes first, each list in order; the first failing check decides.

    The checks: the item validates as its model, its system is loaded, its source is one of
    that system's sources, and for a shape the recomputed fingerprint equals the one sent. An
    item that passes is offered to the catalog, which counts it `accepted` or `duplicate`. A
    rejected item is not stored and holds no dedupe key.

    After the whole batch is counted, `on_drift` is called once for each drift signal the batch
    raised, in batch order. A later occurrence of a signal calls nothing.
    """

    def __init__(
        self,
        catalog: ObservationCatalog,
        fingerprinter: ShapeFingerprinter,
        sources: Mapping[str, Collection[str]],
        on_drift: DriftHook | None = None,
    ) -> None:
        """`sources` maps each loaded system to its source ids."""
        self._catalog = catalog
        self._fingerprinter = fingerprinter
        self._sources = sources
        self._on_drift = on_drift

    def ingest(self, shapes: Sequence[object], spans: Sequence[object]) -> IngestReport:
        raised: list[tuple[ShapeObservation, DriftSignal]] = []

        def add_shape(observation: ShapeObservation) -> bool:
            admission = self._catalog.add_shape(observation)
            if admission.outcome is Admission.DRIFT_RAISED and admission.signal is not None:
                raised.append((observation, admission.signal))
            return admission.stored

        report = IngestReport(
            shapes=_count(shapes, ShapeObservation, self._check_shape, add_shape),
            spans=_count(spans, SpanObservation, self._check_source, self._catalog.add_span),
        )
        if self._on_drift is not None:
            for observation, signal in raised:
                self._on_drift(observation, signal)
        return report

    def _check_source(
        self, observation: ShapeObservation | SpanObservation
    ) -> RejectionReason | None:
        sources = self._sources.get(observation.system)
        if sources is None:
            return RejectionReason.UNKNOWN_SYSTEM
        if observation.source not in sources:
            return RejectionReason.UNKNOWN_SOURCE
        return None

    def _check_shape(self, observation: ShapeObservation) -> RejectionReason | None:
        reason = self._check_source(observation)
        if reason is not None:
            return reason
        if self._fingerprinter.fingerprint(observation.shape) != observation.fingerprint:
            return RejectionReason.FINGERPRINT_MISMATCH
        return None


def _count[M: BaseModel](
    items: Sequence[object],
    model: type[M],
    check: Callable[[M], RejectionReason | None],
    add: Callable[[M], bool],
) -> IngestCounts:
    accepted = duplicate = 0
    rejected: list[Rejection] = []
    for index, item in enumerate(items):
        try:
            observation = model.model_validate(item)
        except ValidationError as error:
            rejected.append(
                Rejection(index=index, reason=RejectionReason.INVALID, fields=_locations(error))
            )
            continue
        reason = check(observation)
        if reason is not None:
            rejected.append(Rejection(index=index, reason=reason))
        elif add(observation):
            accepted += 1
        else:
            duplicate += 1
    return IngestCounts(accepted=accepted, duplicate=duplicate, rejected=rejected)


def _locations(error: ValidationError) -> list[str]:
    """Dotted error locations, each once, in order; never the input that failed."""
    dotted = (
        ".".join(str(part) for part in detail["loc"])
        for detail in error.errors(include_input=False, include_context=False)
    )
    return list(dict.fromkeys(location for location in dotted if location))
