"""Drift incidents: a new shape drift signal, or a flow that fires a new path drift signal, opens
an incident with a dossier of shapes, paths, and counts.

Contract: docs/specs/ingest-drift-and-feedback.md#drift-incidents and
docs/specs/flows-and-paths.md#path-drift.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping, Sequence

from fire.core.domain.dossier import Dossier, EvidenceFragment, EvidenceQuality, ProcessPath
from fire.core.domain.drift import DriftSignal
from fire.core.domain.flow import Node
from fire.core.domain.incident import Incident, Trigger
from fire.core.domain.observation import ChannelKind, ShapeObservation, StreamSummary
from fire.core.domain.path import PathDrift, PathDriftFiring
from fire.core.ports.observation_catalog import ObservationCatalog
from fire.core.services.coverage import CoverageScope, audit_coverage


def _uuid4() -> str:
    return str(uuid.uuid4())


class DriftIncidentService:
    """Opens one incident per new shape drift signal, and one per flow that fires a new path
    drift signal, and assembles each dossier.

    A shape drift dossier holds the drifted observation's shape and fingerprint and four
    fragments, in order: `shape_drift`, `baseline`, `coverage`, and `process_path`. Evidence it
    cannot build is a `missing` fragment. `coverage` maps each loaded system to its
    `CoverageScope`. A path drift dossier holds the flow's process path and one `path_drift`
    fragment per signal the flow fired.
    """

    def __init__(
        self,
        catalog: ObservationCatalog,
        coverage: Mapping[str, CoverageScope],
        new_id: Callable[[], str] = _uuid4,
    ) -> None:
        self._catalog = catalog
        self._coverage = coverage
        self._new_id = new_id

    def open_shape_drift(
        self, observation: ShapeObservation, signal: DriftSignal
    ) -> tuple[Incident, Dossier]:
        streams = self._catalog.streams(observation.system)
        incident = Incident(
            id=self._new_id(),
            system=observation.system,
            trigger=Trigger.SHAPE_DRIFT,
            summary=f"Shape drift on {observation.channel.key}",
            started_at=signal.first_seen,
        )
        dossier = Dossier(
            incident_id=incident.id,
            system=observation.system,
            shape=observation.shape,
            fingerprint=observation.fingerprint,
            process_path=ProcessPath(),
            fragments=[
                _shape_drift(observation, signal),
                _baseline(observation, streams),
                _coverage(self._coverage.get(observation.system), streams),
                EvidenceFragment(
                    collector="process_path",
                    quality=EvidenceQuality.MISSING,
                    summary="No complete flow",
                ),
            ],
        )
        return incident, dossier

    def open_path_drift(self, drift: PathDrift) -> tuple[Incident, Dossier]:
        incident = Incident(
            id=self._new_id(),
            system=drift.system,
            trigger=Trigger.PATH_DRIFT,
            summary=f"Path drift on {drift.entry.operation}",
            started_at=drift.started_at,
        )
        dossier = Dossier(
            incident_id=incident.id,
            system=drift.system,
            process_path=drift.process_path.model_copy(deep=True),
            fragments=[_path_drift(drift.entry, firing) for firing in drift.firings],
        )
        return incident, dossier


def _path_drift(entry: Node, firing: PathDriftFiring) -> EvidenceFragment:
    signal = firing.signal
    dumped = signal.model_dump(mode="json")
    return EvidenceFragment(
        collector="path_drift",
        quality=EvidenceQuality.PRESENT,
        summary=f"{signal.rule}: {signal.node.operation} on {entry.operation}",
        data={
            "rule": dumped["rule"],
            "entry": entry.model_dump(mode="json"),
            "node": dumped["node"],
            "path_flows": firing.path_flows,
            "node_flows": firing.node_flows,
            "node_errors": firing.node_errors,
            "flow_spans": firing.flow_spans,
            "flow_errors": firing.flow_errors,
            "occurrences": signal.occurrences,
            "first_seen": dumped["first_seen"],
            "trace_ids": dumped["trace_ids"],
        },
    )


def _shape_drift(observation: ShapeObservation, signal: DriftSignal) -> EvidenceFragment:
    key = observation.channel.key
    diff = signal.diff
    dumped = signal.model_dump(mode="json")
    return EvidenceFragment(
        collector="shape_drift",
        quality=EvidenceQuality.PRESENT,
        summary=(
            f"Shape drift on {key}: added={len(diff.added)} "
            f"type_changed={len(diff.type_changed)} removed={len(diff.removed)}"
        ),
        data={
            "source": observation.source,
            "stream": key,
            "schema_hash": signal.schema_hash,
            "diff": dumped["diff"],
            "occurrences": signal.occurrences,
            "first_seen": dumped["first_seen"],
        },
    )


def _baseline(observation: ShapeObservation, streams: Sequence[StreamSummary]) -> EvidenceFragment:
    stream_ref = {"source": observation.source, "stream": observation.channel.key}
    summary = next(
        (
            stream
            for stream in streams
            if (stream.source, stream.key) == (observation.source, observation.channel.key)
        ),
        None,
    )
    if summary is None:
        return EvidenceFragment(
            collector="baseline",
            quality=EvidenceQuality.MISSING,
            summary="No baseline for this stream",
            data=stream_ref,
        )
    drifted = {signal.schema_hash for signal in summary.drift}
    folded = sum(
        1 for fingerprint in summary.fingerprints if fingerprint.schema_hash not in drifted
    )
    return EvidenceFragment(
        collector="baseline",
        quality=EvidenceQuality.PRESENT,
        summary=f"{summary.status}: {summary.samples} samples, {folded} fingerprints known",
        data={
            **stream_ref,
            "status": summary.status.value,
            "samples": summary.samples,
            "fingerprints": folded,
            "observations": summary.observations,
            "drift_signals": len(summary.drift),
        },
    )


def _coverage(scope: CoverageScope | None, streams: Sequence[StreamSummary]) -> EvidenceFragment:
    if scope is None:
        return EvidenceFragment(
            collector="coverage",
            quality=EvidenceQuality.MISSING,
            summary="No coverage scope for this system",
        )
    if scope.inventory is None:
        return EvidenceFragment(
            collector="coverage",
            quality=EvidenceQuality.MISSING,
            summary=scope.missing_reason,
        )
    observed = {
        stream.channel.name for stream in streams if stream.channel.kind is ChannelKind.EVENT
    }
    report = audit_coverage(
        scope.inventory, observed, critical=scope.critical, ignore=scope.ignore
    )
    return EvidenceFragment(
        collector="coverage",
        quality=EvidenceQuality.PRESENT,
        summary=(
            f"{len(report.declared_unseen)} declared events unseen, "
            f"{len(report.seen_undeclared)} seen undeclared, "
            f"{len(report.unknown_references)} unknown references"
        ),
        data=report.model_dump(mode="json"),
    )
