"""ObservationCatalog in process memory. The dedupe index, baselines, open flows, and paths live
as long as the process.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from fire.core.domain.baseline import Baseline, DriftPolicy
from fire.core.domain.dossier import Shape
from fire.core.domain.drift import Admission, ShapeAdmission
from fire.core.domain.flow import FlowAssembler, FlowCounters, Node, PathPolicy, ShapeRef
from fire.core.domain.observation import (
    Channel,
    FingerprintSummary,
    ShapeObservation,
    SpanObservation,
    StreamSummary,
)
from fire.core.domain.path import Path, PathDrift, PathsReport, settle


@dataclass
class _Fingerprint:
    shape: Shape
    observations: int
    first_observed_at: datetime
    last_observed_at: datetime


@dataclass
class _Stream:
    channel: Channel
    fingerprints: dict[str, _Fingerprint] = field(default_factory=dict)
    baseline: Baseline = field(default_factory=Baseline)


@dataclass(frozen=True)
class _IndexEntry:
    """One accepted shape observation, without its shape."""

    stream_key: tuple[str, str, str]
    schema_hash: str
    occurred_at: datetime
    observed_at: datetime
    region: str | None
    trace_id: str | None
    span_id: str | None
    work_id: str | None
    correlation_id: str | None


@dataclass
class _Flows:
    """One system's open flows and its paths, one per entry."""

    assembler: FlowAssembler
    paths: dict[Node, Path] = field(default_factory=dict)


def _utc_now() -> datetime:
    return datetime.now(UTC)


class MemoryObservationCatalog:
    """Thread-safe: one lock covers every read and write.

    `policies` maps a system to its drift policy and `path_policies` to its path policy; a
    system either does not name gets the defaults. `clock` is the body's clock, the current UTC
    time by default: it stamps each observation's acceptance and decides when flows settle.
    """

    def __init__(
        self,
        policies: Mapping[str, DriftPolicy] | None = None,
        *,
        path_policies: Mapping[str, PathPolicy] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._policies = dict(policies or {})
        self._default_policy = DriftPolicy()
        self._path_policies = dict(path_policies or {})
        self._clock = clock or _utc_now
        self._index: dict[tuple[str, str, str], _IndexEntry] = {}
        self._spans: dict[tuple[str, str, str], SpanObservation] = {}
        self._streams: dict[str, dict[tuple[str, str], _Stream]] = {}
        self._flows: dict[str, _Flows] = {}

    def _flows_of(self, system: str) -> _Flows:
        """The system's flows, created on first use; the caller holds the lock."""
        flows = self._flows.get(system)
        if flows is None:
            policy = self._path_policies.get(system, PathPolicy())
            flows = self._flows[system] = _Flows(FlowAssembler(policy))
        return flows

    def add_shape(self, observation: ShapeObservation) -> ShapeAdmission:
        schema_hash = observation.fingerprint.schema_hash
        observed_at = observation.observed_at
        policy = self._policies.get(observation.system, self._default_policy)
        now = self._clock()
        with self._lock:
            if observation.dedupe_key in self._index:
                return ShapeAdmission(outcome=Admission.DUPLICATE)
            self._index[observation.dedupe_key] = _IndexEntry(
                stream_key=observation.stream_key,
                schema_hash=schema_hash,
                occurred_at=observation.occurred_at,
                observed_at=observed_at,
                region=observation.region,
                trace_id=observation.trace_id,
                span_id=observation.span_id,
                work_id=observation.work_id,
                correlation_id=observation.correlation_id,
            )
            streams = self._streams.setdefault(observation.system, {})
            stream = streams.setdefault(
                (observation.source, observation.channel.key), _Stream(observation.channel)
            )
            seen = stream.fingerprints.get(schema_hash)
            if seen is None:
                stream.fingerprints[schema_hash] = _Fingerprint(
                    observation.shape, 1, observed_at, observed_at
                )
            else:
                seen.observations += 1
                seen.first_observed_at = min(seen.first_observed_at, observed_at)
                seen.last_observed_at = max(seen.last_observed_at, observed_at)
            admission = stream.baseline.admit(schema_hash, observation.shape, observed_at, policy)
            if observation.trace_id is not None or observation.work_id is not None:
                self._flows_of(observation.system).assembler.add_shape(
                    ShapeRef(
                        channel_key=observation.channel.key,
                        trace_id=observation.trace_id,
                        span_id=observation.span_id,
                        work_id=observation.work_id,
                    ),
                    now,
                )
            return admission

    def add_span(self, observation: SpanObservation) -> bool:
        now = self._clock()
        with self._lock:
            if observation.dedupe_key in self._spans:
                return False
            self._spans[observation.dedupe_key] = observation
            self._flows_of(observation.system).assembler.add_span(observation, now)
            return True

    def complete_flows(self) -> list[PathDrift]:
        now = self._clock()
        drifts: list[PathDrift] = []
        with self._lock:
            for system in sorted(self._flows):
                flows = self._flows[system]
                policy = self._policies.get(system, self._default_policy)
                for flow in flows.assembler.complete(now):
                    _outcome, drift = settle(
                        flow, flows.paths, policy, flows.assembler.counters, system
                    )
                    if drift is not None:
                        drifts.append(drift)
        return drifts

    def paths(self, system: str) -> PathsReport:
        policy = self._policies.get(system, self._default_policy)
        with self._lock:
            flows = self._flows.get(system)
            if flows is None:
                return PathsReport(system=system, open_flows=0, counters=FlowCounters(), paths=[])
            return PathsReport(
                system=system,
                open_flows=flows.assembler.open_flows,
                counters=flows.assembler.counters.model_copy(),
                paths=[
                    flows.paths[entry].summary(policy)
                    for entry in sorted(flows.paths, key=lambda entry: entry.sort_key)
                ],
            )

    def streams(self, system: str) -> list[StreamSummary]:
        policy = self._policies.get(system, self._default_policy)
        with self._lock:
            streams = sorted(self._streams.get(system, {}).items(), key=lambda item: item[0])
            return [
                StreamSummary(
                    source=source,
                    channel=stream.channel,
                    status=stream.baseline.status(policy),
                    samples=stream.baseline.samples,
                    fingerprints=sorted(
                        (
                            FingerprintSummary(
                                schema_hash=schema_hash,
                                observations=seen.observations,
                                first_observed_at=seen.first_observed_at,
                                last_observed_at=seen.last_observed_at,
                            )
                            for schema_hash, seen in stream.fingerprints.items()
                        ),
                        key=lambda summary: (-summary.observations, summary.schema_hash),
                    ),
                    drift=[
                        signal.model_copy(deep=True)
                        for signal in sorted(
                            stream.baseline.signals.values(),
                            key=lambda signal: (signal.first_seen, signal.schema_hash),
                        )
                    ],
                )
                for (source, _key), stream in streams
            ]
