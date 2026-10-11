"""ObservationCatalog in process memory. The dedupe index lives as long as the process."""

from __future__ import annotations

import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

from fire.core.domain.baseline import Baseline, DriftPolicy
from fire.core.domain.dossier import Shape
from fire.core.domain.observation import (
    Channel,
    FingerprintSummary,
    ShapeObservation,
    SpanObservation,
    StreamSummary,
)


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
    """One accepted shape observation, without its shape; flows join shapes to spans with it."""

    stream_key: tuple[str, str, str]
    schema_hash: str
    occurred_at: datetime
    observed_at: datetime
    region: str | None
    trace_id: str | None
    span_id: str | None
    work_id: str | None
    correlation_id: str | None


class MemoryObservationCatalog:
    """Thread-safe: one lock covers every read and write.

    `policies` maps a system to its drift policy; a system it does not name gets the defaults.
    """

    def __init__(self, policies: Mapping[str, DriftPolicy] | None = None) -> None:
        self._lock = threading.Lock()
        self._policies = dict(policies or {})
        self._default_policy = DriftPolicy()
        self._index: dict[tuple[str, str, str], _IndexEntry] = {}
        self._spans: dict[tuple[str, str, str], SpanObservation] = {}
        self._streams: dict[str, dict[tuple[str, str], _Stream]] = {}

    def add_shape(self, observation: ShapeObservation) -> bool:
        schema_hash = observation.fingerprint.schema_hash
        observed_at = observation.observed_at
        with self._lock:
            if observation.dedupe_key in self._index:
                return False
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
            stream.baseline.fold(schema_hash, observation.shape)
            return True

    def add_span(self, observation: SpanObservation) -> bool:
        with self._lock:
            if observation.dedupe_key in self._spans:
                return False
            self._spans[observation.dedupe_key] = observation
            return True

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
                )
                for (source, _key), stream in streams
            ]
