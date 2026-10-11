"""Port: where accepted observations are kept, each once, and summarized per stream key."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from fire.core.domain.observation import ShapeObservation, SpanObservation, StreamSummary


@runtime_checkable
class ObservationCatalog(Protocol):
    """Stores each observation once, by its dedupe key; the first write wins.

    `add_shape` and `add_span` store an observation unless its dedupe key is known and return
    whether they did. Checking and storing a key is one atomic step, so concurrent tentacles,
    two regions, and resent batches store each observation once.

    `streams` returns one summary per stream key of the system, sorted by `source`, then
    `key`; each summary's fingerprints are sorted by `observations` descending, then
    `schema_hash`. A system with no observations has none.

    Contract: docs/specs/observations.md#observationcatalog.
    """

    def add_shape(self, observation: ShapeObservation) -> bool: ...

    def add_span(self, observation: SpanObservation) -> bool: ...

    def streams(self, system: str) -> list[StreamSummary]: ...
