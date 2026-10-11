"""Port: where accepted observations are kept, each once, and summarized per stream key."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from fire.core.domain.drift import ShapeAdmission
from fire.core.domain.observation import ShapeObservation, SpanObservation, StreamSummary


@runtime_checkable
class ObservationCatalog(Protocol):
    """Stores each observation once, by its dedupe key; the first write wins.

    `add_shape` and `add_span` store an observation unless its dedupe key is known. Checking
    and storing a key is one atomic step, so concurrent tentacles, two regions, and resent
    batches store each observation once. `add_span` returns whether it stored the span.

    `add_shape` admits a stored shape to the baseline of its stream key in the same step, under
    the system's drift policy, and returns the outcome: `duplicate`, `folded`, `drift_raised`
    for the one occurrence that opens a drift signal, or `drift_repeated`. A drift outcome
    carries the signal as that occurrence left it.

    `streams` returns one summary per stream key of the system, sorted by `source`, then
    `key`, with the baseline's `samples`, its `status` under the system's drift policy, and
    its open drift signals; each summary's fingerprints are sorted by `observations`
    descending, then `schema_hash`. A system with no observations has none.

    Contract: docs/specs/observations.md#observationcatalog.
    """

    def add_shape(self, observation: ShapeObservation) -> ShapeAdmission: ...

    def add_span(self, observation: SpanObservation) -> bool: ...

    def streams(self, system: str) -> list[StreamSummary]: ...
