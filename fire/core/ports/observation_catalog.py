"""Port: where accepted observations are kept, each once, summarized per stream key, and
assembled into flows and paths.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from fire.core.domain.drift import ShapeAdmission
from fire.core.domain.observation import ShapeObservation, SpanObservation, StreamSummary
from fire.core.domain.path import PathDrift, PathsReport


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

    Every stored span joins its system's flows, and every stored shape with a trace or work id
    joins them too, on the catalog's clock at acceptance. `complete_flows` completes the flows
    of every system that settled by that clock, folds them into their paths or compares them,
    and returns one `PathDrift` per flow that fired a new path drift signal. `paths` returns a
    system's open flow count, counters, and paths; it completes nothing.

    Contract: docs/specs/observations.md#observationcatalog and docs/specs/flows-and-paths.md.
    """

    def add_shape(self, observation: ShapeObservation) -> ShapeAdmission: ...

    def add_span(self, observation: SpanObservation) -> bool: ...

    def streams(self, system: str) -> list[StreamSummary]: ...

    def complete_flows(self) -> list[PathDrift]: ...

    def paths(self, system: str) -> PathsReport: ...
