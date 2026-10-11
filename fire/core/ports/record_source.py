"""Port: where the tentacle reads raw records. Records never leave the tentacle process."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable


class StartPosition(StrEnum):
    """Where a live source starts: what is written after start, or what it still retains."""

    LATEST = "latest"
    OLDEST = "oldest"


async def _nothing_to_ack() -> None:
    return None


@dataclass(frozen=True)
class RawRecord:
    """One record as its source read it.

    `record` is the decoded JSON object, or None when the wire form did not decode to an
    object. `region` is where it was read, None for a source without regions. `ack` settles
    the record at its source, for example by deleting a queue message. Neither `record` nor
    `ack` appears in `repr`, so a logged record never carries its values.
    """

    source: str
    region: str | None
    record: Mapping[str, Any] | None = field(repr=False)
    ack: Callable[[], Awaitable[None]] = field(default=_nothing_to_ack, repr=False, compare=False)


@runtime_checkable
class RecordSource(Protocol):
    """The records of one declared source, in the order it reads them.

    A finite source, such as a replay, ends when it has read everything; a live source runs
    until its task is cancelled. Adapters live in `fire/adapters/sources/`.

    Contract: docs/specs/tentacle.md#record-pipeline.
    """

    def records(self) -> AsyncIterator[RawRecord]: ...
