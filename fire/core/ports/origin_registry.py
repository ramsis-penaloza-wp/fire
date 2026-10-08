"""Port: look up an Origin by id — adapter owns storage."""

from __future__ import annotations

from typing import Protocol

from fire.core.domain.origin import Origin


class OriginRegistry(Protocol):
    async def get(self, origin_id: str) -> Origin | None: ...

    async def upsert(self, origin: Origin) -> None: ...
