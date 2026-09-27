"""Port: ShapeRecord catalog keyed by project + origin — adapter owns storage."""

from __future__ import annotations

from typing import Protocol

from fire.core.domain.dossier import ShapeFingerprint, ShapeRecord


class ShapeCatalog(Protocol):
    async def get(
        self,
        project_id: str,
        origin_id: str,
        fingerprint: ShapeFingerprint | None = None,
    ) -> ShapeRecord | None: ...

    async def upsert(self, record: ShapeRecord) -> None: ...
