"""Port: JSON payload → Shape (form only). No fingerprint, no AI."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from fire.core.domain.dossier import Shape


@runtime_checkable
class ShapeGeneralizer(Protocol):
    """Structural generalizer — keys/types/cardinality only; never stores values."""

    def generalize(self, payload: Any) -> Shape: ...
