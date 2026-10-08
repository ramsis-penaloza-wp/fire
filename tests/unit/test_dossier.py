"""Dossier holds each fact in exactly one field."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fire.core.domain.dossier import Dossier


@pytest.mark.unit
def test_dossier_has_no_shape_record() -> None:
    assert "shape_record" not in Dossier.model_fields


@pytest.mark.unit
def test_dossier_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="shape_record"):
        Dossier(incident_id="inc-1", project_id="proj-a", shape_record=None)
