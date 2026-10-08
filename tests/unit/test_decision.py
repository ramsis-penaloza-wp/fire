"""Decision contracts: typed primitive."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fire.core.domain.decision import Decision, Primitive


@pytest.mark.unit
def test_decision_primitive_defaults_to_choice() -> None:
    assert Decision(incident_id="inc-1").primitive is Primitive.CHOICE


@pytest.mark.unit
@pytest.mark.parametrize(
    ("value", "primitive"),
    [("choice", Primitive.CHOICE), ("score", Primitive.SCORE), ("noul", Primitive.NOUL)],
)
def test_decision_primitive_parses_wire_value(value: str, primitive: Primitive) -> None:
    assert Decision(incident_id="inc-1", primitive=value).primitive is primitive


@pytest.mark.unit
def test_decision_rejects_unknown_primitive() -> None:
    with pytest.raises(ValidationError, match="primitive"):
        Decision(incident_id="inc-1", primitive="vote")
