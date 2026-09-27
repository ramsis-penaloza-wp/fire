"""DiagnosisService wires Reasoner → Narrator → Notifier."""

from __future__ import annotations

import pytest

from fire.adapters.narrators.template import TemplateNarrator
from fire.adapters.notifiers.console import ConsoleNotifier
from fire.adapters.reasoners.echo import EchoReasoner
from fire.core.domain.dossier import Dossier, ProcessHop, ProcessPath, ShapeFingerprint
from fire.core.domain.incident import Incident
from fire.core.services.diagnosis import DiagnosisService


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_runs(capsys: pytest.CaptureFixture[str]) -> None:
    service = DiagnosisService(
        reasoner=EchoReasoner(),
        narrator=TemplateNarrator(),
        notifier=ConsoleNotifier(),
    )
    incident = Incident(id="inc-2", project_id="example", service="api")
    dossier = Dossier(
        incident_id="inc-2",
        project_id="example",
        service="api",
        fingerprint=ShapeFingerprint(schema_hash="xyz"),
        process_path=ProcessPath(hops=[ProcessHop(service="api")]),
    )
    decision, narration = await service.run(incident, dossier)
    assert decision.incident_id == "inc-2"
    assert "inc-2" in narration.channel_text
    captured = capsys.readouterr()
    assert "inc-2" in captured.out
