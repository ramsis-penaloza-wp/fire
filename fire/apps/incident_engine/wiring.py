"""Composition root — provider selection from config (v0: echo/template/console)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from fire.adapters.narrators.template import TemplateNarrator
from fire.adapters.notifiers.console import ConsoleNotifier
from fire.adapters.reasoners.echo import EchoReasoner
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.config import FireConfig, load_fire_config
from fire.core.ports.shape_fingerprinter import ShapeFingerprinter
from fire.core.services.diagnosis import DiagnosisService

DEFAULT_CONFIG = Path(__file__).resolve().parents[3] / "config" / "fire.yml"


@dataclass
class Container:
    config: FireConfig
    diagnosis: DiagnosisService
    fingerprinter: ShapeFingerprinter


def build_container(config_path: Path | None = None) -> Container:
    config = load_fire_config(config_path or DEFAULT_CONFIG)

    if config.reasoner.provider != "echo":
        raise ValueError(
            f"reasoner provider {config.reasoner.provider!r} not wired in v0 "
            "(use echo; laya/jev come next)"
        )
    if config.narrator.provider != "template":
        raise ValueError(
            f"narrator provider {config.narrator.provider!r} not wired in v0 "
            "(use template; openai_compat comes next)"
        )
    if config.notifier.provider != "console":
        raise ValueError(
            f"notifier provider {config.notifier.provider!r} not wired in v0 "
            "(use console; slack comes next)"
        )

    diagnosis = DiagnosisService(
        reasoner=EchoReasoner(),
        narrator=TemplateNarrator(),
        notifier=ConsoleNotifier(),
    )
    return Container(config=config, diagnosis=diagnosis, fingerprinter=Sha256ShapeFingerprinter())
