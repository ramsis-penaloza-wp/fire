"""Composition root — provider selection from config (v0: echo/template/console)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from fire.adapters.manifests.event_chains_graph import EventChainsGraphParser
from fire.adapters.narrators.template import TemplateNarrator
from fire.adapters.notifiers.console import ConsoleNotifier
from fire.adapters.reasoners.echo import EchoReasoner
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.config import FireConfig, SystemConfig, load_fire_config, load_system_configs
from fire.apps.discovery import Discovery, check_manifest_kinds, discover
from fire.core.ports.manifest_parser import ManifestParser
from fire.core.ports.shape_fingerprinter import ShapeFingerprinter
from fire.core.services.diagnosis import DiagnosisService

DEFAULT_CONFIG = Path(__file__).resolve().parents[3] / "config" / "fire.yml"


@dataclass
class Container:
    config: FireConfig
    diagnosis: DiagnosisService
    fingerprinter: ShapeFingerprinter
    systems: dict[str, SystemConfig] = field(default_factory=dict)
    discoveries: dict[str, Discovery] = field(default_factory=dict)


def manifest_parsers() -> dict[str, ManifestParser]:
    parser = EventChainsGraphParser()
    return {parser.kind: parser}


def build_container(config_path: Path | None = None) -> Container:
    config_path = config_path or DEFAULT_CONFIG
    config = load_fire_config(config_path)

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

    systems = load_system_configs(config_path.parent / "systems")
    parsers = manifest_parsers()
    check_manifest_kinds(systems, parsers)

    diagnosis = DiagnosisService(
        reasoner=EchoReasoner(),
        narrator=TemplateNarrator(),
        notifier=ConsoleNotifier(),
    )
    return Container(
        config=config,
        diagnosis=diagnosis,
        fingerprinter=Sha256ShapeFingerprinter(),
        systems=systems,
        discoveries={name: discover(system, parsers) for name, system in systems.items()},
    )
