"""Composition root — provider selection from config (v0: echo/template/console)."""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from fire.adapters.manifests.event_chains_graph import EventChainsGraphParser
from fire.adapters.memory.observation_catalog import MemoryObservationCatalog
from fire.adapters.narrators.template import TemplateNarrator
from fire.adapters.notifiers.console import ConsoleNotifier
from fire.adapters.reasoners.echo import EchoReasoner
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.config import FireConfig, SystemConfig, load_fire_config, load_system_configs
from fire.apps.discovery import Discovery, check_manifest_kinds, discover
from fire.core.domain.drift import DriftSignal
from fire.core.domain.observation import ShapeObservation
from fire.core.domain.path import PathDrift, PathDriftRule
from fire.core.ports.manifest_parser import ManifestParser
from fire.core.ports.observation_catalog import ObservationCatalog
from fire.core.ports.shape_fingerprinter import ShapeFingerprinter
from fire.core.services.coverage import CoverageScope
from fire.core.services.diagnosis import DiagnosisService
from fire.core.services.drift_incident import DriftIncidentService
from fire.core.services.ingest import IngestService

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = Path(__file__).resolve().parents[3] / "config" / "fire.yml"


@dataclass
class Container:
    config: FireConfig
    diagnosis: DiagnosisService
    fingerprinter: ShapeFingerprinter
    catalog: ObservationCatalog
    ingest: IngestService
    drift_incidents: DriftIncidentService
    systems: dict[str, SystemConfig] = field(default_factory=dict)
    discoveries: dict[str, Discovery] = field(default_factory=dict)


def manifest_parsers() -> dict[str, ManifestParser]:
    parser = EventChainsGraphParser()
    return {parser.kind: parser}


def log_drift(observation: ShapeObservation, signal: DriftSignal) -> None:
    """One INFO line per new drift signal, with names and counts only: a path may hold data."""
    logger.info(
        "shape drift raised: system=%s source=%s stream=%s fingerprint=%s "
        "added=%d type_changed=%d removed=%d",
        observation.system,
        observation.source,
        observation.channel.key,
        signal.schema_hash,
        len(signal.diff.added),
        len(signal.diff.type_changed),
        len(signal.diff.removed),
    )


def log_path_drift(drift: PathDrift) -> None:
    """One INFO line per flow that fires a new path drift signal, with its entry and counts."""
    rules = Counter(firing.signal.rule for firing in drift.firings)
    logger.info(
        "path drift raised: system=%s service=%s entry=%s "
        "hop_added=%d hop_missing=%d hop_failing=%d new=%d",
        drift.system,
        drift.entry.service,
        drift.entry.operation,
        rules[PathDriftRule.HOP_ADDED],
        rules[PathDriftRule.HOP_MISSING],
        rules[PathDriftRule.HOP_FAILING],
        sum(firing.new for firing in drift.firings),
    )


def build_container(
    config_path: Path | None = None, clock: Callable[[], datetime] | None = None
) -> Container:
    """`clock` is the body's clock for flows, the current UTC time unless a test injects one."""
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
    fingerprinter = Sha256ShapeFingerprinter()
    drift_policies = {name: system.drift for name, system in systems.items()}
    path_policies = {name: system.paths for name, system in systems.items()}
    catalog = MemoryObservationCatalog(drift_policies, path_policies=path_policies, clock=clock)
    sources = {name: system.source_ids() for name, system in systems.items()}
    discoveries = {name: discover(system, parsers) for name, system in systems.items()}
    scopes = {
        name: CoverageScope(
            inventory=discoveries[name].inventory,
            missing_reason=discoveries[name].missing_reason,
            critical=tuple(system.policy.critical),
            ignore=tuple(system.policy.ignore),
        )
        for name, system in systems.items()
    }
    return Container(
        config=config,
        diagnosis=diagnosis,
        fingerprinter=fingerprinter,
        catalog=catalog,
        ingest=IngestService(catalog, fingerprinter, sources),
        drift_incidents=DriftIncidentService(catalog, scopes),
        systems=systems,
        discoveries=discoveries,
    )
