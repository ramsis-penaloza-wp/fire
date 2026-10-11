"""Load each system's inventory from its declared manifest. Missing is declared, never guessed."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from fire.apps.config import SystemConfig
from fire.core.domain.inventory import SystemInventory
from fire.core.ports.manifest_parser import ManifestParser


@dataclass(frozen=True)
class Discovery:
    """`inventory` is None exactly when `missing_reason` says why."""

    system: str
    inventory: SystemInventory | None = None
    missing_reason: str | None = None


def check_manifest_kinds(
    systems: Mapping[str, SystemConfig], parsers: Mapping[str, ManifestParser]
) -> None:
    """Fail fast on a config naming a manifest format no parser handles."""
    for config in systems.values():
        manifest = config.discovery.manifest
        if manifest is not None and manifest.kind not in parsers:
            raise ValueError(
                f"system {config.system!r}: manifest kind {manifest.kind!r} has no parser "
                f"(known: {', '.join(sorted(parsers)) or 'none'})"
            )


def discover(
    config: SystemConfig,
    parsers: Mapping[str, ManifestParser],
    environ: Mapping[str, str] = os.environ,
) -> Discovery:
    manifest = config.discovery.manifest
    if manifest is None:
        return Discovery(config.system, missing_reason="No manifest declared (observation only)")
    location = environ.get(manifest.path_env)
    if not location:
        return Discovery(config.system, missing_reason=f"{manifest.path_env} is not set")
    path = Path(location)
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        return Discovery(config.system, missing_reason=f"Manifest unreadable: {error.strerror}")
    except json.JSONDecodeError as error:
        return Discovery(config.system, missing_reason=f"Manifest is not JSON: {error.msg}")
    try:
        inventory = parsers[manifest.kind].parse(config.system, document)
    except ValueError as error:
        return Discovery(config.system, missing_reason=f"Manifest rejected: {error}")
    return Discovery(config.system, inventory=inventory)
