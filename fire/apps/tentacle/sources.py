"""Readers by source kind. A declared kind with no reader yet is skipped at startup."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

from fire.adapters.sources.replay import SUFFIXES, ReplayRecordSource
from fire.apps.config import ReplaySourceConfig, SourceConfig
from fire.core.ports.record_source import RecordSource, StartPosition

# Opens a source from its config and the value of its `location_env`.
type Reader = Callable[[SourceConfig, str, StartPosition], RecordSource]


class SourceUnavailable(Exception):
    """The source cannot be read as configured. The message names env vars, never their values."""


def _replay(config: SourceConfig, location: str, _start: StartPosition) -> RecordSource:
    if not isinstance(config, ReplaySourceConfig):
        raise TypeError(f"replay reader got a {config.kind} source")
    path = Path(location)
    if path.is_dir() or (path.is_file() and path.suffix in SUFFIXES):
        return ReplayRecordSource(config.id, path)
    raise SourceUnavailable(f"{config.path_env} names no .json or .jsonl file and no directory")


READERS: Mapping[str, Reader] = {"replay": _replay}
