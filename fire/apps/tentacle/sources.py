"""Openers by source kind: readers for record sources, the inbound proxy, and the OTLP receiver.
A declared kind with none of them is skipped at startup.
"""

from __future__ import annotations

import socket
from collections.abc import Callable, Mapping
from pathlib import Path

import httpx

from fire.adapters.sources.http_proxy import HttpProxy
from fire.adapters.sources.listener import listen
from fire.adapters.sources.otlp import OtlpReceiver
from fire.adapters.sources.replay import SUFFIXES, ReplayRecordSource
from fire.apps.config import (
    HttpProxySourceConfig,
    OtlpSourceConfig,
    ReplaySourceConfig,
    SourceConfig,
)
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


def open_http_proxy(config: HttpProxySourceConfig, upstream: str) -> HttpProxy:
    """Checks the upstream URL and binds the listen address, so a busy port fails at startup."""
    try:
        parsed = httpx.URL(upstream)
    except httpx.InvalidURL:
        parsed = None
    if parsed is None or parsed.scheme not in {"http", "https"} or not parsed.host:
        raise SourceUnavailable(f"{config.upstream_env} is not an http or https URL")
    return HttpProxy(
        source=config.id,
        sock=_listen(config.listen_host, config.listen_port),
        upstream=upstream,
        openapi_path=config.openapi_path,
        openapi_refresh_s=config.openapi_refresh_s,
        max_body_bytes=config.max_body_bytes,
    )


def open_otlp_receiver(config: OtlpSourceConfig, address: str) -> OtlpReceiver:
    """Checks the listen address and binds it, so a busy port fails at startup."""
    host, _, port = address.rpartition(":")
    host = host.removeprefix("[").removesuffix("]") if host.startswith("[") else host
    if not host or not (port.isascii() and port.isdigit()) or not 0 < int(port) < 65536:
        raise SourceUnavailable(f"{config.listen_env} is not a host:port address")
    return OtlpReceiver(source=config.id, sock=_listen(host, int(port)))


def _listen(host: str, port: int) -> socket.socket:
    try:
        return listen(host, port)
    except OSError as error:
        reason = error.strerror or type(error).__name__
        raise SourceUnavailable(f"cannot listen on {host}:{port} ({reason})") from None
