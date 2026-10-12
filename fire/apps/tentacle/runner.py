"""The tentacle process: startup checks, then read, build, and send until the sources end or a
signal arrives. Contract: docs/specs/tentacle.md#process.

Logs hold source ids, env var names, counters, reason codes, and exception type names; never a
record, a payload, or the text of an exception raised while a record is in hand.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import signal
from collections.abc import Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import yaml

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.config import (
    EventSourceConfig,
    HttpProxySourceConfig,
    OtlpSourceConfig,
    SourceConfig,
    SystemConfig,
    TentacleConfig,
    load_fire_config,
    load_system_config,
)
from fire.apps.tentacle.counters import SourceCounters
from fire.apps.tentacle.sender import Outgoing, Sender, settle
from fire.apps.tentacle.sources import (
    READERS,
    SourceUnavailable,
    open_http_proxy,
    open_otlp_receiver,
)
from fire.core.domain.operation import OperationNamer
from fire.core.ports.exchange_source import (
    ExchangeNote,
    ExchangeSkipReason,
    ExchangeSource,
    HttpExchange,
)
from fire.core.ports.record_source import RecordSource, StartPosition
from fire.core.ports.span_source import RawSpan, SpanBatchResult, SpanRequestNote, SpanSource
from fire.core.services.exchange_pipeline import ExchangePipeline, PartSkipped
from fire.core.services.record_pipeline import RecordPipeline, Rejected, Skipped
from fire.core.services.span_pipeline import SpanPipeline

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = Path(__file__).resolve().parents[3] / "config" / "fire.yml"
EXIT_OK = 0
EXIT_UNSENT = 1
EXIT_STARTUP_ERROR = 2
COUNTERS_INTERVAL_S = 60.0
REQUEST_TIMEOUT_S = 10.0

_SYSTEM = re.compile(r"[a-z0-9][a-z0-9_-]*")


class StartupError(Exception):
    """A config or startup problem; the tentacle exits with code 2."""


@dataclass(frozen=True)
class PlannedSource:
    id: str
    records: RecordSource
    pipeline: RecordPipeline


@dataclass(frozen=True)
class PlannedProxy:
    id: str
    proxy: ExchangeSource
    pipeline: ExchangePipeline


@dataclass(frozen=True)
class PlannedReceiver:
    id: str
    receiver: SpanSource
    pipeline: SpanPipeline


type Planned = PlannedSource | PlannedProxy | PlannedReceiver


@dataclass(frozen=True)
class Plan:
    """What a checked startup runs: the sources to read and where to send."""

    system: str
    sources: list[Planned]
    tentacle: TentacleConfig
    url: str
    token: str


def prepare(
    system: str,
    *,
    source: str | None = None,
    start: StartPosition = StartPosition.LATEST,
    config_path: Path = DEFAULT_CONFIG,
    environ: Mapping[str, str] = os.environ,
) -> Plan:
    """Loads `fire.yml` and `systems/<system>.yml` next to it and opens the sources to run.

    Every source runs, or only the one `source` names. A source whose env var is unset, or
    whose kind has no reader yet, is logged and skipped unless `source` names it.

    Raises:
        StartupError: on a config error, an unknown `source`, an unset token or body URL env
            var, a named source that cannot run, or no source left to run.
    """
    fire_config = _load(load_fire_config, config_path)
    if _SYSTEM.fullmatch(system) is None:
        raise StartupError(f"system {system!r} is not a system id")
    system_path = config_path.parent / "systems" / f"{system}.yml"
    if not system_path.is_file():
        raise StartupError(f"no config for system {system!r}: {system_path} is missing")
    system_config: SystemConfig = _load(load_system_config, system_path)

    token = environ.get(fire_config.api.token_env)
    if not token:
        raise StartupError(f"{fire_config.api.token_env} is not set")
    url = _body_url(fire_config.tentacle.body_url_env, environ)

    declared = {config.id: config for config in system_config.sources}
    if source is not None and source not in declared:
        known = ", ".join(sorted(declared)) or "none"
        raise StartupError(f"system {system!r} declares no source {source!r} (declared: {known})")
    chosen = [declared[source]] if source is not None else list(declared.values())

    planned: list[Planned] = []
    try:
        for config in chosen:
            try:
                planned.append(_open(config, system_config, start, environ))
            except _Skip as skip:
                if source is not None:
                    raise StartupError(f"source {config.id}: {skip}") from None
                logger.warning("source skipped: source=%s reason=%s", config.id, skip)
    except StartupError:
        for opened in planned:
            if isinstance(opened, PlannedProxy):
                opened.proxy.close()
            elif isinstance(opened, PlannedReceiver):
                opened.receiver.close()
        raise
    if not planned:
        raise StartupError(f"system {system!r} has no source to run")
    return Plan(
        system=system_config.system,
        sources=planned,
        tentacle=fire_config.tentacle,
        url=url,
        token=token,
    )


class _Skip(Exception):
    """Why a source does not run; fatal only when `--source` names it."""


def _load[T](loader: Callable[[Path], T], path: Path) -> T:
    try:
        return loader(path)
    except (OSError, ValueError, yaml.YAMLError) as error:
        raise StartupError(f"{path.name}: {error}") from None


def _body_url(env: str, environ: Mapping[str, str]) -> str:
    base = environ.get(env)
    if not base:
        raise StartupError(f"{env} is not set")
    try:
        parsed = httpx.URL(base)
    except httpx.InvalidURL:
        parsed = None
    if parsed is None or parsed.scheme not in {"http", "https"} or not parsed.host:
        raise StartupError(f"{env} is not an http or https URL")
    return base.rstrip("/") + "/v0/observations"


def _open(
    config: SourceConfig,
    system: SystemConfig,
    start: StartPosition,
    environ: Mapping[str, str],
) -> Planned:
    location = environ.get(config.location_env)
    if not location:
        raise _Skip(f"{config.location_env} is not set")
    if isinstance(config, OtlpSourceConfig):
        try:
            receiver = open_otlp_receiver(config, location)
        except SourceUnavailable as error:
            raise StartupError(f"source {config.id}: {error}") from None
        spans = SpanPipeline(
            system=system.system,
            source=config.id,
            sample_rate=system.fingerprinting.sample_rate,
            namer=OperationNamer(system.operations),
            generalizer=FlatStarShapeGeneralizer(),
            fingerprinter=Sha256ShapeFingerprinter(),
        )
        return PlannedReceiver(id=config.id, receiver=receiver, pipeline=spans)
    if isinstance(config, HttpProxySourceConfig):
        try:
            proxy = open_http_proxy(config, location)
        except SourceUnavailable as error:
            raise StartupError(f"source {config.id}: {error}") from None
        exchanges = ExchangePipeline(
            system=system.system,
            source=config.id,
            sample_rate=system.fingerprinting.sample_rate,
            max_body_bytes=config.max_body_bytes,
            generalizer=FlatStarShapeGeneralizer(),
            fingerprinter=Sha256ShapeFingerprinter(),
        )
        return PlannedProxy(id=config.id, proxy=proxy, pipeline=exchanges)
    reader = READERS.get(config.kind)
    if reader is None:
        raise _Skip(f"kind {config.kind} has no reader yet")
    if not isinstance(config, EventSourceConfig):
        raise _Skip(f"kind {config.kind} has no record pipeline yet")
    try:
        records = reader(config, location, start)
    except SourceUnavailable as error:
        raise StartupError(f"source {config.id}: {error}") from None
    pipeline = RecordPipeline(
        system=system.system,
        source=config.id,
        direction=config.direction,
        envelope=config.envelope,
        sample_rate=system.fingerprinting.sample_rate,
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
    )
    return PlannedSource(id=config.id, records=records, pipeline=pipeline)


def run_tentacle(
    system: str,
    *,
    source: str | None = None,
    start: StartPosition = StartPosition.LATEST,
    config_path: Path = DEFAULT_CONFIG,
    environ: Mapping[str, str] = os.environ,
    transport: httpx.AsyncBaseTransport | None = None,
) -> int:
    """Runs the tentacle to the end and returns its exit code.

    `0`: every record was sent, skipped, or rejected with a counted reason. `1`: some
    observation is unsent, or a source failed. `2`: a config or startup error.
    """
    try:
        plan = prepare(
            system, source=source, start=start, config_path=config_path, environ=environ
        )
    except StartupError as error:
        logger.error("tentacle not started: %s", error)
        return EXIT_STARTUP_ERROR
    return asyncio.run(_serve(plan, transport))


async def _serve(plan: Plan, transport: httpx.AsyncBaseTransport | None) -> int:
    """`run` with SIGINT and SIGTERM setting its stop event."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGINT, signal.SIGTERM)
    for signum in signals:
        with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
            loop.add_signal_handler(signum, stop.set)
    try:
        return await run(plan, stop, transport=transport)
    finally:
        for signum in signals:
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.remove_signal_handler(signum)


async def run(
    plan: Plan,
    stop: asyncio.Event,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    counters_interval_s: float = COUNTERS_INTERVAL_S,
) -> int:
    """Reads every planned source until each ends or `stop` is set, then drains and exits.

    The drain sends what is pending within `tentacle.shutdown_s`; what is still pending then is
    counted as unsent. Counters are logged every `counters_interval_s` and at exit.
    """
    counters = {planned.id: SourceCounters(planned.id) for planned in plan.sources}
    queue: asyncio.Queue[Outgoing | None] = asyncio.Queue(maxsize=plan.tentacle.buffer_size)
    logger.info("tentacle started: system=%s sources=%s", plan.system, ",".join(counters))
    async with httpx.AsyncClient(transport=transport, timeout=REQUEST_TIMEOUT_S) as client:
        sender = Sender(
            client, url=plan.url, token=plan.token, config=plan.tentacle, counters=counters
        )
        sending = asyncio.create_task(sender.run(queue))
        reporting = asyncio.create_task(_report(counters, counters_interval_s))
        stopping = asyncio.create_task(stop.wait())
        readers = [
            asyncio.create_task(_run_source(planned, queue, counters[planned.id]))
            for planned in plan.sources
        ]

        reading = set(readers)
        while reading and not stop.is_set() and not sending.done():
            done, _ = await asyncio.wait(
                {*reading, stopping, sending}, return_when=asyncio.FIRST_COMPLETED
            )
            reading -= done
        for task in reading:
            task.cancel()
        stopping.cancel()
        await asyncio.wait({*readers, stopping})
        failed = [task for task in readers if not task.cancelled() and not task.result()]

        if not sending.done():
            try:
                async with asyncio.timeout(plan.tentacle.shutdown_s):
                    await queue.put(None)
                    await asyncio.wait({sending})
            except TimeoutError:
                logger.warning("shutdown drain timed out after %ss", plan.tentacle.shutdown_s)
                sending.cancel()
        await asyncio.wait({sending})
        sender_failed = not sending.cancelled() and sending.exception() is not None
        if sender_failed:
            logger.error("sender failed: error=%s", type(sending.exception()).__name__)
        reporting.cancel()
        await asyncio.wait({reporting})

    for source_counters in counters.values():
        if source_counters.pending:
            logger.warning(
                "unsent at shutdown: source=%s count=%d",
                source_counters.source,
                source_counters.pending,
            )
            source_counters.unsent += source_counters.pending
        logger.info("tentacle counters: %s", source_counters.line())
    unsent = any(source_counters.unsent for source_counters in counters.values())
    return EXIT_UNSENT if unsent or failed or sender_failed else EXIT_OK


def _run_source(
    planned: Planned, queue: asyncio.Queue[Outgoing | None], counters: SourceCounters
) -> Coroutine[Any, Any, bool]:
    if isinstance(planned, PlannedProxy):
        return _forward(planned, queue, counters)
    if isinstance(planned, PlannedReceiver):
        return _receive(planned, queue, counters)
    return _read(planned, queue, counters)


async def _read(
    planned: PlannedSource, queue: asyncio.Queue[Outgoing | None], counters: SourceCounters
) -> bool:
    """Reads one source to its end; False when it failed. A full queue pauses reading."""
    try:
        async for raw in planned.records.records():
            counters.read += 1
            outcome = planned.pipeline.process(raw)
            if isinstance(outcome, Rejected):
                counters.rejected[outcome.reason] += 1
                await settle(raw.ack, planned.id)
            elif isinstance(outcome, Skipped):
                counters.skipped[outcome.reason] += 1
                await settle(raw.ack, planned.id)
            else:
                counters.built += 1
                await queue.put(Outgoing(outcome, raw.ack))
            await asyncio.sleep(0)
    except Exception as error:
        logger.error("source failed: source=%s error=%s", planned.id, type(error).__name__)
        return False
    return True


async def _forward(
    planned: PlannedProxy, queue: asyncio.Queue[Outgoing | None], counters: SourceCounters
) -> bool:
    """Serves one proxy until its task is cancelled; False when it failed."""
    try:
        await planned.proxy.serve(_ExchangeCounter(planned.pipeline, queue, counters))
    except Exception as error:
        logger.error("source failed: source=%s error=%s", planned.id, type(error).__name__)
        return False
    return True


async def _receive(
    planned: PlannedReceiver, queue: asyncio.Queue[Outgoing | None], counters: SourceCounters
) -> bool:
    """Serves one receiver until its task is cancelled; False when it failed."""
    try:
        await planned.receiver.serve(_SpanCounter(planned.pipeline, queue, counters))
    except Exception as error:
        logger.error("source failed: source=%s error=%s", planned.id, type(error).__name__)
        return False
    return True


async def _nothing_to_ack() -> None:
    return None


class _ExchangeCounter:
    """The sink a proxy reports to: it shapes each exchange and queues what it built.

    A proxy cannot pause the traffic it forwards, so an observation that finds the queue full
    is skipped as `buffer_full` instead of waiting. An exchange counts as one `read`.
    """

    def __init__(
        self,
        pipeline: ExchangePipeline,
        queue: asyncio.Queue[Outgoing | None],
        counters: SourceCounters,
    ) -> None:
        self._pipeline = pipeline
        self._queue = queue
        self._counters = counters

    def exchange(self, exchange: HttpExchange) -> None:
        self._counters.read += 1
        for outcome in self._pipeline.process(exchange):
            if isinstance(outcome, PartSkipped):
                self._counters.skipped[outcome.reason] += 1
                continue
            try:
                self._queue.put_nowait(Outgoing(outcome, _nothing_to_ack))
            except asyncio.QueueFull:
                self._counters.skipped["buffer_full"] += 1
            else:
                self._counters.built += 1

    def skip(self, reason: ExchangeSkipReason) -> None:
        self._counters.read += 1
        self._counters.skipped[reason] += 1

    def note(self, reason: ExchangeNote) -> None:
        self._counters.noted[reason] += 1


class _SpanCounter:
    """The sink a receiver reports to: it maps each request's spans and queues what they built.

    A batch is taken whole or not at all: when its observations do not all fit the queue,
    nothing is counted or queued, `buffer_full` is noted, and the receiver asks the sender to
    retry, so the collector's queue holds the spans. A span counts as one `read`.
    """

    def __init__(
        self,
        pipeline: SpanPipeline,
        queue: asyncio.Queue[Outgoing | None],
        counters: SourceCounters,
    ) -> None:
        self._pipeline = pipeline
        self._queue = queue
        self._counters = counters

    def spans(self, spans: Sequence[RawSpan]) -> SpanBatchResult:
        outcomes = [self._pipeline.process(span) for span in spans]
        observations = [item for outcome in outcomes for item in outcome.observations]
        if len(observations) > self._queue.maxsize - self._queue.qsize():
            self._counters.noted["buffer_full"] += 1
            return SpanBatchResult(taken=False)
        for observation in observations:
            self._queue.put_nowait(Outgoing(observation, _nothing_to_ack))
        self._counters.read += len(spans)
        self._counters.built += len(observations)
        for outcome in outcomes:
            if outcome.rejected is not None:
                self._counters.rejected[outcome.rejected] += 1
            self._counters.skipped.update(outcome.skipped)
            self._counters.noted.update(outcome.noted)
        rejected = sum(1 for outcome in outcomes if outcome.rejected is not None)
        return SpanBatchResult(taken=True, rejected=rejected)

    def note(self, reason: SpanRequestNote) -> None:
        self._counters.noted[reason] += 1


async def _report(counters: Mapping[str, SourceCounters], interval_s: float) -> None:
    while True:
        await asyncio.sleep(interval_s)
        for source_counters in counters.values():
            logger.info("tentacle counters: %s", source_counters.line())
