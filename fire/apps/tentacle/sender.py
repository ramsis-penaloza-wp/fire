"""Sends observations to the body in batches, retrying with backoff; nothing is dropped silently.

Contract: docs/specs/tentacle.md#sending-to-the-body.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass

import httpx

from fire.apps.config import TentacleConfig
from fire.apps.tentacle.counters import SourceCounters
from fire.core.domain.ingest import IngestReport
from fire.core.domain.observation import ShapeObservation

logger = logging.getLogger(__name__)

MAX_BACKOFF_MS = 30_000
_RETRIED_STATUSES = frozenset({408, 429})

type Ack = Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class Outgoing:
    """A built observation and the ack of the record it came from."""

    observation: ShapeObservation
    ack: Ack


async def settle(ack: Ack, source: str) -> None:
    """Acks a record; a failing ack is logged by exception type and does not stop the source."""
    try:
        await ack()
    except Exception as error:
        logger.warning("ack failed: source=%s error=%s", source, type(error).__name__)


class Sender:
    """Posts batches to `POST /v0/observations` and counts every observation sent or unsent.

    A batch holds one source's observations, so the body's counts are that source's. It is
    posted when it reaches `batch_size` or its first observation has waited `flush_ms`. A
    connection error, a timeout, `408`, `429`, or a `5xx` is retried up to
    `retry.max_attempts`; any other status is not. A batch that runs out of attempts or is
    refused is counted as unsent and logged by size, source, and status. Records are acked only
    after the body answers `202` for their batch.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        url: str,
        token: str,
        config: TentacleConfig,
        counters: Mapping[str, SourceCounters],
        sleep: Callable[[float], Awaitable[object]] = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        self._client = client
        self._url = url
        self._headers = {"X-FIRE-Token": token, "Content-Type": "application/json"}
        self._config = config
        self._counters = counters
        self._sleep = sleep
        self._jitter = jitter

    async def run(self, queue: asyncio.Queue[Outgoing | None]) -> None:
        """Batches what the queue holds until it yields None, then sends every partial batch."""
        loop = asyncio.get_running_loop()
        batches: dict[str, list[Outgoing]] = {}
        deadlines: dict[str, float] = {}
        while True:
            for source in [source for source, due in deadlines.items() if due <= loop.time()]:
                await self._flush(source, batches, deadlines)
            timeout = max(0.0, min(deadlines.values()) - loop.time()) if deadlines else None
            try:
                item = await asyncio.wait_for(queue.get(), timeout)
            except TimeoutError:
                continue
            if item is None:
                for source in list(batches):
                    await self._flush(source, batches, deadlines)
                return
            source = item.observation.source
            batch = batches.setdefault(source, [])
            if not batch:
                deadlines[source] = loop.time() + self._config.flush_ms / 1000
            batch.append(item)
            if len(batch) >= self._config.batch_size:
                await self._flush(source, batches, deadlines)

    async def _flush(
        self, source: str, batches: dict[str, list[Outgoing]], deadlines: dict[str, float]
    ) -> None:
        del deadlines[source]
        await self.send(batches.pop(source))

    async def send(self, batch: list[Outgoing]) -> None:
        """Posts one source's batch, retrying as configured; counts it sent or unsent."""
        source = batch[0].observation.source
        content = json.dumps(
            {
                "shapes": [
                    item.observation.model_dump(mode="json", exclude_none=True) for item in batch
                ]
            }
        ).encode()
        status = "none"
        attempt = 0
        while attempt < self._config.retry.max_attempts:
            if attempt:
                await self._sleep(self._backoff_s(attempt))
            attempt += 1
            try:
                response = await self._client.post(
                    self._url, content=content, headers=self._headers
                )
            except httpx.HTTPError as error:
                status = type(error).__name__
                continue
            if response.status_code == 202:
                await self._accepted(source, batch, response)
                return
            status = str(response.status_code)
            if response.status_code not in _RETRIED_STATUSES and response.status_code < 500:
                break
        self._counters[source].unsent += len(batch)
        logger.warning(
            "batch unsent: source=%s size=%d status=%s attempts=%d",
            source,
            len(batch),
            status,
            attempt,
        )

    def _backoff_s(self, retry: int) -> float:
        """Full jitter over the backoff of the `retry`-th retry, doubled each time, capped."""
        ceiling_ms = min(self._config.retry.backoff_ms * 2 ** (retry - 1), MAX_BACKOFF_MS)
        return ceiling_ms / 1000 * self._jitter()

    async def _accepted(
        self, source: str, batch: list[Outgoing], response: httpx.Response
    ) -> None:
        counters = self._counters[source]
        counters.sent += len(batch)
        try:
            report = IngestReport.model_validate(response.json())
        except ValueError:
            logger.warning("body report unreadable: source=%s size=%d", source, len(batch))
        else:
            counters.body_duplicate += report.shapes.duplicate
            counters.body_rejected.update(rejection.reason for rejection in report.shapes.rejected)
        for item in batch:
            await settle(item.ack, source)
