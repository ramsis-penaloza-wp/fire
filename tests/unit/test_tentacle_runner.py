"""The tentacle process: startup checks, exit codes, backpressure, shutdown, and counters."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.config import TentacleConfig
from fire.apps.tentacle.__main__ import main
from fire.apps.tentacle.runner import (
    EXIT_OK,
    EXIT_STARTUP_ERROR,
    EXIT_UNSENT,
    Plan,
    PlannedProxy,
    PlannedReceiver,
    PlannedSource,
    _serve,
    prepare,
    run,
    run_tentacle,
)
from fire.core.domain.envelope import Envelope
from fire.core.domain.observation import ChannelDirection, SpanKind
from fire.core.domain.operation import OperationInputs, OperationNamer
from fire.core.ports.exchange_source import (
    ExchangeNote,
    ExchangeSink,
    ExchangeSkipReason,
    HttpExchange,
    HttpMessage,
)
from fire.core.ports.record_source import RawRecord
from fire.core.ports.span_source import (
    RawLink,
    RawSpan,
    SpanBatchResult,
    SpanRequestNote,
    SpanSink,
)
from fire.core.services.exchange_pipeline import ExchangePipeline
from fire.core.services.record_pipeline import RecordPipeline
from fire.core.services.span_pipeline import SpanPipeline

TOKEN = "runner-test-token"
ENVELOPE = {"event_id": "id", "event_type": "type", "occurred_at": "at", "payload": "data"}


def _record(observation_id: str, event_type: str = "order.created") -> dict[str, Any]:
    return {
        "id": observation_id,
        "type": event_type,
        "at": "2026-10-11T16:30:00Z",
        "data": {"order_id": f"order-{observation_id}"},
    }


def _replay_source(source_id: str = "samples", path_env: str = "REPLAY") -> dict[str, Any]:
    return {
        "id": source_id,
        "kind": "replay",
        "direction": "internal",
        "path_env": path_env,
        "envelope": ENVELOPE,
    }


KINESIS_SOURCE = {
    "id": "events",
    "kind": "kinesis",
    "direction": "internal",
    "stream_env": "STREAM",
    "regions": ["us-east-1"],
    "envelope": ENVELOPE,
}


def _configure(
    tmp_path: Path,
    sources: list[dict[str, Any]] | None = None,
    tentacle: dict[str, Any] | None = None,
) -> Path:
    config = tmp_path / "config"
    (config / "systems").mkdir(parents=True)
    fire_yml = config / "fire.yml"
    fire_yml.write_text(yaml.safe_dump({"tentacle": tentacle or {}}), encoding="utf-8")
    system = {
        "system": "shop",
        "display_name": "Shop",
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [_replay_source(), KINESIS_SOURCE] if sources is None else sources,
    }
    (config / "systems" / "shop.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    return fire_yml


def _replay_file(tmp_path: Path, records: list[Any]) -> Path:
    path = tmp_path / "records.json"
    path.write_text(json.dumps(records), encoding="utf-8")
    return path


def _environ(replay: Path | None = None, **extra: str) -> dict[str, str]:
    environ = {"FIRE_API_TOKEN": TOKEN, "FIRE_BODY_URL": "http://body"}
    if replay is not None:
        environ["REPLAY"] = str(replay)
    environ.update(extra)
    return environ


class Body:
    """Answers 202 with an all-accepted report, or a fixed status; can hold requests."""

    def __init__(self, status: int = 202) -> None:
        self.status = status
        self.shapes: list[dict[str, Any]] = []
        self.spans: list[dict[str, Any]] = []
        self.release = asyncio.Event()
        self.release.set()

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        await self.release.wait()
        posted = json.loads(request.content)
        if self.status != 202:
            return httpx.Response(self.status, json={"detail": "x"})
        self.shapes.extend(posted["shapes"])
        self.spans.extend(posted["spans"])
        report = {
            kind: {"accepted": len(posted[kind]), "duplicate": 0, "rejected": []}
            for kind in ("shapes", "spans")
        }
        return httpx.Response(202, json=report)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self)


def _run(fire_yml: Path, environ: Mapping[str, str], body: Body | None = None, **kw: Any) -> int:
    transport = (body or Body()).transport()
    return run_tentacle("shop", config_path=fire_yml, environ=environ, transport=transport, **kw)


STARTUP_ERRORS = {
    "unknown-system": ({"system": "billing"}, None, "no config for system 'billing'"),
    "not-a-system-id": ({"system": "../shop"}, None, "is not a system id"),
    "unknown-source": ({"source": "orders"}, None, "declares no source 'orders'"),
    "token-unset": ({}, {"FIRE_API_TOKEN": ""}, "FIRE_API_TOKEN is not set"),
    "body-url-unset": ({}, {"FIRE_BODY_URL": ""}, "FIRE_BODY_URL is not set"),
    "body-url-ftp": ({}, {"FIRE_BODY_URL": "ftp://body"}, "not an http or https URL"),
    "body-url-no-scheme": ({}, {"FIRE_BODY_URL": "body:8000"}, "not an http or https URL"),
    "named-source-env-unset": (
        {"source": "samples"},
        {"REPLAY": ""},
        "source samples: REPLAY is not set",
    ),
    "named-source-without-reader": (
        {"source": "events"},
        {"STREAM": "orders-stream"},
        "source events: kind kinesis has no reader yet",
    ),
    "no-source-to-run": ({}, {"REPLAY": ""}, "system 'shop' has no source to run"),
    "replay-path-missing": ({}, {"REPLAY": "/nonexistent/records.json"}, "REPLAY names no"),
}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("kwargs", "environ", "message"), list(STARTUP_ERRORS.values()), ids=list(STARTUP_ERRORS)
)
def test_startup_errors_exit_2(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    kwargs: dict[str, Any],
    environ: dict[str, str] | None,
    message: str,
) -> None:
    fire_yml = _configure(tmp_path)
    system = kwargs.pop("system", "shop")
    env = _environ(_replay_file(tmp_path, [_record("1")]), **(environ or {}))
    body = Body()

    code = run_tentacle(
        system, config_path=fire_yml, environ=env, transport=body.transport(), **kwargs
    )

    assert code == EXIT_STARTUP_ERROR
    [error] = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("tentacle not started: ")
    ]
    assert message in error
    assert body.shapes == []
    assert TOKEN not in caplog.text


@pytest.mark.unit
def test_replay_path_with_another_suffix_is_a_startup_error(tmp_path: Path) -> None:
    other = tmp_path / "records.txt"
    other.write_text("{}", encoding="utf-8")
    assert _run(_configure(tmp_path), _environ(other)) == EXIT_STARTUP_ERROR


@pytest.mark.unit
@pytest.mark.parametrize(
    ("fire_yml", "system_yml"),
    [
        ({"tentacle": {"batch_size": 0}}, None),
        (None, "sources: [{id: x, kind: kinesis, stream_env: S, regions: [r], dedupe_key: id}]"),
        ("tentacle: [unclosed", None),
    ],
    ids=["bad-fire-yml", "dedupe-key", "bad-yaml"],
)
def test_config_errors_exit_2(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    fire_yml: dict[str, Any] | str | None,
    system_yml: str | None,
) -> None:
    path = _configure(tmp_path)
    if fire_yml is not None:
        text = fire_yml if isinstance(fire_yml, str) else yaml.safe_dump(fire_yml)
        path.write_text(text, encoding="utf-8")
    if system_yml is not None:
        base = "system: shop\ndisplay_name: Shop\nnotify: {slack: {channel_id: C0123456789}}\n"
        (path.parent / "systems" / "shop.yml").write_text(base + system_yml, encoding="utf-8")

    assert _run(path, _environ(_replay_file(tmp_path, []))) == EXIT_STARTUP_ERROR
    assert "tentacle not started:" in caplog.text


@pytest.mark.unit
def test_sources_that_cannot_run_are_skipped_and_said(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    sources = [_replay_source(), _replay_source("unset", "UNSET"), KINESIS_SOURCE]
    fire_yml = _configure(tmp_path, sources)
    environ = _environ(_replay_file(tmp_path, [_record("1")]), STREAM="orders-stream")

    assert _run(fire_yml, environ) == EXIT_OK
    assert "source skipped: source=unset reason=UNSET is not set" in caplog.text
    assert "source skipped: source=events reason=kind kinesis has no reader yet" in caplog.text
    assert "orders-stream" not in caplog.text
    assert "tentacle started: system=shop sources=samples" in caplog.text


@pytest.mark.unit
def test_named_source_runs_alone(tmp_path: Path) -> None:
    sources = [_replay_source(), _replay_source("other", "OTHER")]
    fire_yml = _configure(tmp_path, sources)
    other = tmp_path / "other.json"
    other.write_text(json.dumps([_record("9")]), encoding="utf-8")
    body = Body()
    environ = _environ(_replay_file(tmp_path, [_record("1")]), OTHER=str(other))

    assert _run(fire_yml, environ, body, source="other") == EXIT_OK
    assert [(shape["source"], shape["observation_id"]) for shape in body.shapes] == [
        ("other", "9")
    ]


@pytest.mark.unit
def test_every_record_settled_exits_0_and_logs_its_counters(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    records = [
        _record("1"),
        _record("2"),
        {"type": "x"},
        "text",
        _record("1"),
        _record("3", "a b"),
    ]
    body = Body()

    assert _run(_configure(tmp_path), _environ(_replay_file(tmp_path, records)), body) == EXIT_OK

    assert sorted(shape["observation_id"] for shape in body.shapes) == ["1", "2"]
    assert (
        "tentacle counters: source=samples read=6 built=2 sent=2 unsent=0 pending=0 "
        "rejected=bad_name:1,missing_event_id:1,undecodable:1 skipped=local_duplicate:1 "
        "noted=- body_duplicate=0 body_rejected=-"
    ) in caplog.text


@pytest.mark.unit
def test_refused_batch_exits_1(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    environ = _environ(_replay_file(tmp_path, [_record("1"), _record("2")]))

    assert _run(_configure(tmp_path), environ, Body(status=401)) == EXIT_UNSENT
    assert "batch unsent: source=samples size=2 status=401 attempts=1" in caplog.text
    assert "source=samples read=2 built=2 sent=0 unsent=2 pending=0" in caplog.text


@pytest.mark.unit
def test_drain_past_shutdown_s_counts_pending_as_unsent(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    body = Body()
    body.release.clear()
    fire_yml = _configure(tmp_path, tentacle={"shutdown_s": 0.2})
    environ = _environ(_replay_file(tmp_path, [_record("1"), _record("2")]))

    assert _run(fire_yml, environ, body) == EXIT_UNSENT
    assert "shutdown drain timed out after 0.2s" in caplog.text
    assert "unsent at shutdown: source=samples count=2" in caplog.text
    assert "source=samples read=2 built=2 sent=0 unsent=2 pending=0" in caplog.text


class ScriptedSource:
    """Yields records, counting what it handed out; optionally fails or waits forever."""

    def __init__(
        self, records: list[Any], *, fail: bool = False, live: bool = False, acks: list[str]
    ) -> None:
        self.records_to_yield = records
        self.fail = fail
        self.live = live
        self.acks = acks
        self.yielded = 0

    async def records(self) -> AsyncIterator[RawRecord]:
        for record in self.records_to_yield:
            self.yielded += 1
            yield RawRecord(source="scripted", region=None, record=record, ack=self._ack(record))
        if self.fail:
            raise RuntimeError("planted-failure-detail")
        if self.live:
            await asyncio.Event().wait()

    def _ack(self, record: Any) -> Any:
        async def ack() -> None:
            self.acks.append(record["id"] if isinstance(record, dict) and "id" in record else "-")

        return ack


def _plan(source: ScriptedSource, **tentacle: Any) -> Plan:
    pipeline = RecordPipeline(
        system="shop",
        source="scripted",
        direction=ChannelDirection.INTERNAL,
        envelope=Envelope.model_validate(ENVELOPE),
        sample_rate=1.0,
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
    )
    return Plan(
        system="shop",
        sources=[PlannedSource(id="scripted", records=source, pipeline=pipeline)],
        tentacle=TentacleConfig.model_validate(tentacle),
        url="http://body/v0/observations",
        token=TOKEN,
    )


@pytest.mark.unit
async def test_full_buffer_pauses_reading_instead_of_dropping() -> None:
    source = ScriptedSource([_record(str(index)) for index in range(20)], acks=[])
    body = Body()
    body.release.clear()
    running = asyncio.create_task(
        run(
            _plan(source, buffer_size=2, batch_size=1, flush_ms=1),
            asyncio.Event(),
            transport=body.transport(),
        )
    )

    await asyncio.sleep(0.2)
    # One observation in flight, two in the buffer, and one waiting to enter it.
    assert source.yielded == 4
    body.release.set()

    assert await asyncio.wait_for(running, 5) == EXIT_OK
    assert source.yielded == 20
    assert len(body.shapes) == 20


@pytest.mark.unit
async def test_records_are_acked_after_202_and_settled_ones_at_once() -> None:
    acks: list[str] = []
    source = ScriptedSource([_record("1"), {"id": "2"}, _record("3")], acks=acks)
    body = Body()
    body.release.clear()
    running = asyncio.create_task(
        run(_plan(source, flush_ms=1), asyncio.Event(), transport=body.transport())
    )

    await asyncio.sleep(0.1)
    assert acks == ["2"]
    body.release.set()

    assert await asyncio.wait_for(running, 5) == EXIT_OK
    assert sorted(acks) == ["1", "2", "3"]


@pytest.mark.unit
async def test_refused_records_are_not_acked() -> None:
    acks: list[str] = []
    source = ScriptedSource([_record("1"), {"id": "2"}], acks=acks)
    plan = _plan(source)
    assert await run(plan, asyncio.Event(), transport=Body(401).transport()) == EXIT_UNSENT
    assert acks == ["2"]


@pytest.mark.unit
async def test_failing_source_exits_1_by_exception_type_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    body = Body()
    source = ScriptedSource([_record("1")], fail=True, acks=[])

    assert await run(_plan(source), asyncio.Event(), transport=body.transport()) == EXIT_UNSENT
    assert "source failed: source=scripted error=RuntimeError" in caplog.text
    assert "planted-failure-detail" not in caplog.text
    assert [shape["observation_id"] for shape in body.shapes] == ["1"]


@pytest.mark.unit
async def test_stop_ends_a_live_source_and_drains() -> None:
    body = Body()
    stop = asyncio.Event()
    source = ScriptedSource([_record("1"), _record("2")], live=True, acks=[])
    running = asyncio.create_task(run(_plan(source, flush_ms=1), stop, transport=body.transport()))

    await asyncio.sleep(0.1)
    assert not running.done()
    stop.set()

    assert await asyncio.wait_for(running, 5) == EXIT_OK
    assert len(body.shapes) == 2


@pytest.mark.unit
async def test_sigterm_stops_the_tentacle() -> None:
    body = Body()
    source = ScriptedSource([_record("1")], live=True, acks=[])
    loop = asyncio.get_running_loop()
    loop.call_later(0.2, os.kill, os.getpid(), signal.SIGTERM)

    assert await asyncio.wait_for(_serve(_plan(source), body.transport()), 5) == EXIT_OK
    assert len(body.shapes) == 1


@pytest.mark.unit
async def test_counters_are_logged_periodically(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    stop = asyncio.Event()
    source = ScriptedSource([_record("1")], live=True, acks=[])
    running = asyncio.create_task(
        run(_plan(source), stop, transport=Body().transport(), counters_interval_s=0.05)
    )
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(running, 5)

    assert caplog.text.count("tentacle counters: source=scripted") >= 3


class ScriptedProxy:
    """Reports scripted calls to its sink, then serves until cancelled or fails."""

    def __init__(self, calls: list[Callable[[ExchangeSink], None]], *, fail: bool = False):
        self.calls = calls
        self.fail = fail
        self.cancelled = False

    async def serve(self, sink: ExchangeSink) -> None:
        for call in self.calls:
            call(sink)
        if self.fail:
            raise RuntimeError("planted-proxy-detail")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


def _message(body: bytes, content_type: str | None = "application/json") -> HttpMessage:
    return HttpMessage(
        received_at=datetime(2026, 10, 11, 16, 30, tzinfo=UTC),
        content_type=content_type,
        content_encoding=None,
        body=body,
    )


def _exchange(
    method: str, request: bytes, status: int, response: bytes, exchange_id: str
) -> HttpExchange:
    return HttpExchange(
        exchange_id=exchange_id,
        method=method,
        template="/v1/orders/{order_id}",
        status_code=status,
        request=_message(request),
        response=_message(response),
    )


def _proxy_plan(proxy: ScriptedProxy, **tentacle: Any) -> Plan:
    pipeline = ExchangePipeline(
        system="shop",
        source="api",
        sample_rate=1.0,
        max_body_bytes=1024,
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
    )
    return Plan(
        system="shop",
        sources=[PlannedProxy(id="api", proxy=proxy, pipeline=pipeline)],
        tentacle=TentacleConfig.model_validate(tentacle),
        url="http://body/v0/observations",
        token=TOKEN,
    )


async def _serve_briefly(plan: Plan, body: Body) -> int:
    stop = asyncio.Event()
    running = asyncio.create_task(run(plan, stop, transport=body.transport()))
    await asyncio.sleep(0.1)
    stop.set()
    return await asyncio.wait_for(running, 5)


@pytest.mark.unit
async def test_proxy_exchanges_are_shaped_sent_and_counted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    shaped = _exchange("PUT", b'{"qty": 1}', 201, b'{"id": "x"}', "a" * 32)
    empty = _exchange("GET", b"", 204, b"", "b" * 32)
    proxy = ScriptedProxy(
        [
            lambda sink: sink.exchange(shaped),
            lambda sink: sink.skip(ExchangeSkipReason.UNMATCHED_ROUTE),
            lambda sink: sink.note(ExchangeNote.INVALID_TRACEPARENT),
            lambda sink: sink.exchange(empty),
        ]
    )
    body = Body()

    assert await _serve_briefly(_proxy_plan(proxy, flush_ms=1), body) == EXIT_OK

    assert proxy.cancelled
    assert sorted((shape["observation_id"], shape["source"]) for shape in body.shapes) == [
        ("a" * 32 + ":request", "api"),
        ("a" * 32 + ":response", "api"),
    ]
    assert (
        "source=api read=3 built=2 sent=2 unsent=0 pending=0 rejected=- "
        "skipped=no_body:2,unmatched_route:1 noted=invalid_traceparent:1 "
        "body_duplicate=0 body_rejected=-"
    ) in caplog.text


@pytest.mark.unit
async def test_proxy_observations_that_find_the_buffer_full_are_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    shaped = _exchange("PUT", b'{"qty": 1}', 201, b'{"id": "x"}', "c" * 32)
    proxy = ScriptedProxy([lambda sink: sink.exchange(shaped)])

    assert await _serve_briefly(_proxy_plan(proxy, buffer_size=1, flush_ms=1), Body()) == EXIT_OK
    assert "read=1 built=1 sent=1 unsent=0 pending=0 rejected=- skipped=buffer_full:1" in (
        caplog.text
    )


@pytest.mark.unit
async def test_failing_proxy_exits_1_by_exception_type_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    proxy = ScriptedProxy([], fail=True)

    assert await run(_proxy_plan(proxy), asyncio.Event(), transport=Body().transport()) == (
        EXIT_UNSENT
    )
    assert "source failed: source=api error=RuntimeError" in caplog.text
    assert "planted-proxy-detail" not in caplog.text


def _proxy_source(port: int, upstream_env: str = "UPSTREAM") -> dict[str, Any]:
    return {"id": "api", "kind": "http_proxy", "listen_port": port, "upstream_env": upstream_env}


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.mark.unit
@pytest.mark.parametrize("upstream", ["localhost:8020", "ftp://upstream", "http://"])
def test_proxy_upstream_must_be_an_http_url(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, upstream: str
) -> None:
    fire_yml = _configure(tmp_path, [_proxy_source(port=8021)])

    assert _run(fire_yml, _environ(UPSTREAM=upstream)) == EXIT_STARTUP_ERROR
    assert "source api: UPSTREAM is not an http or https URL" in caplog.text
    assert upstream not in caplog.text


@pytest.mark.unit
def test_proxy_on_a_busy_port_is_a_startup_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with socket.socket() as busy:
        busy.bind(("127.0.0.1", 0))
        busy.listen()
        port = busy.getsockname()[1]
        fire_yml = _configure(tmp_path, [_proxy_source(port=port)])

        assert _run(fire_yml, _environ(UPSTREAM="http://localhost:1")) == EXIT_STARTUP_ERROR
    assert f"source api: cannot listen on 127.0.0.1:{port}" in caplog.text


@pytest.mark.unit
def test_startup_error_after_a_proxy_releases_its_port(tmp_path: Path) -> None:
    port = _free_port()
    fire_yml = _configure(tmp_path, [_proxy_source(port), _replay_source()])
    environ = _environ(UPSTREAM="http://localhost:8020", REPLAY="/nonexistent/records.json")

    assert _run(fire_yml, environ) == EXIT_STARTUP_ERROR
    with socket.socket() as again:
        again.bind(("127.0.0.1", port))


@pytest.mark.unit
def test_prepare_opens_a_proxy_with_its_pipeline(tmp_path: Path) -> None:
    fire_yml = _configure(tmp_path, [_replay_source(), _proxy_source(_free_port())])
    environ = _environ(_replay_file(tmp_path, []), UPSTREAM="http://localhost:8020")

    plan = prepare("shop", config_path=fire_yml, environ=environ)
    try:
        assert [type(planned).__name__ for planned in plan.sources] == [
            "PlannedSource",
            "PlannedProxy",
        ]
    finally:
        for planned in plan.sources:
            if isinstance(planned, PlannedProxy):
                planned.proxy.close()


TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
START_NS = 1_791_768_368_257_502_981


def _raw_span(span_id: str, **overrides: Any) -> RawSpan:
    span = RawSpan(
        trace_id=TRACE,
        span_id=span_id,
        parent_span_id=None,
        service="orders-api",
        kind=SpanKind.CLIENT,
        start_ns=START_NS,
        end_ns=START_NS + 1_000_000,
        error=False,
        naming=OperationInputs(SpanKind.CLIENT, "POST", method="POST", url="https://x.net/v1"),
        status_code=200,
    )
    return replace(span, **overrides)


class ScriptedReceiver:
    """Reports scripted batches and notes to its sink, then serves until cancelled or fails."""

    def __init__(
        self,
        batches: list[list[RawSpan]],
        notes: tuple[SpanRequestNote, ...] = (),
        *,
        fail: bool = False,
    ) -> None:
        self.batches = batches
        self.notes = notes
        self.fail = fail
        self.results: list[SpanBatchResult] = []
        self.closed = False

    async def serve(self, sink: SpanSink) -> None:
        self.results = [sink.spans(batch) for batch in self.batches]
        for note in self.notes:
            sink.note(note)
        if self.fail:
            raise RuntimeError("planted-receiver-detail")
        await asyncio.Event().wait()

    def close(self) -> None:
        self.closed = True


def _receiver_plan(receiver: ScriptedReceiver, **tentacle: Any) -> Plan:
    pipeline = SpanPipeline(
        system="shop",
        source="traces",
        sample_rate=1.0,
        namer=OperationNamer(),
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
    )
    return Plan(
        system="shop",
        sources=[PlannedReceiver(id="traces", receiver=receiver, pipeline=pipeline)],
        tentacle=TentacleConfig.model_validate(tentacle),
        url="http://body/v0/observations",
        token=TOKEN,
    )


@pytest.mark.unit
async def test_received_spans_are_mapped_sent_and_counted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    batches = [
        [_raw_span("a" * 16, request_body='{"qty": 1}'), _raw_span("b" * 16, trace_id=None)],
        [_raw_span("c" * 16, request_body="", links=(RawLink(TRACE, "x"),))],
    ]
    receiver = ScriptedReceiver(batches, (SpanRequestNote.MALFORMED_REQUEST,))
    body = Body()

    assert await _serve_briefly(_receiver_plan(receiver, flush_ms=1), body) == EXIT_OK

    assert receiver.results == [SpanBatchResult(True, 1), SpanBatchResult(True, 0)]
    assert sorted(span["span_id"] for span in body.spans) == ["a" * 16, "c" * 16]
    assert [span["operation"] for span in body.spans] == ["POST x.net/v1"] * 2
    assert [shape["observation_id"] for shape in body.shapes] == [f"{TRACE}:{'a' * 16}:request"]
    assert (
        "source=traces read=3 built=3 sent=3 unsent=0 pending=0 rejected=invalid_ids:1 "
        "skipped=body_empty:1 noted=invalid_link:1,malformed_request:1 "
        "body_duplicate=0 body_rejected=-"
    ) in caplog.text


@pytest.mark.unit
async def test_a_batch_that_does_not_fit_the_buffer_is_not_taken(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    too_many = [_raw_span(f"{index + 1:016x}") for index in range(3)]
    receiver = ScriptedReceiver([too_many, too_many[:2]])
    body = Body()

    assert await _serve_briefly(_receiver_plan(receiver, buffer_size=2, flush_ms=1), body) == (
        EXIT_OK
    )
    assert receiver.results == [SpanBatchResult(False), SpanBatchResult(True)]
    assert len(body.spans) == 2
    assert "source=traces read=2 built=2 sent=2 unsent=0 pending=0 rejected=- skipped=- " in (
        caplog.text
    )
    assert "noted=buffer_full:1" in caplog.text


@pytest.mark.unit
async def test_failing_receiver_exits_1_by_exception_type_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    receiver = ScriptedReceiver([], fail=True)
    plan = _receiver_plan(receiver)

    assert await run(plan, asyncio.Event(), transport=Body().transport()) == EXIT_UNSENT
    assert "source failed: source=traces error=RuntimeError" in caplog.text
    assert "planted-receiver-detail" not in caplog.text


def _otlp_source(listen_env: str = "LISTEN") -> dict[str, Any]:
    return {"id": "traces", "kind": "otlp", "listen_env": listen_env}


@pytest.mark.unit
def test_an_otlp_source_without_its_address_is_skipped(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    fire_yml = _configure(tmp_path, [_replay_source(), _otlp_source()])
    environ = _environ(_replay_file(tmp_path, [_record("1")]))

    assert _run(fire_yml, environ) == EXIT_OK
    assert "source skipped: source=traces reason=LISTEN is not set" in caplog.text


@pytest.mark.unit
@pytest.mark.parametrize(
    "address",
    ["4318", "127.0.0.1", "127.0.0.1:", ":4318", "127.0.0.1:0", "127.0.0.1:70000", "h:43a"],
)
def test_an_otlp_listen_address_must_be_host_and_port(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, address: str
) -> None:
    fire_yml = _configure(tmp_path, [_otlp_source()])

    assert _run(fire_yml, _environ(LISTEN=address)) == EXIT_STARTUP_ERROR
    assert "source traces: LISTEN is not a host:port address" in caplog.text


@pytest.mark.unit
def test_an_otlp_receiver_on_a_busy_port_is_a_startup_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with socket.socket() as busy:
        busy.bind(("127.0.0.1", 0))
        busy.listen()
        port = busy.getsockname()[1]
        fire_yml = _configure(tmp_path, [_otlp_source()])

        assert _run(fire_yml, _environ(LISTEN=f"127.0.0.1:{port}")) == EXIT_STARTUP_ERROR
    assert f"source traces: cannot listen on 127.0.0.1:{port}" in caplog.text


@pytest.mark.unit
def test_prepare_opens_a_receiver_and_releases_it_on_a_later_startup_error(
    tmp_path: Path,
) -> None:
    port = _free_port()
    fire_yml = _configure(tmp_path, [_otlp_source(), _replay_source()])
    environ = _environ(_replay_file(tmp_path, []), LISTEN=f"127.0.0.1:{port}")
    plan = prepare("shop", config_path=fire_yml, environ=environ)
    try:
        assert [type(planned).__name__ for planned in plan.sources] == [
            "PlannedReceiver",
            "PlannedSource",
        ]
    finally:
        for planned in plan.sources:
            if isinstance(planned, PlannedReceiver):
                planned.receiver.close()

    environ["REPLAY"] = "/nonexistent/records.json"
    assert _run(fire_yml, environ) == EXIT_STARTUP_ERROR
    with socket.socket() as again:
        again.bind(("127.0.0.1", port))


@pytest.mark.unit
@pytest.mark.parametrize(
    "argv",
    [[], ["--source", "x"], ["--system", "shop", "--start", "middle"]],
    ids=["no-system", "source-only", "bad-start"],
)
def test_bad_arguments_exit_2(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        main(argv)
    assert caught.value.code == EXIT_STARTUP_ERROR
