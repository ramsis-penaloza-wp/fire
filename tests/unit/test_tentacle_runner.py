"""The tentacle process: startup checks, exit codes, backpressure, shutdown, and counters."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from collections.abc import AsyncIterator, Mapping
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
    PlannedSource,
    _serve,
    run,
    run_tentacle,
)
from fire.core.domain.envelope import Envelope
from fire.core.domain.observation import ChannelDirection
from fire.core.ports.record_source import RawRecord
from fire.core.services.record_pipeline import RecordPipeline

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
        self.release = asyncio.Event()
        self.release.set()

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        await self.release.wait()
        shapes = json.loads(request.content)["shapes"]
        if self.status != 202:
            return httpx.Response(self.status, json={"detail": "x"})
        self.shapes.extend(shapes)
        report = {"accepted": len(shapes), "duplicate": 0, "rejected": []}
        empty = {"accepted": 0, "duplicate": 0, "rejected": []}
        return httpx.Response(202, json={"shapes": report, "spans": empty})

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
        "body_duplicate=0 body_rejected=-"
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
