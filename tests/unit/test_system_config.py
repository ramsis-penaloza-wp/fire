"""SystemConfig: per-system YAML holds decisions, validated strictly."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from fire.apps.config import (
    EventSourceConfig,
    HttpProxySourceConfig,
    OtlpSourceConfig,
    ReplaySourceConfig,
    S3CaptureSourceConfig,
    SystemConfig,
    load_system_config,
    load_system_configs,
)
from fire.core.domain.observation import ChannelDirection

SYSTEMS = Path(__file__).resolve().parents[2] / "config" / "systems"
TEMPLATE = SYSTEMS / "_template.yml"
ENVELOPE = {"event_id": "id", "event_type": "type", "occurred_at": "at", "payload": "data"}


def _minimal(**overrides: Any) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "system": "example",
        "display_name": "Example",
        "notify": {"slack": {"channel_id": "C0123456789"}},
    }
    raw.update(overrides)
    return raw


def _write(directory: Path, name: str, raw: dict[str, Any]) -> Path:
    path = directory / name
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    return path


@pytest.mark.unit
def test_template_is_a_valid_system_config() -> None:
    config = SystemConfig.model_validate(yaml.safe_load(TEMPLATE.read_text()))
    assert config.notify.slack.channel_id == "C0123456789"
    assert config.discovery.manifest is not None
    traces, source = config.sources
    assert isinstance(traces, OtlpSourceConfig)
    assert traces.location_env == "EXAMPLE_OTLP_LISTEN"
    assert source.id == "domain-events"
    assert source.kind == "kinesis"
    assert isinstance(source, EventSourceConfig)
    assert source.envelope.event_id.path == "work_unit_id"
    [rule] = config.operations.http
    assert (rule.hosts, rule.alias) == (["*.partner.example.com"], "partner")


@pytest.mark.unit
def test_repo_system_configs_are_valid() -> None:
    configs = load_system_configs(SYSTEMS)
    assert "omg" in configs
    assert configs["omg"].source_ids() == {
        "omg-traces",
        "omg-api",
        "omg-validation",
        "domain-events",
        "order-domain-events",
        "rx-updates",
        "carrier-events",
        "financials-events",
        "transmissions",
        "work-unit-samples",
    }


@pytest.mark.unit
def test_omg_replays_work_units_through_their_envelope() -> None:
    sources = {source.id: source for source in load_system_configs(SYSTEMS)["omg"].sources}
    replay = sources["work-unit-samples"]
    assert isinstance(replay, ReplaySourceConfig)
    assert replay.direction is ChannelDirection.INTERNAL
    assert replay.location_env == "OMG_REPLAY_PATH"
    envelope = replay.envelope
    assert envelope.work_id is not None
    assert envelope.event_id.path == envelope.work_id.path == "work_unit_id"
    assert envelope.occurred_at.path == "metadata.timestamp"
    assert envelope.correlation_id is None


@pytest.mark.unit
def test_minimal_config_defaults() -> None:
    config = SystemConfig.model_validate(_minimal())
    assert config.notify.slack.channel_name is None
    assert config.discovery.manifest is None
    assert config.sources == []
    assert config.operations.http == []
    assert config.policy.critical == []
    assert config.fingerprinting.sample_rate == 1.0


@pytest.mark.unit
def test_slack_channel_id_is_required() -> None:
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(notify={"slack": {"channel_name": "#x"}}))


@pytest.mark.unit
@pytest.mark.parametrize("channel_id", ["C0123456789", "G0ABCDEFGH", "C12345678"])
def test_slack_channel_id_accepts_channel_ids(channel_id: str) -> None:
    config = SystemConfig.model_validate(_minimal(notify={"slack": {"channel_id": channel_id}}))
    assert config.notify.slack.channel_id == channel_id


@pytest.mark.unit
@pytest.mark.parametrize(
    "channel_id",
    ["#example-incidents", "c0123456789", "D0123456789", "U0123456789", "C1234", ""],
    ids=["name", "lowercase", "dm", "user", "short", "empty"],
)
def test_slack_channel_id_rejects_non_channel_ids(channel_id: str) -> None:
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(notify={"slack": {"channel_id": channel_id}}))


@pytest.mark.unit
@pytest.mark.parametrize("channel_name", ["example-incidents", "#Upper", "#has space"])
def test_slack_channel_name_must_look_like_a_channel(channel_name: str) -> None:
    slack = {"channel_id": "C0123456789", "channel_name": channel_name}
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(notify={"slack": slack}))


@pytest.mark.unit
def test_unknown_keys_are_rejected() -> None:
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(team_channel="#old"))


@pytest.mark.unit
def test_critical_and_ignore_must_be_disjoint() -> None:
    with pytest.raises(ValidationError, match="order.created"):
        SystemConfig.model_validate(
            _minimal(policy={"critical": ["order.created"], "ignore": ["order.created"]})
        )


@pytest.mark.unit
@pytest.mark.parametrize("sample_rate", [0, -0.1, 1.5])
def test_sample_rate_is_in_zero_one(sample_rate: float) -> None:
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(fingerprinting={"sample_rate": sample_rate}))


DRIFT = {"min_samples": 20, "required_ratio": 1.0}


@pytest.mark.unit
def test_drift_defaults_when_absent() -> None:
    drift = SystemConfig.model_validate(_minimal()).drift
    assert (drift.min_samples, drift.required_ratio) == (20, 1.0)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("drift", "expected"),
    [
        ({"min_samples": 5, "required_ratio": 0.9}, (5, 0.9)),
        ({"min_samples": 5}, (5, 1.0)),
        ({"required_ratio": 0.5}, (20, 0.5)),
        ({}, (20, 1.0)),
    ],
    ids=["both", "min-samples-only", "ratio-only", "empty"],
)
def test_drift_loads_from_yaml(
    tmp_path: Path, drift: dict[str, Any], expected: tuple[int, float]
) -> None:
    config = load_system_config(_write(tmp_path, "example.yml", _minimal(drift=drift)))
    assert (config.drift.min_samples, config.drift.required_ratio) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "value"),
    [("min_samples", 0), ("required_ratio", 0), ("required_ratio", 1.5)],
    ids=["min-samples-zero", "ratio-zero", "ratio-above-one"],
)
def test_drift_rejects_out_of_range_values(key: str, value: float) -> None:
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(drift={key: value}))
    assert [error["loc"] for error in caught.value.errors()] == [("drift", key)]


@pytest.mark.unit
def test_drift_rejects_unknown_keys() -> None:
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(drift={**DRIFT, "window": 5}))
    assert [error["loc"] for error in caught.value.errors()] == [("drift", "window")]


@pytest.mark.unit
def test_omg_declares_its_drift_policy() -> None:
    assert yaml.safe_load((SYSTEMS / "omg.yml").read_text())["drift"] == DRIFT
    drift = load_system_configs(SYSTEMS)["omg"].drift
    assert (drift.min_samples, drift.required_ratio) == (20, 1.0)


@pytest.mark.unit
def test_template_declares_a_drift_policy() -> None:
    raw = yaml.safe_load(TEMPLATE.read_text())
    assert raw["drift"] == DRIFT
    drift = SystemConfig.model_validate(raw).drift
    assert (drift.min_samples, drift.required_ratio) == (20, 1.0)


CAPTURE = {"prefix": "partner-payloads/", "label": "partner", "layout": "work_unit_action"}

# One valid source per kind, with the env var that names what it reads.
SOURCES: dict[str, tuple[dict[str, Any], str]] = {
    "replay": (
        {"kind": "replay", "direction": "internal", "path_env": "P", "envelope": ENVELOPE},
        "P",
    ),
    "kinesis": (
        {
            "kind": "kinesis",
            "direction": "internal",
            "stream_env": "S",
            "regions": ["us-east-1"],
            "envelope": ENVELOPE,
        },
        "S",
    ),
    "dynamodb_stream": (
        {
            "kind": "dynamodb_stream",
            "direction": "internal",
            "table_env": "T",
            "regions": ["us-east-1"],
            "filter": {"attribute": "status", "equals": "ABANDONED"},
            "envelope": ENVELOPE,
        },
        "T",
    ),
    "sns_fanout": (
        {
            "kind": "sns_fanout",
            "direction": "inbound",
            "topic_env": "N",
            "regions": ["us-east-1"],
            "envelope": ENVELOPE,
        },
        "N",
    ),
    "s3_capture": ({"kind": "s3_capture", "bucket_env": "B", "captures": [CAPTURE]}, "B"),
    "http_proxy": ({"kind": "http_proxy", "listen_port": 8021, "upstream_env": "U"}, "U"),
    "otlp": ({"kind": "otlp", "listen_env": "L"}, "L"),
}


def _source(kind: str = "kinesis", **overrides: Any) -> dict[str, Any]:
    source: dict[str, Any] = {"id": "events", **SOURCES[kind][0]}
    source.update(overrides)
    return source


def _source_error_locs(*sources: dict[str, Any]) -> list[tuple[str | int, ...]]:
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(sources=list(sources)))
    return [error["loc"] for error in caught.value.errors()]


@pytest.mark.unit
@pytest.mark.parametrize("kind", sorted(SOURCES))
def test_each_source_kind_loads_and_names_its_env_var(kind: str) -> None:
    [source] = SystemConfig.model_validate(_minimal(sources=[_source(kind)])).sources
    assert source.kind == kind
    assert source.location_env == SOURCES[kind][1]


@pytest.mark.unit
@pytest.mark.parametrize("kind", sorted(SOURCES))
def test_source_rejects_unknown_keys(kind: str) -> None:
    assert _source_error_locs(_source(kind, extra="x")) == [("sources", 0, kind, "extra")]


@pytest.mark.unit
def test_unknown_source_kind_is_rejected() -> None:
    assert _source_error_locs({**_source(), "kind": "kafka"}) == [("sources", 0)]


@pytest.mark.unit
@pytest.mark.parametrize("kind", sorted(SOURCES))
def test_dedupe_key_fails_naming_envelope_event_id(kind: str) -> None:
    with pytest.raises(ValidationError, match=r"dedupe_key was removed.*envelope\.event_id"):
        SystemConfig.model_validate(_minimal(sources=[_source(kind, dedupe_key="id")]))


@pytest.mark.unit
@pytest.mark.parametrize("kind", ["kinesis", "dynamodb_stream", "sns_fanout"])
def test_regional_source_needs_a_region(kind: str) -> None:
    assert _source_error_locs(_source(kind, regions=[])) == [("sources", 0, kind, "regions")]


@pytest.mark.unit
@pytest.mark.parametrize("kind", ["replay", "kinesis", "dynamodb_stream", "sns_fanout"])
@pytest.mark.parametrize("key", ["direction", "envelope"])
def test_event_source_needs_direction_and_envelope(kind: str, key: str) -> None:
    source = _source(kind)
    del source[key]
    assert _source_error_locs(source) == [("sources", 0, kind, key)]


@pytest.mark.unit
@pytest.mark.parametrize("kind", ["replay", "kinesis", "dynamodb_stream", "sns_fanout"])
def test_event_source_is_never_outbound(kind: str) -> None:
    locs = _source_error_locs(_source(kind, direction="outbound"))
    assert locs == [("sources", 0, kind, "direction")]


@pytest.mark.unit
@pytest.mark.parametrize("key", ["direction", "envelope"])
def test_s3_capture_takes_no_direction_or_envelope(key: str) -> None:
    value = ENVELOPE if key == "envelope" else "outbound"
    locs = _source_error_locs(_source("s3_capture", **{key: value}))
    assert locs == [("sources", 0, "s3_capture", key)]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("capture", "loc"),
    [
        ({**CAPTURE, "label": "Partner"}, ("captures", 0, "label")),
        ({**CAPTURE, "layout": "by_order"}, ("captures", 0, "layout")),
        ({**CAPTURE, "prefix": ""}, ("captures", 0, "prefix")),
        ({**CAPTURE, "order_id": "x"}, ("captures", 0, "order_id")),
    ],
    ids=["label", "layout", "prefix", "unknown-key"],
)
def test_s3_capture_checks_each_capture(capture: dict[str, Any], loc: tuple[Any, ...]) -> None:
    locs = _source_error_locs(_source("s3_capture", captures=[capture]))
    assert locs == [("sources", 0, "s3_capture", *loc)]


@pytest.mark.unit
def test_s3_capture_needs_unique_prefixes() -> None:
    captures = [CAPTURE, {**CAPTURE, "label": "other"}]
    with pytest.raises(ValidationError, match="duplicate capture prefixes: partner-payloads/"):
        SystemConfig.model_validate(_minimal(sources=[_source("s3_capture", captures=captures)]))


@pytest.mark.unit
def test_s3_capture_needs_a_capture() -> None:
    locs = _source_error_locs(_source("s3_capture", captures=[]))
    assert locs == [("sources", 0, "s3_capture", "captures")]


@pytest.mark.unit
def test_s3_capture_layouts() -> None:
    layouts = ["work_unit_action", "work_unit_order_action", "work_unit_order"]
    captures = [
        {"prefix": f"{layout}/", "label": "partner", "layout": layout} for layout in layouts
    ]
    [source] = SystemConfig.model_validate(
        _minimal(sources=[_source("s3_capture", captures=captures)])
    ).sources
    assert isinstance(source, S3CaptureSourceConfig)
    assert [capture.layout for capture in source.captures] == layouts


@pytest.mark.unit
def test_http_proxy_defaults() -> None:
    [source] = SystemConfig.model_validate(_minimal(sources=[_source("http_proxy")])).sources
    assert isinstance(source, HttpProxySourceConfig)
    assert (source.listen_host, source.listen_port) == ("127.0.0.1", 8021)
    assert source.openapi_path == "/openapi.json"
    assert source.openapi_refresh_s == 300
    assert source.max_body_bytes == 1_048_576


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("listen_port", 0),
        ("listen_port", 65536),
        ("listen_host", ""),
        ("openapi_path", "openapi.json"),
        ("openapi_path", "/open api.json"),
        ("openapi_refresh_s", 0),
        ("max_body_bytes", 0),
        ("direction", "inbound"),
        ("envelope", ENVELOPE),
    ],
)
def test_http_proxy_checks_its_keys(key: str, value: Any) -> None:
    locs = _source_error_locs(_source("http_proxy", **{key: value}))
    assert locs == [("sources", 0, "http_proxy", key)]


@pytest.mark.unit
@pytest.mark.parametrize("key", ["listen_port", "upstream_env"])
def test_http_proxy_needs_a_port_and_an_upstream(key: str) -> None:
    source = _source("http_proxy")
    del source[key]
    assert _source_error_locs(source) == [("sources", 0, "http_proxy", key)]


@pytest.mark.unit
def test_http_proxies_need_unique_listen_addresses() -> None:
    sources = [
        _source("http_proxy", id="api"),
        _source("http_proxy", id="other-host", listen_host="0.0.0.0"),
        _source("http_proxy", id="same", upstream_env="V"),
    ]
    with pytest.raises(ValidationError, match="duplicate listen addresses: 127.0.0.1:8021"):
        SystemConfig.model_validate(_minimal(sources=sources))


@pytest.mark.unit
def test_omg_proxies_sit_in_front_of_the_api_and_the_validation_engine() -> None:
    sources = {source.id: source for source in load_system_configs(SYSTEMS)["omg"].sources}
    proxies = {
        source_id: (source.listen_port, source.location_env)
        for source_id, source in sources.items()
        if isinstance(source, HttpProxySourceConfig)
    }
    assert proxies == {
        "omg-api": (8120, "OMG_API_UPSTREAM"),
        "omg-validation": (8130, "OMG_VALIDATION_UPSTREAM"),
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("listen_env", ""),
        ("listen_port", 4318),
        ("listen_host", "0.0.0.0"),
        ("direction", "inbound"),
        ("envelope", ENVELOPE),
        ("capture_event", {"name": "transmission.capture"}),
    ],
)
def test_otlp_checks_its_keys(key: str, value: Any) -> None:
    locs = _source_error_locs(_source("otlp", **{key: value}))
    assert locs == [("sources", 0, "otlp", key)]


@pytest.mark.unit
def test_otlp_needs_a_listen_env() -> None:
    source = _source("otlp")
    del source["listen_env"]
    assert _source_error_locs(source) == [("sources", 0, "otlp", "listen_env")]


@pytest.mark.unit
def test_omg_receives_its_collectors_spans() -> None:
    sources = {source.id: source for source in load_system_configs(SYSTEMS)["omg"].sources}
    traces = sources["omg-traces"]
    assert isinstance(traces, OtlpSourceConfig)
    assert traces.location_env == "OMG_OTLP_LISTEN"


@pytest.mark.unit
def test_operations_load_from_the_system_yaml() -> None:
    rule = {"hosts": ["api.partner.example.com"], "alias": "partner", "paths": ["/v1/{id}"]}
    config = SystemConfig.model_validate(_minimal(operations={"http": [rule]}))
    assert config.operations.http[0].paths == ["/v1/{id}"]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("operations", "loc"),
    [
        ({"grpc": []}, ("operations", "grpc")),
        ({"http": [{"hosts": ["a.example"], "alias": "A"}]}, ("operations", "http", 0, "alias")),
        (
            {"http": [{"hosts": ["a.example"], "alias": "a", "paths": ["/x", "/x"]}]},
            ("operations", "http", 0, "paths"),
        ),
    ],
    ids=["unknown-key", "alias", "duplicate-template"],
)
def test_operations_errors_name_their_location(
    operations: dict[str, Any], loc: tuple[str | int, ...]
) -> None:
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(operations=operations))
    assert [error["loc"] for error in caught.value.errors()] == [loc]


@pytest.mark.unit
def test_source_id_is_required() -> None:
    source = _source()
    del source["id"]
    assert _source_error_locs(source) == [("sources", 0, "kinesis", "id")]


@pytest.mark.unit
@pytest.mark.parametrize("source_id", ["domain-events", "rx_updates", "0", "a1"])
def test_source_id_accepts_slugs(source_id: str) -> None:
    config = SystemConfig.model_validate(_minimal(sources=[_source(id=source_id)]))
    assert config.source_ids() == {source_id}


@pytest.mark.unit
@pytest.mark.parametrize(
    "source_id",
    ["", "-events", "_events", "Events", "domain events", "domain.events", "events/1"],
    ids=["empty", "leading-dash", "leading-underscore", "uppercase", "space", "dot", "slash"],
)
def test_source_id_rejects_non_slugs(source_id: str) -> None:
    assert _source_error_locs(_source(id=source_id)) == [("sources", 0, "kinesis", "id")]


@pytest.mark.unit
def test_source_ids_are_unique_within_a_system() -> None:
    sources = [_source(id="b"), _source("replay", id="a"), _source(id="b"), _source(id="a")]
    with pytest.raises(ValidationError, match="duplicate source ids: a, b"):
        SystemConfig.model_validate(_minimal(sources=sources))


@pytest.mark.unit
def test_source_ids_may_repeat_across_systems() -> None:
    first = SystemConfig.model_validate(_minimal(sources=[_source()]))
    second = SystemConfig.model_validate(_minimal(system="other", sources=[_source()]))
    assert first.source_ids() == second.source_ids() == {"events"}


@pytest.mark.unit
def test_file_name_must_match_system(tmp_path: Path) -> None:
    path = _write(tmp_path, "other.yml", _minimal())
    with pytest.raises(ValueError, match="must match the file name"):
        load_system_config(path)


@pytest.mark.unit
def test_load_system_configs_skips_underscore_files(tmp_path: Path) -> None:
    _write(tmp_path, "example.yml", _minimal())
    _write(tmp_path, "_template.yml", _minimal(system="template"))
    (tmp_path / "notes.txt").write_text("ignored", encoding="utf-8")

    assert list(load_system_configs(tmp_path)) == ["example"]


@pytest.mark.unit
def test_load_system_configs_missing_directory(tmp_path: Path) -> None:
    assert load_system_configs(tmp_path / "absent") == {}
