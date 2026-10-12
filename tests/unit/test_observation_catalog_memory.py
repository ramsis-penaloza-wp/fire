"""MemoryObservationCatalog: each observation stored once, streams summarized per stream key."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from spans import Clock, order_spans, trace_id

from fire.adapters.memory.observation_catalog import MemoryObservationCatalog
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.core.domain.baseline import BaselineStatus, DriftPolicy
from fire.core.domain.dossier import Shape, ShapeField, ValueType
from fire.core.domain.drift import Admission, ShapeAdmission
from fire.core.domain.flow import FlowCounters, Node, PathPolicy
from fire.core.domain.observation import (
    Channel,
    ChannelDirection,
    ChannelKind,
    ChannelPart,
    ShapeObservation,
    SpanKind,
    SpanObservation,
)
from fire.core.domain.path import PathsReport
from fire.core.ports.observation_catalog import ObservationCatalog

T0 = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID = "00f067aa0ba902b7"
FOLDED = ShapeAdmission(outcome=Admission.FOLDED)
DUPLICATE = ShapeAdmission(outcome=Admission.DUPLICATE)
DRIFTED = ("order_id", "coupon")


def _channel(name: str = "order.created") -> Channel:
    return Channel(
        kind=ChannelKind.EVENT,
        direction=ChannelDirection.INTERNAL,
        name=name,
        part=ChannelPart.PAYLOAD,
    )


def _shape_observation(
    observation_id: str = "obs-1",
    *,
    paths: tuple[str, ...] = ("order_id",),
    observed_at: datetime = T0,
    **overrides: Any,
) -> ShapeObservation:
    shape = Shape(fields=[ShapeField(path=path, value_type=ValueType.STRING) for path in paths])
    data: dict[str, Any] = {
        "system": "shop",
        "source": "order-events",
        "channel": _channel(),
        "observation_id": observation_id,
        "occurred_at": T0,
        "observed_at": observed_at,
        "shape": shape,
        "fingerprint": Sha256ShapeFingerprinter().fingerprint(shape),
    }
    data.update(overrides)
    return ShapeObservation(**data)


def _span(**overrides: Any) -> SpanObservation:
    data: dict[str, Any] = {
        "system": "shop",
        "source": "traces",
        "trace_id": TRACE_ID,
        "span_id": SPAN_ID,
        "service": "orders-api",
        "kind": SpanKind.SERVER,
        "operation": "POST /v1/orders",
        "error": False,
        "start": T0,
        "duration_ms": 12.5,
    }
    data.update(overrides)
    return SpanObservation(**data)


def _summary(catalog: MemoryObservationCatalog, system: str = "shop") -> list[dict[str, Any]]:
    return [
        {
            "source": stream.source,
            "key": stream.key,
            "status": stream.status,
            "samples": stream.samples,
            "observations": stream.observations,
            "fingerprints": [
                (f.schema_hash, f.observations, f.first_observed_at, f.last_observed_at)
                for f in stream.fingerprints
            ],
            "drift": stream.drift,
        }
        for stream in catalog.streams(system)
    ]


def _baselines(
    catalog: MemoryObservationCatalog, system: str = "shop"
) -> list[tuple[BaselineStatus, int]]:
    return [(stream.status, stream.samples) for stream in catalog.streams(system)]


def _warm(min_samples: int = 2) -> MemoryObservationCatalog:
    catalog = MemoryObservationCatalog({"shop": DriftPolicy(min_samples=min_samples)})
    for index in range(min_samples):
        catalog.add_shape(_shape_observation(f"warm-{index}"))
    return catalog


def _schema_hash(paths: tuple[str, ...]) -> str:
    return _shape_observation(paths=paths).fingerprint.schema_hash


@pytest.mark.unit
def test_memory_catalog_implements_the_port() -> None:
    assert isinstance(MemoryObservationCatalog(), ObservationCatalog)


@pytest.mark.unit
def test_first_add_stores_and_summarizes_the_stream() -> None:
    catalog = MemoryObservationCatalog()
    observation = _shape_observation()

    assert catalog.add_shape(observation) == FOLDED

    [stream] = catalog.streams("shop")
    assert stream.source == "order-events"
    assert stream.channel == observation.channel
    assert stream.key == "event:internal:order.created:payload"
    assert stream.status is BaselineStatus.REBUILDING
    assert stream.samples == 1
    assert stream.observations == 1
    [fingerprint] = stream.fingerprints
    assert fingerprint.schema_hash == observation.fingerprint.schema_hash
    assert fingerprint.observations == 1
    assert fingerprint.first_observed_at == fingerprint.last_observed_at == T0


@pytest.mark.unit
def test_second_add_of_one_shape_key_is_a_duplicate_and_changes_nothing() -> None:
    catalog = MemoryObservationCatalog()
    assert catalog.add_shape(_shape_observation()) == FOLDED
    before = _summary(catalog)

    resent = _shape_observation(
        paths=("order_id", "total"), observed_at=T0 + timedelta(hours=1), work_id="w-2"
    )
    assert catalog.add_shape(resent) == DUPLICATE

    assert _summary(catalog) == before


@pytest.mark.unit
def test_second_add_of_one_span_key_returns_false() -> None:
    catalog = MemoryObservationCatalog()
    assert catalog.add_span(_span()) is True
    assert catalog.add_span(_span(operation="GET /v1/orders", error=True)) is False


@pytest.mark.unit
def test_concurrent_adds_store_once() -> None:
    catalog = MemoryObservationCatalog()
    workers = 16
    barrier = threading.Barrier(workers)
    observation = _shape_observation()
    span = _span()

    def add(_: int) -> tuple[ShapeAdmission, bool]:
        barrier.wait()
        return catalog.add_shape(observation), catalog.add_span(span)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(add, range(workers)))

    assert [shape.stored for shape, _ in results].count(True) == 1
    assert [shape for shape, _ in results].count(DUPLICATE) == workers - 1
    assert [stored for _, stored in results].count(True) == 1
    [stream] = catalog.streams("shop")
    assert stream.observations == stream.samples == 1


@pytest.mark.unit
def test_one_event_from_two_sources_is_two_observations() -> None:
    catalog = MemoryObservationCatalog()
    assert catalog.add_shape(_shape_observation(source="order-events")) == FOLDED
    assert catalog.add_shape(_shape_observation(source="order-table")) == FOLDED

    streams = catalog.streams("shop")
    assert [(stream.source, stream.observations) for stream in streams] == [
        ("order-events", 1),
        ("order-table", 1),
    ]


@pytest.mark.unit
def test_one_span_from_two_sources_is_one_observation() -> None:
    catalog = MemoryObservationCatalog()
    assert catalog.add_span(_span(source="traces")) is True
    assert catalog.add_span(_span(source="traces-replica")) is False


@pytest.mark.unit
def test_one_observation_id_in_two_systems_is_two_observations() -> None:
    catalog = MemoryObservationCatalog()
    assert catalog.add_shape(_shape_observation(system="shop")) == FOLDED
    assert catalog.add_shape(_shape_observation(system="billing")) == FOLDED
    assert catalog.add_span(_span(system="shop")) is True
    assert catalog.add_span(_span(system="billing")) is True

    assert [stream.observations for stream in catalog.streams("shop")] == [1]
    assert [stream.observations for stream in catalog.streams("billing")] == [1]


@pytest.mark.unit
def test_streams_of_a_system_without_observations_is_empty() -> None:
    catalog = MemoryObservationCatalog()
    catalog.add_shape(_shape_observation())
    catalog.add_span(_span(system="billing"))
    assert catalog.streams("billing") == []
    assert catalog.streams("unknown") == []


@pytest.mark.unit
def test_streams_are_sorted_by_source_then_key() -> None:
    catalog = MemoryObservationCatalog()
    added = [
        ("order-table", "order.created"),
        ("order-events", "order.shipped"),
        ("order-events", "order.created"),
        ("billing-events", "refund.issued"),
    ]
    for index, (source, name) in enumerate(added):
        catalog.add_shape(
            _shape_observation(f"obs-{index}", source=source, channel=_channel(name))
        )

    assert [(stream.source, stream.channel.name) for stream in catalog.streams("shop")] == [
        ("billing-events", "refund.issued"),
        ("order-events", "order.created"),
        ("order-events", "order.shipped"),
        ("order-table", "order.created"),
    ]


@pytest.mark.unit
def test_fingerprints_are_sorted_by_count_then_schema_hash() -> None:
    catalog = MemoryObservationCatalog()
    forms = [("a",), ("b",), ("c",), ("c",), ("a",), ("d",), ("a",)]
    for index, paths in enumerate(forms):
        catalog.add_shape(_shape_observation(f"obs-{index}", paths=paths))

    [stream] = catalog.streams("shop")
    hashes = {paths: _shape_observation(paths=paths).fingerprint.schema_hash for paths in forms}
    tied = sorted([hashes[("b",)], hashes[("d",)]])
    assert [(f.schema_hash, f.observations) for f in stream.fingerprints] == [
        (hashes[("a",)], 3),
        (hashes[("c",)], 2),
        (tied[0], 1),
        (tied[1], 1),
    ]
    assert stream.observations == len(forms)


@pytest.mark.unit
def test_fingerprint_times_are_the_earliest_and_latest_observed_at() -> None:
    catalog = MemoryObservationCatalog()
    for index, minutes in enumerate([10, 0, 30, 20]):
        catalog.add_shape(
            _shape_observation(f"obs-{index}", observed_at=T0 + timedelta(minutes=minutes))
        )

    [stream] = catalog.streams("shop")
    [fingerprint] = stream.fingerprints
    assert fingerprint.observations == 4
    assert fingerprint.first_observed_at == T0
    assert fingerprint.last_observed_at == T0 + timedelta(minutes=30)


@pytest.mark.unit
def test_stream_turns_warm_at_the_systems_min_samples() -> None:
    catalog = MemoryObservationCatalog({"shop": DriftPolicy(min_samples=3)})
    catalog.add_shape(_shape_observation("obs-0"))
    catalog.add_shape(_shape_observation("obs-1"))
    assert _baselines(catalog) == [(BaselineStatus.REBUILDING, 2)]

    catalog.add_shape(_shape_observation("obs-2"))
    assert _baselines(catalog) == [(BaselineStatus.WARM, 3)]


@pytest.mark.unit
def test_drift_policies_are_per_system() -> None:
    catalog = MemoryObservationCatalog(
        {"shop": DriftPolicy(min_samples=2), "billing": DriftPolicy()}
    )
    for system in ("shop", "billing"):
        for index in range(2):
            catalog.add_shape(_shape_observation(f"obs-{index}", system=system))

    assert _baselines(catalog, "shop") == [(BaselineStatus.WARM, 2)]
    assert _baselines(catalog, "billing") == [(BaselineStatus.REBUILDING, 2)]


@pytest.mark.unit
def test_system_without_a_policy_gets_the_default_min_samples() -> None:
    catalog = MemoryObservationCatalog({"billing": DriftPolicy(min_samples=1)})
    for index in range(19):
        catalog.add_shape(_shape_observation(f"obs-{index}"))
    assert _baselines(catalog) == [(BaselineStatus.REBUILDING, 19)]

    catalog.add_shape(_shape_observation("obs-19"))
    assert _baselines(catalog) == [(BaselineStatus.WARM, 20)]


@pytest.mark.unit
def test_duplicate_add_folds_nothing() -> None:
    catalog = MemoryObservationCatalog({"shop": DriftPolicy(min_samples=2)})
    assert catalog.add_shape(_shape_observation()) == FOLDED
    assert catalog.add_shape(_shape_observation(paths=("order_id", "total"))) == DUPLICATE

    assert _baselines(catalog) == [(BaselineStatus.REBUILDING, 1)]


@pytest.mark.unit
def test_samples_count_every_stored_observation_while_rebuilding() -> None:
    catalog = MemoryObservationCatalog()
    forms = [("a",), ("b",), ("a",), ("a", "b")]
    for index, paths in enumerate(forms):
        catalog.add_shape(_shape_observation(f"obs-{index}", paths=paths))

    [stream] = catalog.streams("shop")
    assert len(stream.fingerprints) == 3
    assert stream.samples == stream.observations == len(forms)


@pytest.mark.unit
def test_each_stream_key_has_its_own_baseline() -> None:
    catalog = MemoryObservationCatalog({"shop": DriftPolicy(min_samples=2)})
    added = [
        ("obs-1", "order-events", "order.created"),
        ("obs-1", "order-table", "order.created"),
        ("obs-2", "order-events", "order.created"),
        ("obs-3", "order-events", "order.shipped"),
    ]
    for observation_id, source, name in added:
        catalog.add_shape(
            _shape_observation(observation_id, source=source, channel=_channel(name))
        )

    streams = catalog.streams("shop")
    assert [
        (stream.source, stream.channel.name, stream.status, stream.samples) for stream in streams
    ] == [
        ("order-events", "order.created", BaselineStatus.WARM, 2),
        ("order-events", "order.shipped", BaselineStatus.REBUILDING, 1),
        ("order-table", "order.created", BaselineStatus.REBUILDING, 1),
    ]


@pytest.mark.unit
def test_streams_list_drift_signals_by_first_seen_then_schema_hash() -> None:
    catalog = _warm()
    added = [
        ("late", ("order_id", "a"), 10),
        ("tie-1", ("order_id", "b"), 5),
        ("tie-2", ("order_id", "c"), 5),
        ("early", ("order_id", "a"), 1),
    ]
    for observation_id, paths, minutes in added:
        catalog.add_shape(
            _shape_observation(
                observation_id, paths=paths, observed_at=T0 + timedelta(minutes=minutes)
            )
        )

    [stream] = catalog.streams("shop")
    tied = sorted([_schema_hash(("order_id", "b")), _schema_hash(("order_id", "c"))])
    assert [(s.schema_hash, s.occurrences, s.first_seen) for s in stream.drift] == [
        (_schema_hash(("order_id", "a")), 2, T0 + timedelta(minutes=1)),
        (tied[0], 1, T0 + timedelta(minutes=5)),
        (tied[1], 1, T0 + timedelta(minutes=5)),
    ]


@pytest.mark.unit
def test_drifted_observations_count_in_fingerprints_but_not_in_samples() -> None:
    catalog = _warm()
    for index in range(3):
        assert catalog.add_shape(_shape_observation(f"drift-{index}", paths=DRIFTED)).stored

    [stream] = catalog.streams("shop")
    assert (stream.status, stream.samples, stream.observations) == (BaselineStatus.WARM, 2, 5)
    assert [(f.schema_hash, f.observations) for f in stream.fingerprints] == [
        (_schema_hash(DRIFTED), 3),
        (_schema_hash(("order_id",)), 2),
    ]
    assert [(s.schema_hash, s.occurrences) for s in stream.drift] == [(_schema_hash(DRIFTED), 3)]


@pytest.mark.unit
def test_duplicate_of_a_drifted_observation_adds_no_occurrence() -> None:
    catalog = _warm()
    drifted = _shape_observation("drift-0", paths=DRIFTED)
    assert catalog.add_shape(drifted).outcome is Admission.DRIFT_RAISED
    before = _summary(catalog)

    resent = _shape_observation("drift-0", paths=DRIFTED, observed_at=T0 + timedelta(hours=1))
    assert catalog.add_shape(resent) == DUPLICATE

    assert _summary(catalog) == before
    [stream] = catalog.streams("shop")
    assert [signal.occurrences for signal in stream.drift] == [1]


@pytest.mark.unit
def test_concurrent_adds_of_one_drifted_fingerprint_raise_one_signal() -> None:
    catalog = _warm()
    workers = 16
    barrier = threading.Barrier(workers)

    def add(index: int) -> Admission:
        observation = _shape_observation(f"drift-{index}", paths=DRIFTED)
        barrier.wait()
        return catalog.add_shape(observation).outcome

    with ThreadPoolExecutor(max_workers=workers) as pool:
        outcomes = list(pool.map(add, range(workers)))

    assert outcomes.count(Admission.DRIFT_RAISED) == 1
    assert outcomes.count(Admission.DRIFT_REPEATED) == workers - 1
    [stream] = catalog.streams("shop")
    assert [signal.occurrences for signal in stream.drift] == [workers]
    assert (stream.samples, stream.observations) == (2, 2 + workers)


# Flows and paths


def _flow_catalog(
    min_samples: int = 2, settle_s: float = 60
) -> tuple[MemoryObservationCatalog, Clock]:
    clock = Clock()
    catalog = MemoryObservationCatalog(
        {"shop": DriftPolicy(min_samples=min_samples)},
        path_policies={"shop": PathPolicy(settle_s=settle_s)},
        clock=clock,
    )
    return catalog, clock


def _add_spans(catalog: MemoryObservationCatalog, spans: list[SpanObservation]) -> None:
    for item in spans:
        assert catalog.add_span(item) is True


@pytest.mark.unit
def test_a_settled_flow_folds_into_its_path() -> None:
    catalog, clock = _flow_catalog()
    _add_spans(catalog, order_spans(1, "DynamoDB.PutItem orders"))

    clock.advance(59)
    assert catalog.complete_flows() == []
    assert catalog.paths("shop").open_flows == 1
    clock.advance(1)
    assert catalog.complete_flows() == []

    report = catalog.paths("shop")
    assert report.open_flows == 0
    [path] = report.paths
    assert (path.entry, path.status, path.flows) == (
        Node(service="orders-api", operation="POST /orders"),
        BaselineStatus.REBUILDING,
        1,
    )


@pytest.mark.unit
def test_a_duplicate_span_does_not_extend_its_flows_settle_time() -> None:
    catalog, clock = _flow_catalog()
    spans = order_spans(1, "DynamoDB.PutItem orders")
    _add_spans(catalog, spans)

    clock.advance(30)
    assert catalog.add_span(spans[1]) is False
    clock.advance(30)
    catalog.complete_flows()

    assert catalog.paths("shop").open_flows == 0


@pytest.mark.unit
def test_complete_flows_returns_the_path_drift_of_a_flow_with_a_new_signal() -> None:
    catalog, clock = _flow_catalog()
    for trace in (1, 2):
        _add_spans(catalog, order_spans(trace, "DynamoDB.PutItem orders"))
    clock.advance(60)
    assert catalog.complete_flows() == []

    _add_spans(catalog, order_spans(3, "DynamoDB.PutItem orders", "POST pay.example.com/v1"))
    _add_spans(catalog, order_spans(4, "DynamoDB.PutItem orders", "POST pay.example.com/v1"))
    clock.advance(60)
    [drift] = catalog.complete_flows()

    assert (drift.system, drift.entry.operation, drift.trace_ids) == (
        "shop",
        "POST /orders",
        [trace_id(3)],
    )
    [path] = catalog.paths("shop").paths
    assert (path.status, path.flows, path.drifted_flows) == (BaselineStatus.WARM, 2, 2)
    [signal] = path.drift
    assert (signal.rule, signal.node.operation, signal.occurrences) == (
        "hop_added",
        "POST pay.example.com/v1",
        2,
    )


@pytest.mark.unit
def test_each_system_has_its_own_flows_and_path_policy() -> None:
    clock = Clock()
    catalog = MemoryObservationCatalog(
        path_policies={"shop": PathPolicy(settle_s=10)}, clock=clock
    )
    _add_spans(catalog, order_spans(1, "DynamoDB.PutItem orders"))
    _add_spans(catalog, order_spans(1, "DynamoDB.PutItem orders", system="billing"))

    clock.advance(10)
    catalog.complete_flows()

    assert [path.flows for path in catalog.paths("shop").paths] == [1]
    billing = catalog.paths("billing")
    assert (billing.open_flows, billing.paths) == (1, [])


@pytest.mark.unit
def test_stored_shapes_with_a_trace_join_their_flow_and_duplicates_do_not() -> None:
    catalog, clock = _flow_catalog()
    spans = order_spans(1, "DynamoDB.PutItem orders")
    _add_spans(catalog, spans)
    joined = _shape_observation(
        "shape-1", trace_id=trace_id(1), span_id=spans[1].span_id, observed_at=T0
    )
    assert catalog.add_shape(joined).stored
    assert catalog.add_shape(joined) == DUPLICATE
    assert catalog.add_shape(_shape_observation("shape-2")).stored

    clock.advance(60)
    catalog.complete_flows()

    [path] = catalog.paths("shop").paths
    put = next(node for node in path.nodes if node.operation == "DynamoDB.PutItem orders")
    assert put.channels == {"event:internal:order.created:payload": 1}
    assert catalog.paths("shop").counters.unattached_shapes == 0


@pytest.mark.unit
def test_paths_of_a_system_without_spans_is_an_empty_report() -> None:
    report = MemoryObservationCatalog().paths("shop")

    assert report == PathsReport(system="shop", open_flows=0, counters=FlowCounters(), paths=[])


@pytest.mark.unit
def test_paths_returns_copies_of_the_counters() -> None:
    catalog, _clock = _flow_catalog()
    catalog.paths("shop").counters.late_spans = 5
    _add_spans(catalog, order_spans(1))

    assert catalog.paths("shop").counters.late_spans == 0
