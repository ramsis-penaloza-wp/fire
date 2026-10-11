"""MemoryObservationCatalog: each observation stored once, streams summarized per stream key."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from fire.adapters.memory.observation_catalog import MemoryObservationCatalog
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.core.domain.baseline import BaselineStatus, DriftPolicy
from fire.core.domain.dossier import Shape, ShapeField, ValueType
from fire.core.domain.observation import (
    Channel,
    ChannelDirection,
    ChannelKind,
    ChannelPart,
    ShapeObservation,
    SpanKind,
    SpanObservation,
)
from fire.core.ports.observation_catalog import ObservationCatalog

T0 = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID = "00f067aa0ba902b7"


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
        }
        for stream in catalog.streams(system)
    ]


def _baselines(
    catalog: MemoryObservationCatalog, system: str = "shop"
) -> list[tuple[BaselineStatus, int]]:
    return [(stream.status, stream.samples) for stream in catalog.streams(system)]


@pytest.mark.unit
def test_memory_catalog_implements_the_port() -> None:
    assert isinstance(MemoryObservationCatalog(), ObservationCatalog)


@pytest.mark.unit
def test_first_add_stores_and_summarizes_the_stream() -> None:
    catalog = MemoryObservationCatalog()
    observation = _shape_observation()

    assert catalog.add_shape(observation) is True

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
def test_second_add_of_one_shape_key_returns_false_and_changes_nothing() -> None:
    catalog = MemoryObservationCatalog()
    assert catalog.add_shape(_shape_observation()) is True
    before = _summary(catalog)

    resent = _shape_observation(
        paths=("order_id", "total"), observed_at=T0 + timedelta(hours=1), work_id="w-2"
    )
    assert catalog.add_shape(resent) is False

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

    def add(_: int) -> tuple[bool, bool]:
        barrier.wait()
        return catalog.add_shape(observation), catalog.add_span(span)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(add, range(workers)))

    assert [shape for shape, _ in results].count(True) == 1
    assert [stored for _, stored in results].count(True) == 1
    [stream] = catalog.streams("shop")
    assert stream.observations == stream.samples == 1


@pytest.mark.unit
def test_one_event_from_two_sources_is_two_observations() -> None:
    catalog = MemoryObservationCatalog()
    assert catalog.add_shape(_shape_observation(source="order-events")) is True
    assert catalog.add_shape(_shape_observation(source="order-table")) is True

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
    assert catalog.add_shape(_shape_observation(system="shop")) is True
    assert catalog.add_shape(_shape_observation(system="billing")) is True
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
    assert catalog.add_shape(_shape_observation()) is True
    assert catalog.add_shape(_shape_observation(paths=("order_id", "total"))) is False

    assert _baselines(catalog) == [(BaselineStatus.REBUILDING, 1)]


@pytest.mark.unit
def test_samples_count_every_stored_observation_of_a_stream() -> None:
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
