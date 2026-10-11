"""openapi_routes: OpenAPI `paths` to routes, and OMG's documents through RouteTable."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import httpx
import pytest

from fire.adapters.manifests.openapi import KIND, OPERATIONS, OpenApiRoutes, openapi_routes
from fire.core.domain.route_template import MAX_TEMPLATE_LENGTH, RouteTable

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "omg" / "openapi"
API = FIXTURES / "order_management_api.json"
VALIDATION = FIXTURES / "item_rx_validation_service.json"
LIVE = {
    API: "http://localhost:8020/openapi.json",
    VALIDATION: "http://localhost:8030/openapi.json",
}
IDS = ("300000123456789", "01J9ZKX4W3V8Q2T6M5N7P0R1S2", "SO-1234567")
PLACEHOLDER = re.compile(r"\{[^/{}]+\}")


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _pairs(result: OpenApiRoutes) -> list[tuple[str, str]]:
    return [(route.method, route.template.template) for route in result.routes]


@pytest.mark.unit
def test_one_route_per_declared_operation() -> None:
    result = openapi_routes(
        {
            "paths": {
                "/items": {"get": {}, "post": {}},
                "/items/{item_id}": {operation: {} for operation in OPERATIONS},
                "/healthcheck": {"get": {"operationId": "healthcheck"}},
            }
        }
    )
    assert _pairs(result) == [
        ("GET", "/items"),
        ("POST", "/items"),
        *((operation.upper(), "/items/{item_id}") for operation in OPERATIONS),
        ("GET", "/healthcheck"),
    ]
    assert result.dropped == 0


@pytest.mark.unit
def test_non_operation_keys_are_ignored() -> None:
    result = openapi_routes(
        {
            "paths": {
                "/items/{item_id}": {
                    "summary": "One item",
                    "parameters": [{"name": "item_id", "in": "path"}],
                    "servers": [{"url": "/v2"}],
                    "x-internal": True,
                    "GET": {},
                    "delete": {},
                },
                "/described": {"summary": "No operations", "parameters": []},
            }
        }
    )
    assert _pairs(result) == [("DELETE", "/items/{item_id}")]
    assert result.dropped == 0


@pytest.mark.unit
def test_servers_prefix_is_not_applied() -> None:
    result = openapi_routes({"servers": [{"url": "/base"}], "paths": {"/items": {"get": {}}}})
    assert _pairs(result) == [("GET", "/items")]


@pytest.mark.unit
def test_invalid_keys_and_non_object_items_are_dropped_and_counted() -> None:
    paths: dict[Any, Any] = {
        "/ok/{id}": {"get": {}},
        "no-leading-slash": {"get": {}},
        "/files/{name}.json": {"get": {}},
        "/" + "a" * MAX_TEMPLATE_LENGTH: {"get": {}},
        "/with space": {"get": {}},
        1: {"get": {}},
        "/list": [{"get": {}}],
        "/null": None,
        "/text": "get",
    }
    result = openapi_routes({"paths": paths})
    assert _pairs(result) == [("GET", "/ok/{id}")]
    assert result.dropped == 8


@pytest.mark.unit
@pytest.mark.parametrize(
    "document",
    [{}, {"openapi": "3.0.2"}, {"paths": []}, {"paths": None}, [], "paths", None],
    ids=["empty", "no-paths", "paths-list", "paths-null", "list", "string", "null"],
)
def test_document_without_paths_object_raises(document: Any) -> None:
    with pytest.raises(ValueError, match=f"^{KIND}: "):
        openapi_routes(document)


@pytest.mark.unit
def test_api_fixture_gives_one_route_per_operation() -> None:
    document = _load(API)
    expected = [
        (operation.upper(), path) for path, item in document["paths"].items() for operation in item
    ]
    result = openapi_routes(document)
    assert sorted(_pairs(result)) == sorted(expected)
    assert len(set(expected)) == len(expected)
    assert result.dropped == 0


@pytest.mark.unit
def test_validation_fixture_routes() -> None:
    result = openapi_routes(_load(VALIDATION))
    assert _pairs(result) == [("POST", "/v1/validate"), ("GET", "/healthcheck")]
    assert result.dropped == 0


@pytest.mark.unit
@pytest.mark.parametrize("fixture", [API, VALIDATION], ids=["api", "validation"])
@pytest.mark.parametrize("value", IDS)
def test_every_route_routes_its_own_concrete_path(fixture: Path, value: str) -> None:
    result = openapi_routes(_load(fixture))
    assert all(value not in route.template.literals for route in result.routes)
    table = RouteTable(result.routes)
    for method, template in _pairs(result):
        path = PLACEHOLDER.sub(value, template)
        assert table.match(method, path) == template, f"{method} {path}"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("method", "path", "template"),
    [
        ("GET", "/v1/orders/search", "/v1/orders/search"),
        ("GET", "/v1/orders/SO-1234567", "/v1/orders/{order_id}"),
        ("GET", "/v1/tasks/filter-options", "/v1/tasks/filter-options"),
        ("GET", "/v1/tasks/01J9ZKX4W3V8Q2T6M5N7P0R1S2", "/v1/tasks/{work_unit_id}"),
        ("GET", "/v1/scheduled-jobs/latest", "/v1/scheduled-jobs/latest"),
        ("GET", "/v1/scheduled-jobs/rules", "/v1/scheduled-jobs/rules"),
        (
            "POST",
            "/v1/orders/order-services-snapshot/preview",
            "/v1/orders/order-services-snapshot/preview",
        ),
        ("GET", "/v1/orders/123/logs", "/v1/orders/{order_id}/logs"),
        ("POST", "/v1/orders/place_order", "/v1/orders/place_order"),
    ],
)
def test_api_literal_beats_placeholder(method: str, path: str, template: str) -> None:
    assert RouteTable(openapi_routes(_load(API)).routes).match(method, path) == template


@pytest.mark.unit
def test_api_method_without_the_literal_falls_to_the_placeholder() -> None:
    table = RouteTable(openapi_routes(_load(API)).routes)
    assert table.match("GET", "/v1/orders/place_order") == "/v1/orders/{order_id}"
    assert table.match("GET", "/v1/tasks/close") == "/v1/tasks/{work_unit_id}"
    assert table.match("GET", "/v1/scheduled-jobs/300000123456789") == (
        "/v1/scheduled-jobs/{job_name}"
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("DELETE", "/v1/orders/place_order"),
        ("GET", "/v1/orders/order-services-snapshot/preview"),
        ("GET", "/v1/bulk-reallocation"),
        ("GET", "/v1/classification/resolve"),
        ("HEAD", "/healthcheck"),
        ("get", "/healthcheck"),
        ("GET", "/healthcheck/"),
        ("GET", "/v1/orders/"),
    ],
)
def test_api_unmatched_requests(method: str, path: str) -> None:
    assert RouteTable(openapi_routes(_load(API)).routes).match(method, path) is None


@pytest.mark.unit
def test_validation_fixture_matches() -> None:
    table = RouteTable(openapi_routes(_load(VALIDATION)).routes)
    assert table.match("POST", "/v1/validate") == "/v1/validate"
    assert table.match("GET", "/healthcheck") == "/healthcheck"
    assert table.match("GET", "/v1/validate") is None
    assert table.match("HEAD", "/healthcheck") is None


@pytest.mark.unit
@pytest.mark.omg_local
@pytest.mark.parametrize("fixture", [API, VALIDATION], ids=["api", "validation"])
def test_fixture_routes_match_omgs_live_document(fixture: Path) -> None:
    with httpx.Client(trust_env=False, timeout=5.0) as client:
        response = client.get(LIVE[fixture])
    response.raise_for_status()
    live = openapi_routes(response.json())
    assert live.dropped == 0
    assert set(_pairs(openapi_routes(_load(fixture)))) == set(_pairs(live))
