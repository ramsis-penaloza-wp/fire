"""Hooks shared by every test."""

from __future__ import annotations

import os

import pytest
from canary import Canary


@pytest.fixture
def canary() -> Canary:
    """Unique values to plant in records, and the checks that find leaks (tests/canary.py)."""
    return Canary()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip `omg_local` tests unless OMG_REPO points at an order-management-gateway checkout."""
    if os.environ.get("OMG_REPO"):
        return
    skip = pytest.mark.skip(reason="omg_local needs OMG_REPO")
    for item in items:
        if item.get_closest_marker("omg_local") is not None:
            item.add_marker(skip)
