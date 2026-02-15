"""Shared fixtures for BDD step implementations."""

from __future__ import annotations

from typing import Any

import pytest
import respx

from djen_backup.archive import CircuitBreaker
from djen_backup.state import State


@pytest.fixture()
def mock_api() -> respx.MockRouter:
    """A ``respx`` mock router activated for the test."""
    with respx.mock(assert_all_called=False) as router:
        yield router


@pytest.fixture()
def state() -> State:
    return State()


@pytest.fixture()
def circuit_breaker() -> CircuitBreaker:
    return CircuitBreaker(threshold=5, recovery_timeout=60.0)


@pytest.fixture()
def context() -> dict[str, Any]:
    """A mutable bag for sharing data across Given/When/Then steps."""
    return {}


def parse_table(datatable: list[list[str]]) -> list[dict[str, str]]:
    """Convert pytest-bdd datatable (list of lists) to list of dicts."""
    headers = datatable[0]
    return [dict(zip(headers, row, strict=False)) for row in datatable[1:]]
