"""BDD steps for gap detection feature."""

from __future__ import annotations

import asyncio
from datetime import date
from typing import TYPE_CHECKING

import httpx
from pytest_bdd import given, parsers, scenario, then, when

from djen_backup.runner import WorkItem, discover_gaps
from djen_backup.state import ItemStatus, State

from .conftest import parse_table

if TYPE_CHECKING:
    import respx


@scenario("../gap_detection.feature", "Detect missing tribunals for a date")
def test_detect_missing() -> None:
    pass


@scenario("../gap_detection.feature", "No gaps when all tribunals are covered")
def test_no_gaps() -> None:
    pass


@scenario("../gap_detection.feature", "All tribunals missing when IA item does not exist")
def test_all_missing() -> None:
    pass


@scenario("../gap_detection.feature", "State cache skips IA query for fully-covered date")
def test_cache_skip() -> None:
    pass


# ── Given ────────────────────────────────────────────────────────────


@given(
    parsers.parse('Internet Archive has files for "{date_str}":'),
    target_fixture="ia_date_str",
)
def given_ia_files(
    mock_api: respx.MockRouter,
    date_str: str,
    datatable: list[list[str]],
) -> str:
    rows = parse_table(datatable)
    filenames = [row["filename"] for row in rows]
    payload = {"files": [{"name": fn} for fn in filenames]}
    mock_api.get(f"https://archive.org/metadata/djen-{date_str}").respond(200, json=payload)
    return date_str


@given(
    parsers.parse('Internet Archive has no item for "{date_str}"'),
    target_fixture="ia_date_str",
)
def given_ia_empty(mock_api: respx.MockRouter, date_str: str) -> str:
    mock_api.get(f"https://archive.org/metadata/djen-{date_str}").respond(200, json={"files": []})
    return date_str


@given(
    "the tribunal list is:",
    target_fixture="tribunal_list",
)
def given_tribunal_list(datatable: list[list[str]]) -> list[str]:
    rows = parse_table(datatable)
    return [row["tribunal"] for row in rows]


@given(
    parsers.parse('the state cache marks "{date_str}" as fully covered for:'),
    target_fixture="ia_date_str",
)
def given_state_cache_covered(
    state: State,
    date_str: str,
    datatable: list[list[str]],
) -> str:
    d = date.fromisoformat(date_str)
    rows = parse_table(datatable)
    for row in rows:
        state.mark(d, row["tribunal"], ItemStatus.UPLOADED)
    return date_str


# ── When ─────────────────────────────────────────────────────────────


@when(
    parsers.parse('I detect gaps for "{date_str}"'),
    target_fixture="gaps",
)
def when_detect_gaps(
    state: State,
    tribunal_list: list[str],
    date_str: str,
) -> list[WorkItem]:
    d = date.fromisoformat(date_str)

    async def _run() -> list[WorkItem]:
        async with httpx.AsyncClient() as client:
            return await discover_gaps(client, state, tribunal_list, d, d, force_recheck=False)

    return asyncio.run(_run())


# ── Then ─────────────────────────────────────────────────────────────


@then("the gaps should be:")
def then_gaps_are(gaps: list[WorkItem], datatable: list[list[str]]) -> None:
    rows = parse_table(datatable)
    expected = {row["tribunal"] for row in rows}
    actual = {item.tribunal for item in gaps}
    assert actual == expected, f"Expected gaps {expected}, got {actual}"


@then("there should be no gaps")
def then_no_gaps(gaps: list[WorkItem]) -> None:
    assert len(gaps) == 0, f"Expected no gaps, got {gaps}"


@then("the Internet Archive should not have been queried")
def then_ia_not_queried(mock_api: respx.MockRouter) -> None:
    for route in mock_api.routes:
        url_pattern = str(route.pattern)
        if "archive.org/metadata" in url_pattern:
            assert not route.called, f"IA metadata was queried: {url_pattern}"
