"""BDD steps for the absent marking feature."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import date
from typing import TYPE_CHECKING, Any

import httpx
from pytest_bdd import given, parsers, scenario, then, when

from djen_backup.archive import CircuitBreaker
from djen_backup.runner import RunConfig, Summary, WorkItem, _process_item

if TYPE_CHECKING:
    import respx

    from djen_backup.state import State

FAKE_AUTH = "LOW test-access:test-secret"


# ── Scenarios ────────────────────────────────────────────────────────


@scenario("../absent_marking.feature", "Mark absent when DJEN returns 404")
def test_absent_404() -> None:
    pass


@scenario("../absent_marking.feature", "Mark absent when DJEN returns empty URL")
def test_absent_empty() -> None:
    pass


# ── Given ────────────────────────────────────────────────────────────


@given(
    parsers.parse('DJEN proxy returns 404 for "{tribunal}" on "{date_str}"'),
    target_fixture="item_context",
)
def given_djen_404(
    mock_api: respx.MockRouter,
    tribunal: str,
    date_str: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    url = f"https://djen-proxy.test/api/v1/caderno/{tribunal}/{date_str}/D"
    mock_api.get(url).respond(404)
    context["tribunal"] = tribunal
    context["date_str"] = date_str
    return context


@given(
    parsers.parse('DJEN proxy returns an empty URL for "{tribunal}" on "{date_str}"'),
    target_fixture="item_context",
)
def given_djen_empty_url(
    mock_api: respx.MockRouter,
    tribunal: str,
    date_str: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    url = f"https://djen-proxy.test/api/v1/caderno/{tribunal}/{date_str}/D"
    mock_api.get(url).respond(200, json={"url": ""})
    context["tribunal"] = tribunal
    context["date_str"] = date_str
    return context


@given("Internet Archive accepts uploads")
def given_ia_accepts(mock_api: respx.MockRouter, context: dict[str, Any]) -> None:
    context["ia_requests"] = []

    def _capture(request: httpx.Request) -> httpx.Response:
        context["ia_requests"].append(request)
        return httpx.Response(200)

    mock_api.put(url__startswith="https://s3.us.archive.org/").mock(side_effect=_capture)


# ── When ─────────────────────────────────────────────────────────────


@when(
    parsers.parse('I process the item "{tribunal}" on "{date_str}"'),
    target_fixture="process_result",
)
def when_process_item(
    state: State,
    mock_api: respx.MockRouter,
    tribunal: str,
    date_str: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    d = date.fromisoformat(date_str)
    item = WorkItem(date=d, tribunal=tribunal)
    config = RunConfig(
        start_date=d,
        end_date=d,
        tribunal=tribunal,
        deadline_minutes=45,
        max_items=0,
        workers=1,
        state_file=None,
        djen_proxy_url="https://djen-proxy.test",
        ia_auth=FAKE_AUTH,
        dry_run=False,
        force_recheck=False,
    )

    async def _run() -> None:
        summary = Summary(total=1)
        breaker = CircuitBreaker(threshold=5)
        sem = asyncio.Semaphore(1)
        deadline = time.monotonic() + 300
        async with httpx.AsyncClient() as client:
            await _process_item(client, sem, breaker, item, state, config, deadline, summary)
        context["summary"] = summary

    asyncio.run(_run())
    context["state"] = state
    context["date"] = d
    return context


# ── Then ─────────────────────────────────────────────────────────────


@then(
    parsers.parse('an absent marker should be uploaded as "{filename}"'),
)
def then_absent_uploaded(context: dict[str, Any], filename: str) -> None:
    ia_requests: list[httpx.Request] = context["ia_requests"]
    urls = [str(r.url) for r in ia_requests]
    matching = [u for u in urls if filename in u]
    assert matching, f"Expected absent marker upload for {filename}, got URLs: {urls}"


@then(
    parsers.parse("the absent marker should contain JSON with status_code {code:d}"),
)
def then_absent_json(context: dict[str, Any], code: int) -> None:
    ia_requests: list[httpx.Request] = context["ia_requests"]
    assert ia_requests
    body = json.loads(ia_requests[0].content)
    assert body["status_code"] == code
    assert "checked_at" in body


@then(
    parsers.parse('the state should mark "{tribunal}" on "{date_str}" as "{status}"'),
)
def then_state_mark(state: State, tribunal: str, date_str: str, status: str) -> None:
    d = date.fromisoformat(date_str)
    assert state.is_done(d, tribunal), f"{tribunal} on {date_str} not in state"
