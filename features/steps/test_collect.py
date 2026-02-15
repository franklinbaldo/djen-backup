"""BDD steps for the collect and upload feature."""

from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import date
from typing import TYPE_CHECKING, Any

import httpx
from pytest_bdd import given, parsers, scenario, then, when

from djen_backup.archive import CircuitBreaker
from djen_backup.runner import RunConfig, Summary, WorkItem, _process_item

from .conftest import parse_table

if TYPE_CHECKING:
    import respx

    from djen_backup.state import State

# ── Scenarios ────────────────────────────────────────────────────────


@scenario("../collect.feature", "Successfully download and upload a ZIP")
def test_upload_zip() -> None:
    pass


@scenario("../collect.feature", "Upload includes Content-MD5 header")
def test_upload_md5() -> None:
    pass


@scenario("../collect.feature", "Idempotent — already uploaded item is skipped")
def test_idempotent() -> None:
    pass


# ── Helpers ──────────────────────────────────────────────────────────

FAKE_AUTH = "LOW test-access:test-secret"


def _make_zip(size: int) -> bytes:
    return b"\x00" * size


# ── Given ────────────────────────────────────────────────────────────


@given(
    parsers.parse('DJEN proxy returns a caderno URL for "{tribunal}" on "{date_str}"'),
    target_fixture="item_context",
)
def given_djen_caderno(
    mock_api: respx.MockRouter,
    tribunal: str,
    date_str: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    download_url = f"https://djen-proxy.test/download/{tribunal}-{date_str}.zip"
    caderno_url = f"https://djen-proxy.test/api/v1/caderno/{tribunal}/{date_str}/D"
    mock_api.get(caderno_url).respond(200, json={"url": download_url})
    context["tribunal"] = tribunal
    context["date_str"] = date_str
    context["download_url"] = download_url
    return context


@given(
    parsers.parse("the caderno URL serves a valid ZIP of {size:d} bytes"),
)
def given_zip_download(
    mock_api: respx.MockRouter,
    size: int,
    context: dict[str, Any],
) -> None:
    content = _make_zip(size)
    context["zip_content"] = content
    mock_api.get(context["download_url"]).respond(200, content=content)


@given("Internet Archive accepts uploads")
def given_ia_accepts(mock_api: respx.MockRouter, context: dict[str, Any]) -> None:
    context["ia_requests"] = []

    def _capture(request: httpx.Request) -> httpx.Response:
        context["ia_requests"].append(request)
        return httpx.Response(200)

    mock_api.put(url__startswith="https://s3.us.archive.org/").mock(side_effect=_capture)


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
    "the tribunal list is:",
    target_fixture="tribunal_list",
)
def given_tribunal_list(datatable: list[list[str]]) -> list[str]:
    rows = parse_table(datatable)
    return [row["tribunal"] for row in rows]


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
        deadline = time.monotonic() + 300
        async with httpx.AsyncClient() as client:
            await _process_item(client, breaker, item, state, config, deadline, summary)
        context["summary"] = summary

    asyncio.run(_run())
    context["state"] = state
    context["date"] = d
    return context


@when(
    parsers.parse('I detect gaps for "{date_str}"'),
    target_fixture="gaps",
)
def when_detect_gaps(
    state: State,
    tribunal_list: list[str],
    date_str: str,
) -> list[WorkItem]:
    from djen_backup.runner import discover_gaps

    d = date.fromisoformat(date_str)

    async def _run() -> list[WorkItem]:
        async with httpx.AsyncClient() as client:
            return await discover_gaps(client, state, tribunal_list, d, d, force_recheck=False)

    return asyncio.run(_run())


# ── Then ─────────────────────────────────────────────────────────────


@then(
    parsers.parse('the ZIP should be uploaded to Internet Archive as "{filename}"'),
)
def then_uploaded(context: dict[str, Any], filename: str) -> None:
    ia_requests: list[httpx.Request] = context["ia_requests"]
    urls = [str(r.url) for r in ia_requests]
    matching = [u for u in urls if filename in u]
    assert matching, f"Expected upload of {filename}, got URLs: {urls}"


@then("the upload should include correct IA S3 headers")
def then_ia_headers(context: dict[str, Any]) -> None:
    ia_requests: list[httpx.Request] = context["ia_requests"]
    assert ia_requests, "No IA upload requests captured"
    req = ia_requests[0]
    assert req.headers.get("authorization") == FAKE_AUTH
    assert "x-archive-meta-collection" in req.headers
    assert "x-archive-meta-title" in req.headers
    assert req.headers["x-archive-auto-make-bucket"] == "1"
    assert req.headers["x-archive-queue-derive"] == "0"


@then(
    parsers.parse('the state should mark "{tribunal}" on "{date_str}" as "{status}"'),
)
def then_state_mark(state: State, tribunal: str, date_str: str, status: str) -> None:
    d = date.fromisoformat(date_str)
    assert state.is_done(d, tribunal), f"{tribunal} on {date_str} not marked in state"


@then("the upload Content-MD5 should match the file's MD5 hash")
def then_md5_matches(context: dict[str, Any]) -> None:
    import base64

    ia_requests: list[httpx.Request] = context["ia_requests"]
    assert ia_requests
    req = ia_requests[0]
    actual_md5 = req.headers.get("content-md5")
    digest = hashlib.md5(context["zip_content"], usedforsecurity=False).digest()
    expected_md5 = base64.b64encode(digest).decode("ascii")
    assert actual_md5 == expected_md5, f"MD5 mismatch: {actual_md5} != {expected_md5}"


@then("there should be no gaps")
def then_no_gaps(gaps: list[WorkItem]) -> None:
    assert len(gaps) == 0, f"Expected no gaps, got {gaps}"
