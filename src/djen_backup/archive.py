"""Internet Archive S3 upload, metadata queries, and circuit breaker."""

from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import TYPE_CHECKING

import structlog

from djen_backup.retry import request_with_retry

if TYPE_CHECKING:
    import httpx

log = structlog.get_logger()

# ── IA metadata ──────────────────────────────────────────────────────

IA_METADATA_URL = "https://archive.org/metadata/djen-{date}"


async def fetch_ia_existing(
    client: httpx.AsyncClient,
    d: date,
) -> dict[str, str]:
    """Query IA metadata; return ``{tribunal: "uploaded"|"absent"}``."""
    url = IA_METADATA_URL.format(date=d.isoformat())
    resp = await request_with_retry(client, "GET", url)
    if resp.status_code != 200:
        log.warning("ia_metadata_error", date=d.isoformat(), status=resp.status_code)
        return {}

    try:
        data: dict[str, object] = resp.json()
    except ValueError:
        return {}

    result: dict[str, str] = {}
    files = data.get("files")
    if not isinstance(files, list):
        return result

    prefix = f"djen-{d.isoformat()}-"
    for entry in files:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str) or not name.startswith(prefix):
            continue
        rest = name[len(prefix) :]
        if rest.endswith(".zip"):
            result[rest[: -len(".zip")]] = "uploaded"
        elif rest.endswith(".absent"):
            result[rest[: -len(".absent")]] = "absent"
    return result


# ── IA S3 upload ─────────────────────────────────────────────────────

IA_S3_URL = "https://s3.us.archive.org/djen-{date}/{filename}"


def _md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _build_upload_headers(
    d: date,
    md5_hex: str,
    auth: str,
) -> dict[str, str]:
    return {
        "Authorization": auth,
        "Content-MD5": md5_hex,
        "x-archive-auto-make-bucket": "1",
        "x-archive-queue-derive": "0",
        "x-archive-meta-collection": "opensource",
        "x-archive-meta-mediatype": "data",
        "x-archive-meta-title": f"DJEN Data - {d.isoformat()}",
        "x-archive-meta-description": (
            "Diario de Justica Eletronico Nacional - Judicial communications from Brazilian courts."
        ),
        "x-archive-meta-subject": "brazilian-law;djen;legal;judiciary;open-data",
        "x-archive-meta-creator": "CausaGanha",
        "x-archive-meta-date": d.isoformat(),
    }


async def upload_zip(
    client: httpx.AsyncClient,
    d: date,
    tribunal: str,
    content: bytes,
    auth: str,
) -> httpx.Response:
    """Upload a ZIP to IA S3."""
    filename = f"djen-{d.isoformat()}-{tribunal}.zip"
    url = IA_S3_URL.format(date=d.isoformat(), filename=filename)
    md5 = _md5_hex(content)
    headers = _build_upload_headers(d, md5, auth)

    log.info("ia_upload_start", date=d.isoformat(), tribunal=tribunal, size=len(content))
    resp = await request_with_retry(
        client,
        "PUT",
        url,
        content=content,
        headers=headers,
    )
    log.info(
        "ia_upload_done",
        date=d.isoformat(),
        tribunal=tribunal,
        status=resp.status_code,
    )
    return resp


async def upload_absent_marker(
    client: httpx.AsyncClient,
    d: date,
    tribunal: str,
    status_code: int,
    reason: str,
    auth: str,
) -> httpx.Response:
    """Upload a ``.absent`` marker with metadata JSON."""
    import json

    filename = f"djen-{d.isoformat()}-{tribunal}.absent"
    url = IA_S3_URL.format(date=d.isoformat(), filename=filename)

    body = json.dumps(
        {
            "status_code": status_code,
            "reason": reason,
            "checked_at": datetime.now(tz=UTC).isoformat(),
        }
    ).encode()

    md5 = _md5_hex(body)
    headers = _build_upload_headers(d, md5, auth)

    log.info("ia_absent_marker", date=d.isoformat(), tribunal=tribunal)
    resp = await request_with_retry(
        client,
        "PUT",
        url,
        content=body,
        headers=headers,
    )
    return resp


# ── Circuit breaker ──────────────────────────────────────────────────


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker with half-open recovery for IA uploads.

    - CLOSED: normal operation, count consecutive failures.
    - OPEN: after *threshold* failures, refuse requests for *recovery_timeout* seconds.
    - HALF_OPEN: after timeout elapses, allow **one** test request.
      Success → CLOSED.  Failure → OPEN with doubled timeout (capped at 5 min).
    """

    def __init__(
        self,
        threshold: int = 5,
        recovery_timeout: float = 60.0,
    ) -> None:
        self._threshold = threshold
        self._base_recovery = recovery_timeout
        self._recovery_timeout = recovery_timeout
        self._failure_count = 0
        self._state = CircuitState.CLOSED
        self._opened_at = 0.0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        if (
            self._state == CircuitState.OPEN
            and time.monotonic() - self._opened_at >= self._recovery_timeout
        ):
            return CircuitState.HALF_OPEN
        return self._state

    async def allow_request(self) -> bool:
        async with self._lock:
            s = self.state
            if s == CircuitState.CLOSED:
                return True
            if s == CircuitState.HALF_OPEN:
                self._state = CircuitState.HALF_OPEN
                return True
            return False

    async def record_success(self) -> None:
        async with self._lock:
            self._failure_count = 0
            self._state = CircuitState.CLOSED
            self._recovery_timeout = self._base_recovery

    async def record_failure(self) -> None:
        async with self._lock:
            self._failure_count += 1
            if self._state == CircuitState.HALF_OPEN:
                # Test request failed — reopen with increased timeout
                self._recovery_timeout = min(self._recovery_timeout * 2, 300.0)
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()
                log.warning(
                    "circuit_breaker_reopen",
                    next_retry_s=self._recovery_timeout,
                )
            elif self._failure_count >= self._threshold:
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()
                log.error(
                    "circuit_breaker_open",
                    failures=self._failure_count,
                    recovery_s=self._recovery_timeout,
                )
