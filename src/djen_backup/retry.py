"""HTTP request retry logic with exponential backoff."""

from __future__ import annotations

import asyncio

import httpx
import structlog

log = structlog.get_logger()

RETRIABLE_STATUS_CODES: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504})


async def request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    max_retries: int = 3,
    retry_djen_400: bool = False,
    content: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    """Send an HTTP request with retries and exponential backoff.

    Retriable conditions:
    - Network / transport errors
    - Status codes: 408, 429, 500, 502, 503, 504
    - DJEN proxy 400 (transient) when *retry_djen_400* is True

    Respects ``Retry-After`` header when present.
    """
    last_exc: BaseException | None = None

    for attempt in range(max_retries + 1):
        try:
            resp = await client.request(
                method,
                url,
                content=content,
                headers=headers,
            )

            if resp.status_code in RETRIABLE_STATUS_CODES:
                wait = _backoff(attempt, resp)
                if attempt < max_retries:
                    log.warning(
                        "http_retry",
                        url=url,
                        status=resp.status_code,
                        attempt=attempt + 1,
                        wait_s=wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                return resp

            if retry_djen_400 and resp.status_code == 400:
                wait = _backoff(attempt, resp)
                if attempt < max_retries:
                    log.warning(
                        "djen_400_retry",
                        url=url,
                        attempt=attempt + 1,
                        wait_s=wait,
                    )
                    await asyncio.sleep(wait)
                    continue

            return resp

        except httpx.TransportError as exc:
            last_exc = exc
            if attempt < max_retries:
                wait = float(2**attempt)
                log.warning(
                    "http_transport_retry",
                    url=url,
                    error=str(exc),
                    attempt=attempt + 1,
                    wait_s=wait,
                )
                await asyncio.sleep(wait)
            else:
                raise

    # Should not reach here, but satisfy the type checker
    if last_exc is not None:
        raise last_exc  # pragma: no cover
    msg = "Exhausted retries"  # pragma: no cover
    raise RuntimeError(msg)  # pragma: no cover


def _backoff(attempt: int, resp: httpx.Response) -> float:
    """Compute wait time, respecting Retry-After header."""
    retry_after = resp.headers.get("Retry-After")
    if retry_after is not None:
        try:
            return max(float(retry_after), 1.0)
        except ValueError:
            pass
    return float(2**attempt)
