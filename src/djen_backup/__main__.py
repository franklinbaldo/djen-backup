"""CLI entry point for ``djen-backup``."""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import click
import structlog

from djen_backup.credentials import get_ia_s3_auth
from djen_backup.runner import RunConfig, run

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

# Stable Cloud Run URL for the DJEN proxy service.
# Override via the DJEN_PROXY_URL environment variable if redeployed.
DEFAULT_PROXY_URL = "https://djen-proxy-mhgmawcn3a-rj.a.run.app"


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


@click.command()
@click.option(
    "--start-date",
    type=click.STRING,
    default=None,
    help="Start date (YYYY-MM-DD). Default: 7 days ago.",
)
@click.option(
    "--end-date",
    type=click.STRING,
    default=None,
    help="End date (YYYY-MM-DD). Default: yesterday.",
)
@click.option(
    "--tribunal",
    type=click.STRING,
    default=None,
    help="Process a single tribunal (e.g. TJSP).",
)
@click.option(
    "--deadline-minutes",
    type=int,
    default=45,
    show_default=True,
    help="Time budget in minutes.",
)
@click.option(
    "--max-items",
    type=int,
    default=0,
    help="Cap work queue size (0 = unlimited).",
)
@click.option(
    "--workers",
    type=int,
    default=8,
    show_default=True,
    help="Parallel workers.",
)
@click.option(
    "--state-file",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to persistent state cache JSON.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Log actions without uploading.",
)
@click.option(
    "--force-recheck",
    is_flag=True,
    default=False,
    help="Ignore state cache; re-query IA metadata for all dates.",
)
def main(
    start_date: str | None,
    end_date: str | None,
    tribunal: str | None,
    deadline_minutes: int,
    max_items: int,
    workers: int,
    state_file: Path | None,
    dry_run: bool,
    force_recheck: bool,
) -> None:
    """Back up DJEN judicial communications to the Internet Archive."""
    today = date.today()
    resolved_end = _parse_date(end_date) if end_date else today - timedelta(days=1)
    resolved_start = _parse_date(start_date) if start_date else resolved_end - timedelta(days=6)

    proxy_url = os.environ.get("DJEN_PROXY_URL", "").strip() or DEFAULT_PROXY_URL

    try:
        ia_auth = get_ia_s3_auth()
    except RuntimeError as exc:
        if dry_run:
            ia_auth = "LOW dry-run:dry-run"
        else:
            click.echo(f"Error: {exc}", err=True)
            sys.exit(1)

    config = RunConfig(
        start_date=resolved_start,
        end_date=resolved_end,
        tribunal=tribunal,
        deadline_minutes=deadline_minutes,
        max_items=max_items,
        workers=workers,
        state_file=state_file,
        djen_proxy_url=proxy_url,
        ia_auth=ia_auth,
        dry_run=dry_run,
        force_recheck=force_recheck,
    )

    log = structlog.get_logger()
    log.info(
        "starting",
        start=config.start_date.isoformat(),
        end=config.end_date.isoformat(),
        tribunal=config.tribunal or "all",
        workers=config.workers,
        deadline_min=config.deadline_minutes,
        dry_run=config.dry_run,
    )

    exit_code = asyncio.run(run(config))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
