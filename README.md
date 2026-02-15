# djen-backup

Complete backup of Brazil's **DJEN** (Diário de Justiça Eletrônico Nacional) to the [Internet Archive](https://archive.org).

DJEN publishes judicial communications daily for ~91 Brazilian courts. Each (date, tribunal) pair produces a ZIP. This tool auto-detects gaps on the Internet Archive and fills them.

## Quick start

```bash
uv run djen-backup                                              # last 7 days, auto-detect gaps
uv run djen-backup --start-date 2024-01-01 --end-date 2024-12-31  # backfill a year
uv run djen-backup --tribunal TJSP                              # single court
uv run djen-backup --dry-run                                    # preview without uploading
```

## How it works

1. **Discover gaps** — queries Internet Archive metadata for each date, compares files found against the full tribunal list.
2. **Download** — fetches ZIPs from the DJEN proxy.
3. **Upload** — pushes to IA's S3-compatible endpoint with proper `x-archive-meta-*` headers.
4. **Mark absent** — when DJEN returns 404 for a tribunal/date, uploads an `.absent` marker so we never retry.

A local state cache (JSON file) is used across GitHub Actions runs to skip already-scanned dates without querying IA metadata again.

## Environment variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `DJEN_PROXY_URL` | No | Base URL of DJEN proxy (default: `https://djen-proxy-mhgmawcn3a-rj.a.run.app`) |
| `IAS3_ACCESS_KEY` | Yes | Internet Archive S3 access key |
| `IAS3_SECRET_KEY` | Yes | Internet Archive S3 secret key |

Fallback: if env vars are empty, reads from `~/.config/internetarchive/ia.ini` `[s3]` section.

## CLI options

```
--start-date       Start date YYYY-MM-DD (default: 7 days ago)
--end-date         End date YYYY-MM-DD (default: yesterday)
--tribunal         Filter to a single court (e.g. TJSP)
--deadline-minutes Time budget (default: 45)
--max-items        Cap work queue size (0 = unlimited)
--workers          Parallel workers (default: 8)
--state-file       Path to persistent state cache JSON
--dry-run          Log actions without uploading
--force-recheck    Ignore state cache, re-query IA metadata
```

## Development

```bash
uv sync --all-extras
uv run pytest                  # run BDD tests
uv run mypy src/               # type checking (strict)
uv run ruff check src/         # lint
uv run ruff format --check src/  # format check
```

## License

MIT
