# djen-backup

Complete backup of Brazil's **DJEN** (Diario de Justica Eletronico Nacional) to the [Internet Archive](https://archive.org).

DJEN publishes judicial communications daily for ~91 Brazilian courts.
Each (date, tribunal) pair produces a ZIP file. This tool auto-detects gaps
on the Internet Archive and fills them, running unattended via GitHub Actions
every 20 minutes.

## Quick start

```bash
uv run djen-backup                                                 # last 7 days, auto-detect gaps
uv run djen-backup --start-date 2024-01-01 --end-date 2024-12-31  # backfill a year
uv run djen-backup --tribunal TJSP                                 # single court
uv run djen-backup --dry-run                                       # preview without uploading
```

## How it works

```
┌──────────────────┐    ┌───────────────┐    ┌──────────────────┐
│  1. Discover gaps │───>│ 2. Download   │───>│  3. Upload to IA │
│  (IA metadata)    │    │ (DJEN proxy)  │    │  (S3-compat API) │
└──────────────────┘    └───────────────┘    └──────────────────┘
                                                     │
                                              ┌──────┴───────┐
                                              │ 4. Mark absent│
                                              │ (if DJEN 404) │
                                              └──────────────┘
```

1. **Discover gaps** -- queries Internet Archive metadata for each date,
   compares files found against the full tribunal list.
2. **Download** -- fetches ZIPs from the DJEN proxy service.
3. **Upload** -- pushes to IA's S3-compatible endpoint with
   `x-archive-meta-*` headers and base64-encoded `Content-MD5`.
4. **Mark absent** -- when DJEN returns 404 for a tribunal/date, uploads
   an `.absent` marker so we never retry.

A local **state cache** (JSON file) persists across GitHub Actions runs to
skip already-scanned dates without querying IA metadata again.

## Architecture

```
src/djen_backup/
├── __main__.py      # CLI entry point (click)
├── runner.py        # Orchestration: gap discovery -> worker pool -> summary
├── archive.py       # IA S3 upload, metadata queries, circuit breaker
├── djen.py          # DJEN proxy client (caderno lookup + ZIP download)
├── state.py         # Persistent JSON state cache
├── retry.py         # HTTP retry with exponential backoff + Retry-After
├── credentials.py   # IA S3 credential resolution (env vars / ia.ini)
└── tribunais.py     # Tribunal list (hardcoded + live API merge)
```

Key design decisions:

- **Bounded worker pool** -- `config.workers` (default 8) `asyncio` tasks
  pull items from an `asyncio.Queue`, so only a bounded number of HTTP
  requests are in flight at any time.
- **Circuit breaker** -- after 5 consecutive IA upload failures, the
  breaker opens for 60 s (doubling on each retry, capped at 5 min).
  A single half-open probe request is allowed before full recovery.
- **Deadline awareness** -- stops processing items 30 seconds before the
  configured deadline to avoid partial uploads near the GHA timeout.
- **Structured logging** -- all events use `structlog` with ISO timestamps
  for operational observability.

## CLI reference

```
Usage: djen-backup [OPTIONS]

  Back up DJEN judicial communications to the Internet Archive.

Options:
  --start-date TEXT       Start date YYYY-MM-DD (default: 7 days ago)
  --end-date TEXT         End date YYYY-MM-DD (default: yesterday)
  --tribunal TEXT         Filter to a single court (e.g. TJSP)
  --deadline-minutes INT  Time budget in minutes (default: 45)
  --max-items INT         Cap work queue size; 0 = unlimited (default: 0)
  --workers INT           Parallel workers (default: 8)
  --state-file PATH       Path to persistent state cache JSON
  --dry-run               Log actions without uploading
  --force-recheck         Ignore state cache, re-query IA metadata
```

## Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `IAS3_ACCESS_KEY` | Yes* | Internet Archive S3 access key |
| `IAS3_SECRET_KEY` | Yes* | Internet Archive S3 secret key |
| `DJEN_PROXY_URL` | No | Base URL of DJEN proxy (default: built-in Cloud Run URL) |

\* Not required with `--dry-run`. Falls back to
`~/.config/internetarchive/ia.ini` `[s3]` section if env vars are unset.

## GitHub Actions

The workflow (`.github/workflows/backup.yml`) runs every 20 minutes via
cron and can also be triggered manually with optional `start_date`,
`end_date`, and `tribunal` inputs.

The state cache is persisted as a GitHub Actions artifact (`djen-state`)
with 30-day retention to avoid redundant IA metadata queries between runs.

**Required repository secrets:**

| Secret | Maps to |
|---|---|
| `IA_ACCESS_KEY` | `IAS3_ACCESS_KEY` |
| `IA_SECRET_KEY` | `IAS3_SECRET_KEY` |

## Internet Archive item layout

Each date gets one IA item named `djen-{YYYY-MM-DD}` containing:

```
djen-2024-01-15/
├── djen-2024-01-15-TJSP.zip       # full ZIP from DJEN
├── djen-2024-01-15-TJRJ.zip
├── djen-2024-01-15-TJRO.absent    # JSON marker: DJEN had no data
└── ...
```

The `.absent` markers contain JSON with the original status code, reason,
and the timestamp of the check.

## Development

```bash
uv sync --all-extras               # install all dependencies
uv run pytest                      # BDD tests (14 scenarios, 5 feature files)
uv run mypy src/                   # strict type checking
uv run ruff check src/ features/   # lint
uv run ruff format --check src/ features/  # format check
```

### Test structure

Tests use **pytest-bdd** with Gherkin feature files under `features/`:

| Feature file | Covers |
|---|---|
| `gap_detection.feature` | IA metadata parsing, state cache fast-path |
| `collect.feature` | Download + upload pipeline, Content-MD5, idempotency |
| `absent_marking.feature` | 404 / empty-URL handling, `.absent` marker upload |
| `circuit_breaker.feature` | Open / half-open / close transitions |
| `deadline.feature` | Deadline guard, full processing when time is available |

HTTP calls are mocked with [respx](https://github.com/lundberg/respx).

## License

MIT -- see [LICENSE](LICENSE).
