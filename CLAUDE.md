# CLAUDE.md

Project context for AI assistants working on this codebase.

## What this project does

Backs up Brazil's DJEN (Diario de Justica Eletronico Nacional) to the
Internet Archive. Runs as a scheduled GitHub Actions workflow every 20
minutes. Each run discovers which (date, tribunal) pairs are missing from
IA, downloads ZIPs from a DJEN proxy, and uploads them.

## Build and test commands

```bash
uv sync --all-extras               # install everything (first time)
uv run pytest                      # run all 14 BDD scenarios
uv run mypy src/                   # strict type checking
uv run ruff check src/ features/   # lint
uv run ruff format src/ features/  # auto-format
```

All four checks must pass before committing.

## Project layout

```
src/djen_backup/
  __main__.py      CLI (click). Entry point: `main()`.
  runner.py        Orchestration. `run()` is the async pipeline entry.
                   Worker pool uses asyncio.Queue with config.workers tasks.
  archive.py       IA S3 uploads, metadata queries, CircuitBreaker class.
  djen.py          DJEN proxy client. Raises DJENNotFound on 404/empty.
  state.py         JSON state cache. State class with mark/get_done_tribunals.
  retry.py         request_with_retry() — exponential backoff, Retry-After.
  credentials.py   Resolves IA S3 auth from env vars or ia.ini.
  tribunais.py     Hardcoded tribunal list + live API merge.

features/          Gherkin .feature files (BDD specs)
features/steps/    pytest-bdd step implementations with respx mocks
```

## Key patterns to understand

- **Worker pool**: `runner.py` uses `asyncio.Queue` with N worker tasks
  (not mass `create_task`). Each worker pulls items sequentially.
- **Circuit breaker**: `archive.py` CircuitBreaker has CLOSED/OPEN/HALF_OPEN
  states. After 5 failures, opens for 60s. Half-open allows one probe.
  The probe slot is consumed by transitioning back to OPEN atomically.
- **State cache**: Optimization only. IA metadata API is the source of truth.
  The cache skips IA queries for dates where all tribunals are already known.
- **Absent markers**: When DJEN returns 404, an `.absent` JSON file is
  uploaded to IA so future runs skip that (date, tribunal) pair.
- **Content-MD5**: Uses base64-encoded MD5 per the S3 spec (not hex).
  Uses `usedforsecurity=False` for FIPS compatibility.
- **DJEN 400 retry**: The DJEN proxy has a known transient 400 bug under
  load. `retry_djen_400=True` opts into retrying these.

## Important constraints

- Python >= 3.12 required (uses StrEnum, modern type syntax)
- mypy is configured with `--strict`
- Line length: 100 characters (ruff)
- Tests use pytest-bdd with Gherkin syntax, not plain pytest
- HTTP mocking uses `respx`, not `unittest.mock`
- async steps in tests use `asyncio.run()` wrappers (pytest-bdd limitation)
- The GHA workflow has a 50-minute timeout; the tool's default deadline is
  45 minutes with a 30-second safety margin

## Credential setup for local runs

Either set environment variables:
```bash
export IAS3_ACCESS_KEY=your_key
export IAS3_SECRET_KEY=your_secret
```

Or configure `~/.config/internetarchive/ia.ini`:
```ini
[s3]
access = your_key
secret = your_secret
```

Use `--dry-run` to skip credential requirements entirely.
