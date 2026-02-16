# Contributing

## Setup

```bash
git clone <repo-url>
cd djen-backup
uv sync --all-extras
```

## Workflow

1. Create a branch from `main`.
2. Make changes.
3. Run all checks:

```bash
uv run ruff format src/ features/       # format
uv run ruff check --fix src/ features/  # lint + auto-fix
uv run mypy src/                        # type check (strict)
uv run pytest                           # BDD tests
```

4. Commit and open a PR.

## Adding a new feature

### Write the spec first

Create or extend a `.feature` file under `features/`:

```gherkin
Feature: My new feature
  Scenario: Description of the behavior
    Given some precondition
    When some action
    Then some outcome
```

Then implement step definitions in `features/steps/test_<feature>.py`.
Use `respx` to mock HTTP calls -- never make real network requests in tests.

### Code style

- Python 3.12+ syntax: `X | Y` unions, `StrEnum`, etc.
- mypy strict mode -- all functions must have type annotations.
- 100-character line length.
- No docstrings required on private helpers; public API should have one.
- Use `structlog` for all logging (not `print` or stdlib `logging`).

### Module responsibilities

Each module has a single responsibility. Before adding code, check if
it belongs in an existing module:

| If you're adding... | Put it in... |
|---|---|
| CLI options or startup logic | `__main__.py` |
| IA upload/download/metadata | `archive.py` |
| DJEN proxy interaction | `djen.py` |
| Retry/backoff logic | `retry.py` |
| Pipeline orchestration | `runner.py` |
| State cache behavior | `state.py` |
| Tribunal list changes | `tribunais.py` |
| Credential resolution | `credentials.py` |

## Testing

Tests are BDD-style using pytest-bdd. Each feature file maps to a step
file:

```
features/gap_detection.feature  ->  features/steps/test_gap_detection.py
features/collect.feature        ->  features/steps/test_collect.py
...
```

### Running a single scenario

```bash
uv run pytest -k "test_upload_zip"
```

### Mocking HTTP

All HTTP is mocked with `respx`. Pattern:

```python
import respx

@respx.mock
def my_test_step():
    respx.get("https://example.com/api").mock(
        return_value=httpx.Response(200, json={"key": "value"})
    )
    # ... run async code with asyncio.run()
```

## Internet Archive credentials

For local development use `--dry-run` to skip uploads entirely. If you
need real uploads for testing, get an IA S3 key pair from
https://archive.org/account/s3.php and set the environment variables
described in the README.
