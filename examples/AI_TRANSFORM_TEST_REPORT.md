# Loafer AI transform test report

**Date:** 2026-07-29

**Suite:** `examples/ai_transform_tests.py` — 45 tests

**Offline (scripted provider):** 45 passed, 0 failed, 0 skipped (exit code 0)

**Live (`openai / gpt-5.6-terra`):** 30 passed, 0 failed, 15 skipped (exit code 0)

**Repository pytest:** 673 passed, 50 skipped, 0 failed

The suite uses a scripted provider, so generated Python/SQL is deterministic
while the real Loafer pipeline still performs prompt preparation, validation,
sandbox execution, retries, destructive-change checks, incremental extraction,
loading, and PostgreSQL ELT execution.

Run it with the isolated PostgreSQL integration database:

```bash
docker run -d --name loafer-ai-test \
  -e POSTGRES_USER=loafer \
  -e POSTGRES_PASSWORD=loafer \
  -e POSTGRES_DB=loafer_test \
  -p 127.0.0.1:55432:5432 \
  postgres:16-alpine

export LOAFER_PG_URL=postgresql://loafer:loafer@127.0.0.1:55432/loafer_test
uv run python examples/ai_transform_tests.py --with-postgres
```

## Resolved cases

Eleven defects were found in the first pass over the AI transform surface; all
are now fixed, and each has a test that fails again if it regresses.

### Critical

| ID | Was | Now | Regression coverage |
| --- | --- | --- | --- |
| ELT-1 | `mode: elt` could never succeed. `load_raw_agent` generated a staging table name, put it in the model's prompt, then built its connector from `target_config` — so raw rows went to the **final target table** and the staging table was never created. Even a perfect model's SQL hit `relation ... does not exist`. | Raw rows are loaded into a unique staging table in the final target's schema. The staging table is removed after success or failure. | `elt_ai_sql_creates_the_target_table` |
| ELT-2 | A failed ELT run reported success: the agent recorded `last_error` instead of raising, so the CLI printed all-green stages and exited 0 while the target table held raw, unfiltered rows. | Exhausted ELT retries raise `PipelineError`; streaming reports the SQL stage as failed and cursors are not committed. | `failed_elt_transform_is_reported_as_a_failure`, `elt_rejects_non_select_sql` |
| AI-4 | Multi-step pipelines containing an `ai` step died on a bare `KeyError: 'llm_provider'` — the runner only built a provider when the *top-level* transform type was `ai`, which never matches `pipeline`. | The runner initializes an LLM provider when any enabled step in a multi-step transform uses AI. | `multi_step_pipeline_with_an_ai_step`, `failing_step_names_itself_and_prior_steps` |

### High

| ID | Was | Now | Regression coverage |
| --- | --- | --- | --- |
| AI-1 | Custom transform code never reached the model. The runner built a prompt containing it and discarded the result; `LLMProvider.generate_transform_function` had no `custom_code` parameter, so every provider rebuilt the prompt without it. | `custom_code` is part of the `LLMProvider` contract and is included by every provider when building the prompt. | `custom_code_actually_reaches_the_model` |
| AI-3 | Execution errors in generated code were never retried on the `AITransformConfig` path — the most common real-world model failure was the one failure not corrected, despite the docstring promising retries. | Runtime errors use the same three-attempt correction budget as generation and validation errors, including previous code and traceback feedback. | `runtime_error_is_fed_back_and_corrected` |
| AI-6 | `incremental` was silently dropped for CSV, Excel, MongoDB, and PDF sources: no filtering, no warning, and the watermark still advanced — so every run reprocessed everything while claiming progress. | Those sources apply incremental cursors client-side and warn that this requires a source scan. Streamed runs advance the watermark after consumption. | `incremental_cursor_limits_what_the_model_sees` |

### Medium

| ID | Was | Now | Regression coverage |
| --- | --- | --- | --- |
| AI-2 | In `custom_first` mode the model was described a schema built from the *raw* rows, while its generated code received the custom transform's output — so it could silently drop the custom columns. | `custom_first` recomputes schema metadata from the custom transform's output before invoking the model. | `custom_runs_before_ai` |
| AI-5 | `total_count`, `null_count`, and `nullable` were computed from `raw_data[:5]` but presented as whole-dataset facts, so a column whose only null appeared later was advertised as non-nullable. | Column statistics are computed across all materialized rows while prompt sample values remain bounded to five. | `schema_sample_statistics_describe_the_whole_dataset` |
| AI-7 | Permanently fatal 400s (exhausted credit, bad model id) were retried three times with 2s/4s/8s backoff; only `LLMAuthError` short-circuited. | Permanent HTTP 4xx provider errors fail immediately; transient responses such as 408, 409, 425, 429, and 5xx remain retryable. | `permanent_400s_are_not_retried`, `rate_limit_is_retried_with_backoff` |

### Fixed concurrently

Two further defects were corrected in the working tree while testing was in
progress, via a new `loafer/llm/models.py` providing per-provider defaults:

| Was | Now | Regression coverage |
| --- | --- | --- |
| The Claude default was `claude-sonnet-4-20250514` — **past its 2026-06-15 retirement**, so every Claude run would 404. | `claude-sonnet-5` | `configured_claude_model_is_a_live_model_id` |
| `LLMConfig.model` defaulted to a Gemini id regardless of provider, so `provider: claude` with no `model:` sent a Gemini id to Anthropic. | Resolved per provider via `default_model_for()`. | `claude_provider_ignores_the_configured_model_default` |

## Probed and sound

These were attacked specifically and held up, and each keeps a test:

- **AST validation before execution.** Blocked imports (`os`, `subprocess`,
  `pathlib`, …) and blocked calls (`eval`, `exec`, `open`, `compile`) are
  rejected before `exec`, and the rejection reason is fed back to the model,
  which corrects on the next attempt.
- **The sandbox as a real second line of defence.** `open()` is absent from the
  execution globals; a `while True` transform is killed on wall-clock timeout; a
  400 MB allocation under a 128 MB cap is killed. Limits come from the
  pipeline's `sandbox:` block.
- **SQL validation.** `DROP` / `DELETE` / `UPDATE` / `INSERT` / `CREATE` and
  statement stacking (`SELECT 1; DROP TABLE users`) are rejected by the sqlglot
  AST walk — verified against a live PostgreSQL that a model emitting
  `DROP TABLE` leaves the database intact.
- **Destructive-change detection.** A transform dropping all but one column is
  blocked, no output is written, and `--yes` is the explicit opt-in that records
  a warning.
- **The review gate.** `review: true` renders the generated code; `n` skips
  execution, `y` runs it, and EOF (piped/CI stdin) declines rather than
  auto-approving.
- **Prompt hygiene.** The full dataset never reaches the model — only column
  metadata and ≤5 sample values, with long strings truncated to 100 characters,
  bounding what untrusted source data can inject into the context.
- **`bypass_ai`** runs the custom transform with zero provider calls and no API
  key, and errors clearly when no `custom_path` is configured.
- **Auditability.** Token usage accumulates across retries and the executed code
  is recorded on the state.
- **`--dry-run`** runs the model and the transform but writes nothing.

## Verification

Re-run on 2026-07-29 against the working tree, with the integration PostgreSQL
container above:

- `python examples/ai_transform_tests.py --with-postgres`:
  **45 passed, 0 failed, 0 skipped, 0 known defects** (exit code 0)
- PostgreSQL ELT subset (`-k elt`): **6 passed**
- Full repository pytest: **673 passed, 50 skipped, 0 failed**
- Ruff over `examples/` and `loafer/`: **clean**

### Regression found and fixed during this run

The first full pytest run reported **2 failures**:

```
FAILED tests/e2e/test_pipeline_e2e.py::TestFullEtlJsonTarget::test_csv_to_json_no_filtering
FAILED tests/e2e/test_pipeline_e2e.py::TestStreamingLargeDataset::test_streaming_csv_source
```

Both were caused by this report's own example rewrite deleting
`examples/transforms/sample_transform.py`. That file is not only an example:
`tests/e2e/test_pipeline_e2e.py` hard-codes its path as a no-op transform
fixture, so a pipeline's extract and load stages can be asserted on without a
transform altering the data. Restoring it (with a docstring recording the
dependency) returns the E2E file to **3 passed, 3 skipped** and the repository
to **0 failures**.

An earlier draft of this section described these as "unrelated E2E isolation
failures" alongside three scheduler CLI failures. That was wrong on both counts:
the E2E failures were a real regression introduced here, and the three scheduler
CLI failures did not reproduce in this environment.

## Independent verification of the critical ELT fixes

The suite's own markers for these defects were removed when they were fixed, so
ELT-1 and ELT-2 were re-checked directly rather than through the suite.

**ELT-2 — a failing ELT run now fails loudly.** Driving the real CLI with
deliberately broken generated SQL:

```
  ✗  Generating and executing SQL  [0.0s]
╭─────────────── Pipeline Failed (run_id=ec6945214d96) ────────────────╮
│ SQL execution failed: ELT SQL execution failed:                      │
│ relation "nowhere" does not exist                                    │
╰──────────────────────────────────────────────────────────────────────╯
$ echo $?
1
```

Exit code 1 (previously 0), and `pg_tables` afterwards reports **no tables at
all** — the target table is no longer left holding raw rows, and the staging
table is cleaned up on the failure path.

**ELT-1 — the staging table is real.** With a provider emitting exactly the
SELECT the prompt asked for, against the exact table name the prompt supplied:

```
staging table named in prompt : loafer_raw_postgres_8663ff88
rows_loaded                   : 8
orders_clean rows, non-lowercased emails : (8, 0)
tables remaining              : orders_clean
```

The staging table is created, selected from, and dropped after success.

### Correction to the original finding

The first version of this report asserted the ELT case should yield **9** rows.
That was wrong. The ETL instruction drops cancelled and refunded orders, leaving
9 (pending included); the ELT instruction filters `status = 'paid'`, and the
fixture contains exactly **8** paid orders. The expected value is 8. The
mistake was never caught originally because ELT-1 aborted the run before any
row count was reached.

## Live-provider acceptance run

The suite has now been run end-to-end against a real model, with a real model
writing every transform:

```bash
export OPENAI_API_KEY=sk-proj-...
export LOAFER_PG_URL=postgresql://loafer:loafer@127.0.0.1:55432/loafer_test
python examples/ai_transform_tests.py --live --provider openai --with-postgres
```

**Result: 30 passed, 0 failed, 15 skipped** (exit code 0), against
`openai / gpt-5.6-terra` — Loafer's own `default_model_for("openai")`.

`--provider` / `--model` were added for this: they rewrite each pipeline's
`llm:` block after `load_config` and clear `api_key`, so Loafer's real
`_build_llm_provider` — including its environment-variable key lookup — remains
the code under test. Without them the example pipelines pin Anthropic.

What a real model actually did, end to end:

| Scenario | Live outcome |
| --- | --- |
| `01_ai_basic` | model wrote the normalisation, 9 rows survived, emails lowercased, `amount_usd` added |
| `02` / `03` custom ordering | both orders composed correctly; custom and AI columns coexist |
| `06_multi_step_ai` | `tag_region:12→12 → normalize:12→9 → emea_only:9→3` |
| `07_ai_elt_postgres` | model's SELECT validated and executed as CTAS; `orders_clean` created with 8 rows |
| `08_ai_incremental` | 6 rows past the watermark, cursor advanced to `2026-06-06T15:30:00Z` |
| `05_ai_review` | generated code rendered for review; decline skipped it, approve ran it |
| sandbox / validators | unchanged — they do not depend on which model wrote the code |

The 15 skips are structural, not gaps: they force conditions a real model cannot
be made to produce on demand (blocked imports, three consecutive bad
generations, a `ZeroDivisionError`, DDL from a model told not to emit DDL, a
429, a bad key) or inspect prompt contents that only the scripted provider
records. Each prints its reason.

### Anthropic remains unverified

The Anthropic key available during this work authenticates and reaches the API
but returns `400 invalid_request_error: "Your credit balance is too low"` for
every model (`claude-opus-5`, `claude-sonnet-5`, `claude-haiku-4-5`); the
`GEMINI_API_KEY` in `.env` is rejected as invalid. With a funded key:

```bash
python examples/ai_transform_tests.py --live --with-postgres
```

## Follow-up from the live run

### SPAWN-1 — resolved: programmatic use no longer requires a `__main__` guard

The POSIX sandbox previously used `multiprocessing.get_context("spawn")`.
Python's spawn bootstrap re-imported the caller's `__main__`, so a script that
called `run_pipeline()` at module level recursively ran the whole pipeline and
eventually timed out with a misleading transform error.

The sandbox now launches a dedicated internal module worker through the current
Python interpreter. The worker receives one serialized request, applies the
same address-space and CPU limits, runs the restricted transform, and returns
one serialized response. It never imports the user's calling script.

`test_programmatic_pipeline_without_main_guard` drives a real pipeline from a
top-level script with deliberately no guard and asserts that it exits normally,
writes its output, and reports one loaded row. The existing timeout, memory,
filesystem, runtime-error, and CLI paths remain green.

### The review gate prompts once per candidate, not once per run

With `review: true`, a run that regenerates after a failed attempt shows the
reviewer each new candidate. Observed live: two generations, two prompts. This
is defensible — you are approving the code that will actually run — but it means
an operator can be prompted up to three times for one pipeline, which is worth
documenting. `review_gate_accepted_runs_the_ai_code` now asserts on the prompt
count rather than assuming a single prompt.
