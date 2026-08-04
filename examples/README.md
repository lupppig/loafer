# Loafer examples — AI transforms

Runnable pipelines plus a manual end-to-end test suite for Loafer's AI-assisted
transforms. Everything here drives Loafer the way a user does: real YAML, real
CSV input, real files on disk, real Postgres.

```
examples/
  pipelines/            nine pipelines covering the AI transform surface
  transforms/           hand-written Python transforms the AI examples compose with
  data/orders.csv       the shared fixture (12 orders, 3 currencies, 4 statuses)
  scripted_llm.py       a scriptable LLMProvider — replays canned model output
  ai_transform_tests.py the test suite
  output/               written by the examples
```

## Run the examples

```bash
export ANTHROPIC_API_KEY=sk-ant-...

uv run loafer validate examples/pipelines/01_ai_basic.yaml
uv run loafer run      examples/pipelines/01_ai_basic.yaml
uv run loafer run      examples/pipelines/01_ai_basic.yaml --dry-run
```

| Pipeline | Covers |
| --- | --- |
| `01_ai_basic.yaml` | plain AI transform, CSV → JSON |
| `02_ai_after_custom.yaml` | `custom_order: custom_first` |
| `03_ai_before_custom.yaml` | `custom_order: ai_first` |
| `04_bypass_ai.yaml` | `bypass_ai: true` — no model call, no API key |
| `05_ai_review.yaml` | `review: true` — human approves the generated code |
| `06_multi_step_ai.yaml` | multi-step custom → ai → sql |
| `07_ai_elt_postgres.yaml` | ELT: model writes SQL that runs in the warehouse |
| `08_ai_incremental.yaml` | AI transform + incremental cursor |
| `09_ai_sandbox_limits.yaml` | tight sandbox timeout / memory cap |

`04_bypass_ai.yaml` is the only one that runs without an API key.

## Run the test suite

```bash
# offline — no API key, no spend, no network
uv run python examples/ai_transform_tests.py

# include the ELT tests
docker run -d --name loafer-ai-test \
  -e POSTGRES_USER=loafer -e POSTGRES_PASSWORD=loafer -e POSTGRES_DB=loafer_test \
  -p 55432:5432 postgres:16-alpine
export LOAFER_PG_URL=postgresql://loafer:loafer@localhost:55432/loafer_test
uv run python examples/ai_transform_tests.py --with-postgres

# against the real model declared in each pipeline (Anthropic)
export ANTHROPIC_API_KEY=sk-ant-...
uv run python examples/ai_transform_tests.py --live

# against a different provider, without editing the pipelines
export OPENAI_API_KEY=sk-proj-...
uv run python examples/ai_transform_tests.py --live --provider openai
uv run python examples/ai_transform_tests.py --live --provider openai --model gpt-5.4-mini

uv run python examples/ai_transform_tests.py -k safety     # filter
uv run python examples/ai_transform_tests.py -v            # tracebacks
```

`--provider` rewrites each pipeline's `llm:` block after `load_config` and
clears `api_key`, so Loafer's own `_build_llm_provider` — including its
environment-variable key lookup — stays the code under test. `--model` defaults
to that provider's `default_model_for()`. Both require `--live`.

Many tests are offline-only by nature — they pin retry counts, force blocked
imports, or feed deliberately broken SQL, none of which a real model can be made
to produce on demand. Those report `SKIP` under `--live` with the reason.

### Why offline is the default

The provider is the only part of an AI transform that needs the network. It
returns Python (ETL) or SQL (ELT) as text; everything after that — prompt
construction, AST validation, the sandboxed subprocess, the retry loop,
destructive-operation detection, the loaders — is deterministic Loafer code,
and that is where the defects are. `ScriptedProvider` replays fixed model
output and records every call, so tests can assert on what Loafer fed back into
the model on retry. `--live` swaps in the real provider and runs the same
assertions.

### Reading the results

| Status | Meaning |
| --- | --- |
| `PASS` | behaved correctly |
| `FAIL` | a regression — fails the run (exit 1) |
| `XFAIL` | a known defect listed in `KNOWN_DEFECTS`; does not fail the run |
| `XPASS` | a known defect appears fixed — **delete its marker** so the test guards the fix (exit 1) |
| `SKIP` | prerequisite missing (e.g. no Postgres) |

Known defects are catalogued in `KNOWN_DEFECTS` at the top of
`ai_transform_tests.py`, each with the mechanism and the affected code path.
When you fix one, the test flips to `XPASS` and tells you to remove the marker.

## Adding a test

```python
@test("safety")
def my_check() -> str:
    with scripted(scripted_llm.NORMALIZE_ORDERS) as provider:
        state = run_pipeline(PIPELINES / "01_ai_basic.yaml", yes=True)
    expect(state["rows_loaded"] == 9, f"rows_loaded={state['rows_loaded']}")
    return "9 rows loaded"  # one-line summary shown in the report
```

Pass one canned response per expected model call; a call beyond the script
raises `ScriptExhausted`, which is how the suite pins retry counts.
