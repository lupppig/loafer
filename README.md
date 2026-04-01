# Loafer

**AI-assisted ETL/ELT pipelines from the command line.**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-200%20passing-brightgreen.svg)](https://github.com/lupppig/loafer)
[![Ruff](https://img.shields.io/badge/style-ruff-darkgreen.svg)](https://docs.astral.sh/ruff/)

---

## What Problem Does This Solve?

Traditional ETL tools require you to write transformation logic by hand. Loafer lets you describe **what** you want in plain English and uses an LLM to generate the transformation code for you.

Instead of writing:
```python
df["full_name"] = df["first_name"] + " " + df["last_name"]
df["age"] = (pd.Timestamp.now() - pd.to_datetime(df["dob"])).dt.days // 365
```

You write:
```yaml
transform:
  type: ai
  instruction: "combine first_name and last_name into full_name, calculate age from dob"
```

Loafer generates, validates, and executes the code — with automatic retry on failure.

---

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐
│   Source    │───▶│   Extract    │───▶│   Validate   │───▶│ Transform│
│  Connectors │    │   Agent      │    │   Agent      │    │  Agent   │
└─────────────┘    └──────────────┘    └──────────────┘    └──────────┘
                                                               │
                    ┌──────────────────────────────────────────┘
                    ▼
              ┌──────────────┐    ┌──────────┐
              │    Load      │◀───│  Runner   │
              │   Agent      │    │ (LangGraph│
              └──────────────┘    └──────────┘
```

### Design Principles

- **Streaming-first** — Data flows through generators in chunks. Full datasets are never buffered in memory.
- **Config-driven** — Pipelines are defined in YAML and validated by Pydantic v2.
- **AI-assisted, not AI-dependent** — Three transform modes: AI (LLM-generated), custom (user `.py` file), and SQL.
- **Safety by default** — Generated code is AST-validated for dangerous imports before execution. SQL is parsed and validated via sqlglot.
- **Pure functions** — Agents are pure functions over `PipelineState`. No agent imports from another agent.
- **Typed** — Strict type annotations throughout, checked with mypy.

---

## Project Structure

```
loafer/
├── agents/                      # Pure functions — pipeline stages
│   ├── extract.py               # Resolve connector, stream/read, build schema
│   ├── validate.py              # Null rates, consistency, hard/soft failures
│   ├── transform.py             # Route to AI/custom/SQL runner
│   ├── load.py                  # Resolve target, write chunks, finalize
│   └── transform_in_target.py   # ELT-only: LLM SQL → CREATE TABLE AS SELECT
│
├── connectors/                  # Data source/target adapters
│   ├── base.py                  # SourceConnector / TargetConnector ABCs
│   ├── registry.py              # Single source of truth: type → connector
│   ├── sources/
│   │   ├── csv_source.py        # Streaming CSV reader
│   │   └── excel_source.py      # Streaming Excel reader (openpyxl)
│   └── targets/
│       ├── csv_target.py        # Streaming CSV writer
│       └── json_target.py       # Streaming JSON array writer
│
├── transform/                   # Transform execution engines
│   ├── __init__.py              # TransformRunner ABC
│   ├── ai_runner.py             # LLM code gen → validate → exec with retry
│   ├── custom_runner.py         # User .py file → validate → exec
│   └── sql_runner.py            # SQL validate → transpile → execute
│
├── llm/                         # LLM provider layer
│   ├── base.py                  # LLMProvider ABC
│   ├── gemini.py                # Google Gemini implementation
│   ├── schema.py                # Schema sampler (type inference)
│   └── prompt_builder.py        # Structured prompts for code/SQL generation
│
├── graph/                       # LangGraph state and pipeline graphs
│   ├── state.py                 # PipelineState TypedDict
│   ├── etl.py                   # ETL pipeline graph (TODO)
│   └── elt.py                   # ELT pipeline graph (TODO)
│
├── runner.py                    # Composition root — wire adapters into graph (TODO)
├── config.py                    # Pydantic v2 config models + YAML parsing
├── exceptions.py                # Domain error hierarchy
└── cli.py                       # Typer CLI entrypoint (scaffold)

tests/
├── unit/
│   ├── agents/                  # Agent tests (37 tests)
│   ├── connectors/              # Connector tests (72 tests)
│   └── ...                      # LLM, config, validator tests
└── manual_test.py               # End-to-end manual verification script
```

---

## Current Status

| Phase | Component | Status |
|-------|-----------|--------|
| **0** | Project scaffold, config, state, exceptions | ✅ Complete |
| **1** | LLM layer (Gemini, schema sampler, prompt builder, validators) | ✅ Complete |
| **2** | Connectors (6 sources, 3 targets, registry) | ✅ Complete |
| **3** | Agents + transform runners (extract, validate, transform, load, transform-in-target) | ✅ Complete |
| **4** | LangGraph graphs (ETL/ELT), runner | ✅ Complete |
| **5** | CLI implementation (`run`, `validate`, `connectors`) | ✅ Complete |
| **6** | Scheduler (cron/interval-based scheduling) | ✅ Complete |

**217 tests passing · 3,300+ lines of source · ruff clean**

---

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Install

```bash
git clone https://github.com/lupppig/loafer.git
cd loafer
uv sync
```

### Manual Test

Run the end-to-end test to verify all agents work together:

```bash
uv run python tests/manual_test.py
```

This runs a full ETL pipeline: CSV → extract → validate → transform (custom Python) → JSON output.

---

## Supported Connectors

### Sources

| Connector | Type | Streaming | Notes |
|-----------|------|-----------|-------|
| CSV | `csv` | ✅ | UTF-8 with latin-1 fallback, malformed row skipping |
| Excel | `excel` | ✅ | Formula values, unmerge cells, mixed type coercion |
| PostgreSQL | `postgres` | ✅ | Server-side cursor, type conversion (Decimal→float, UUID→string, datetime→ISO 8601) |
| MySQL | `mysql` | ✅ | fetchmany-based streaming, type conversion |
| MongoDB | `mongo` | ✅ | ObjectId→string, nested docs passthrough |
| REST API | `rest_api` | ✅ | Pagination, rate limit retry, Bearer auth |

### Targets

| Connector | Type | Streaming | Notes |
|-----------|------|-----------|-------|
| CSV | `csv` | ✅ | Auto-create directories, None→empty string |
| JSON | `json` | ✅ | Streaming JSON array write |
| PostgreSQL | `postgres` | ✅ | Auto-create table, schema inference, batch rollback |

---

## Transform Modes

### AI Mode (LLM-Generated)

Describe your transformation in natural language. The LLM generates Python code, validates it for safety, and executes it with automatic retry on failure.

```yaml
transform:
  type: ai
  instruction: "rename 'full_name' to 'name', convert 'price' to float, drop rows where 'status' is 'inactive'"
```

- **Retry loop**: Up to 3 attempts. On failure, the full traceback is fed back to the LLM.
- **Safety check**: AST-based validation blocks `import os`, `import subprocess`, `eval()`, `exec()`, and other dangerous patterns.
- **Token tracking**: All token usage across retries is accumulated in state.

### Custom Mode (User-Provided Python)

Write your own `.py` file with a `transform(data) -> list[dict]` function.

```yaml
transform:
  type: custom
  path: ./my_transform.py
```

```python
# my_transform.py
def transform(data):
    return [
        {**row, "total": row["price"] * row["quantity"]}
        for row in data
        if row["status"] == "active"
    ]
```

No LLM call. No retry. One attempt. Validation failures are caught immediately.

### SQL Mode

Write SQL with `{{source}}` placeholder. Validated via sqlglot AST analysis, transpiled to the target dialect.

```yaml
transform:
  type: sql
  query: "SELECT id, name, price * quantity AS total FROM {{source}} WHERE status = 'active'"
```

- **Safety**: Only SELECT allowed. DROP/DELETE/UPDATE/INSERT/TRUNCATE/ALTER/CREATE rejected before any DB connection.
- **Table names**: Substituted via `psycopg2.sql.Identifier`, never raw string interpolation.
- **Transpilation**: `sqlglot.transpile()` converts to the correct target dialect.

---

## Scheduling

Schedule pipelines to run on a cron or interval basis. Jobs are persisted in SQLite so they survive restarts.

### Schedule a pipeline

```bash
# Run daily at 9am UTC
uv run loafer schedule pipeline.yaml --cron "0 9 * * *"

# Run every 30 minutes
uv run loafer schedule pipeline.yaml --interval "30m"

# Run every 2 hours with a custom job ID
uv run loafer schedule pipeline.yaml --interval "2h" --id my-etl-job
```

### Manage schedules

```bash
# List all scheduled jobs
uv run loafer list-schedules

# Remove a scheduled job
uv run loafer unschedule my-etl-job
```

### Start the scheduler

```bash
# Start in the foreground (Ctrl+C to stop)
uv run loafer start
```

---

## For Contributors

### Development Setup

```bash
uv sync --all-extras
```

### Running Tests

```bash
# All unit tests
uv run pytest tests/unit/ -v

# Specific module
uv run pytest tests/unit/agents/ -v

# With coverage
uv run pytest tests/unit/ --cov=loafer --cov-report=term-missing
```

### Linting & Type Checking

```bash
uv run ruff check loafer/ tests/
uv run mypy loafer/ --ignore-missing-imports
```

### Code Conventions

- **Type annotations** on every function signature and return type
- **No comments** unless explaining non-obvious behavior
- **Docstrings** for modules and public classes only
- **Pure functions** for agents — no side effects, no connector instantiation
- **Streaming** — generators over lists for data flow
- **Error handling** — domain-specific exceptions, never bare `except:`

### Adding a New Connector

1. Implement the connector class in `loafer/connectors/sources/` or `targets/`
2. Add a wrapper class in `loafer/connectors/registry.py`
3. Register it: `_register_source("my_type", MyConnector)` or `_register_target(...)`
4. Add `_build_source` / `_build_target` case in `registry.py`
5. Write tests in `tests/unit/connectors/`

### Adding a New Agent

1. Create `loafer/agents/my_agent.py`
2. Define a function `my_agent(state: PipelineState) -> PipelineState`
3. Import and use in the appropriate LangGraph graph (Phase 4)
4. Write tests against `conftest.py` fixtures

---

## Planned Features

- [ ] Additional LLM providers (Claude, OpenAI)
- [ ] Additional connectors (S3, BigQuery, Snowflake, Redshift)
- [ ] Rich output — pipeline progress bars, error tables, timing summaries
- [ ] CLI `init` command for scaffolding new projects

---

## License

MIT
