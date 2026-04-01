# Loafer

**AI-assisted ETL/ELT pipelines from the command line.**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-232%20passing-brightgreen.svg)](https://github.com/lupppig/loafer)
[![Ruff](https://img.shields.io/badge/style-ruff-darkgreen.svg)](https://docs.astral.sh/ruff/)

---

## What Problem Does This Solve?

Loafer extracts data from databases, files, and APIs, transforms it according to your instructions, and loads it into your target system — all from a single YAML config file.

Describe **what** you want in plain English and Loafer generates the transformation code. Or write your own Python or SQL. Either way, you get:

- **Live progress bars** — see each stage (extract → validate → transform → load) with row counts and timing
- **Streaming by default** — no full dataset ever sits in memory
- **Three transform modes** — AI-generated, hand-written Python, or SQL
- **Cron scheduling** — run pipelines on a schedule with persistent job storage

### Example

```yaml
# pipeline.yaml
name: Daily Sales Report
source:
  type: postgres
  url: ${DATABASE_URL}
  query: "SELECT * FROM sales WHERE created_at >= NOW() - INTERVAL '1 day'"
target:
  type: csv
  path: ./output/sales_report.csv
transform:
  type: ai
  instruction: >
    combine first_name and last_name into full_name,
    convert price to float rounded to 2 decimals,
    drop rows where status is 'cancelled'
mode: etl
```

```bash
uv run loafer run pipeline.yaml
```

```
Running: Daily Sales Report [ETL]
────────────────────────────────────────────────────────────────────────────────
  ✓  Extracting from POSTGRES            14,523 rows
  ✓  Validating data                     14,523 passed
  ✓  Transforming data (ai)              14,523 → 12,891
  ✓  Loading to CSV                      12,891 rows

─────────────────────────────── Pipeline Summary ───────────────────────────────
 Stage               Status    Rows          Duration
 Extracting from     ✓         14,523 rows      2.1s
 source
 Validating data     ✓         14,523 passed    0.3s
 Transforming data   ✓         14,523 → 12,891  0.8s
 Loading to target   ✓         12,891 rows      1.4s

Total: 4.6s
```

---

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐
│   Source    │───▶│   Extract    │───▶│   Validate   │───▶│ Transform│
│  Connectors │    │   Agent      │    │   Agent      │    │  Agent   │
└─────────────┘    └──────────────┘    └──────────────┘    └──────────┘
                                                               │
                      ┌────────────────────────────────────────┘
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
│   ├── gemini.py                # Google Gemini implementation (google-genai SDK)
│   ├── schema.py                # Schema sampler (type inference)
│   └── prompt_builder.py        # Structured prompts for code/SQL generation
│
├── graph/                       # LangGraph state and pipeline graphs
│   ├── state.py                 # PipelineState TypedDict
│   ├── etl.py                   # ETL pipeline graph (LangGraph StateGraph)
│   └── elt.py                   # ELT pipeline graph (LangGraph StateGraph)
│
├── runner.py                    # Composition root — config → state → graph execution
├── scheduler.py                 # APScheduler-based cron scheduling with SQLite persistence
├── daemon.py                    # Background daemon management (PID file, log tailing)
├── config.py                    # Pydantic v2 config models + YAML parsing
├── exceptions.py                # Domain error hierarchy
└── cli.py                       # Typer CLI — run, validate, schedule, init, daemon

tests/
├── unit/
│   ├── agents/                  # Agent tests (37 tests)
│   ├── connectors/              # Connector tests (72 tests)
│   └── ...                      # LLM, config, validator, scheduler, daemon tests
└── manual_test.py               # End-to-end manual verification script
```

---

## Quick Start

### 1. Install

```bash
git clone https://github.com/lupppig/loafer.git
cd loafer
uv sync
```

### 2. Run the built-in example

No external services needed — this uses local CSV and JSON files:

```bash
uv run loafer run examples/pipeline.quickstart.yaml
```

### 3. Scaffold your own project

```bash
uv run loafer init my-etl
```

Interactive prompts guide you through choosing source, target, and transform types. Creates a ready-to-edit `pipeline.yaml`, `transform.py`, and sample data.

### 4. Schedule it

```bash
uv run loafer schedule my-etl/pipeline.yaml --cron "0 9 * * *"
uv run loafer start -d
```

---

## Configuration Reference

Every pipeline is defined in a single YAML file. All fields are validated at parse time by Pydantic v2.

### Top-Level Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | `""` | Human-readable pipeline name, shown in CLI output and scheduler |
| `source` | object | *(required)* | Source connector configuration (see below) |
| `target` | object | *(required)* | Target connector configuration (see below) |
| `transform` | object / string | *(required)* | Transform config — can be a shorthand string for AI mode (e.g. `transform: "rename x to y"`) |
| `mode` | `"etl"` \| `"elt"` | `"etl"` | ETL: transform before loading; ELT: load raw then transform in-target via SQL |
| `chunk_size` | int | `500` | Number of rows per batch for processing and loading |
| `streaming_threshold` | int | `10_000` | Row count above which streaming mode activates (data flows through generators instead of being buffered) |
| `destructive_filter_threshold` | float | `0.3` | Warn if transform drops more than this fraction of rows (0.3 = 30%) |
| `validation` | object | | Data quality validation settings (see below) |
| `llm` | object | | LLM provider configuration (see below) |

### Source Configurations

#### CSV (`type: csv`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | *(required)* | Path to the CSV file |
| `has_header` | bool | `true` | Whether the first row is a header |
| `encoding` | string | `"utf-8"` | File encoding (falls back to latin-1 on UTF-8 decode error) |
| `column_names` | list[string] | `null` | Required if `has_header: false` — column names to use |

#### Excel (`type: excel`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | *(required)* | Path to the `.xlsx` file |
| `sheet` | string | `null` | Sheet name (defaults to first sheet) |

#### PostgreSQL (`type: postgres`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | *(required)* | PostgreSQL connection URL (e.g. `postgresql://user:pass@host/db`) |
| `query` | string | *(required)* | SQL SELECT query to execute |
| `timeout` | int | `30` | Connection timeout in seconds |

#### MySQL (`type: mysql`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | *(required)* | MySQL connection URL (e.g. `mysql://user:pass@host/db`) |
| `query` | string | *(required)* | SQL SELECT query to execute |
| `timeout` | int | `30` | Connection timeout in seconds |

#### MongoDB (`type: mongo`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | *(required)* | MongoDB connection URL (e.g. `mongodb://user:pass@host/db`) |
| `database` | string | *(required)* | Database name |
| `collection` | string | *(required)* | Collection name |
| `filter` | object | `{}` | MongoDB query filter |

#### REST API (`type: rest_api`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | *(required)* | API endpoint URL |
| `method` | `"GET"` \| `"POST"` | `"GET"` | HTTP method |
| `headers` | object | `{}` | HTTP headers |
| `params` | object | `{}` | Query parameters |
| `body` | object | `null` | Request body (for POST) |
| `response_key` | string | `null` | JSON key to extract array from (e.g. `"results"`) |
| `pagination` | object | `null` | Pagination config (offset/limit or cursor-based) |
| `auth_token` | string | `null` | Bearer token for authorization |
| `verify_ssl` | bool | `true` | SSL certificate verification |
| `timeout` | int | `30` | Request timeout in seconds |

### Target Configurations

#### CSV (`type: csv`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | *(required)* | Output CSV file path |
| `write_mode` | `"overwrite"` \| `"error"` | `"overwrite"` | Whether to overwrite existing file or error |

#### JSON (`type: json`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | *(required)* | Output JSON file path (writes a JSON array) |
| `write_mode` | `"overwrite"` \| `"error"` | `"overwrite"` | Whether to overwrite existing file or error |

#### PostgreSQL (`type: postgres`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | *(required)* | PostgreSQL connection URL |
| `table` | string | *(required)* | Target table name (auto-created if it doesn't exist) |
| `write_mode` | `"append"` \| `"replace"` \| `"error"` | `"append"` | Whether to append, replace, or error if table exists |

### Transform Configurations

#### AI (`type: ai`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `instruction` | string | *(required)* | Natural language description of the transformation |

The LLM generates a Python `transform(data) -> list[dict]` function. Code is AST-validated for safety before execution. On failure, the traceback is fed back to the LLM for up to 3 retry attempts.

#### Custom (`type: custom`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | *(required)* | Path to a `.py` file containing a `transform(data)` function |

No LLM call. The file is loaded, validated, and executed directly.

#### SQL (`type: sql`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `query` | string | *(required)* | SQL query with `{{source}}` placeholder for the source table |

Validated via sqlglot — only SELECT allowed. Transpiled to the target dialect.

### Validation

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_null_rate` | float | `0.5` | Maximum fraction of null values allowed per column before hard failure (0.5 = 50%) |
| `strict` | bool | `false` | If true, any validation failure stops the pipeline; if false, soft warnings are logged but execution continues |

### LLM

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `provider` | `"gemini"` \| `"claude"` \| `"openai"` \| `"qwen"` | `"gemini"` | LLM provider for AI transforms and ELT SQL generation |
| `model` | string | `"gemini-2.5-flash"` | Model name (provider-specific) |
| `api_key` | string | `""` | API key — supports `${ENV_VAR}` syntax for environment variable interpolation |

Example with environment variable:

```yaml
llm:
  provider: gemini
  model: gemini-2.5-flash
  api_key: ${GOOGLE_API_KEY}
```

#### Setting up LLM providers

**Google Gemini (default)**

1. Get an API key from [Google AI Studio](https://aistudio.google.com/apikey)
2. Set the environment variable:
   ```bash
   export GOOGLE_API_KEY="your-api-key-here"
   ```
3. Configure your pipeline:
   ```yaml
   llm:
     provider: gemini
     model: gemini-2.5-flash
     api_key: ${GOOGLE_API_KEY}
   ```

**OpenAI**

1. Get an API key from [OpenAI Platform](https://platform.openai.com/api-keys)
2. Set the environment variable:
   ```bash
   export OPENAI_API_KEY="your-api-key-here"
   ```
3. Configure your pipeline:
   ```yaml
   llm:
     provider: openai
     model: gpt-4o-mini
     api_key: ${OPENAI_API_KEY}
   ```

**Anthropic Claude**

1. Get an API key from [Anthropic Console](https://console.anthropic.com/)
2. Set the environment variable:
   ```bash
   export ANTHROPIC_API_KEY="your-api-key-here"
   ```
3. Configure your pipeline:
   ```yaml
   llm:
     provider: claude
     model: claude-sonnet-4-20250514
     api_key: ${ANTHROPIC_API_KEY}
   ```

**Qwen**

1. Get an API key from [DashScope](https://dashscope.console.aliyun.com/)
2. Set the environment variable:
   ```bash
   export DASHSCOPE_API_KEY="your-api-key-here"
   ```
3. Configure your pipeline:
   ```yaml
   llm:
     provider: qwen
     model: qwen-plus
     api_key: ${DASHSCOPE_API_KEY}
   ```

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

# Start in the foreground
uv run loafer start

# Start in the background (daemon mode)
uv run loafer start -d

# Check status
uv run loafer status

# Stop the background scheduler
uv run loafer stop

# View logs
uv run loafer logs
```

### Cron expressions

Cron uses standard 5-field syntax: `minute hour day month day_of_week`

| Expression | Description | Example |
|------------|-------------|---------|
| `0 9 * * *` | Daily at 9am UTC | `--cron "0 9 * * *"` |
| `0 */2 * * *` | Every 2 hours | `--cron "0 */2 * * *"` |
| `0 9 * * 1-5` | Weekdays at 9am | `--cron "0 9 * * 1-5"` |
| `0 0 1 * *` | First of every month | `--cron "0 0 1 * *"` |
| `*/15 * * * *` | Every 15 minutes | `--cron "*/15 * * * *"` |

### Interval strings

| Format | Description | Example |
|--------|-------------|---------|
| `30m` | Every 30 minutes | `--interval "30m"` |
| `1h` | Every hour | `--interval "1h"` |
| `6h` | Every 6 hours | `--interval "6h"` |
| `1d` | Every day | `--interval "1d"` |

### How scheduling works

1. **`loafer schedule`** — Creates a job entry in the SQLite store (`loafer_jobs.sqlite`)
2. **`loafer start`** — Starts the scheduler in foreground (blocks until Ctrl+C)
3. **`loafer start -d`** — Starts the scheduler as a background daemon (PID file at `~/.loafer/scheduler.pid`)
4. Jobs persist across restarts — stop and start the scheduler without losing schedules

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
3. Import and use in the appropriate LangGraph graph
4. Write tests against `conftest.py` fixtures

---

## Planned Features

- [ ] Additional LLM providers (Claude, OpenAI)
- [ ] Additional connectors (S3, BigQuery, Snowflake, Redshift)

---

## License

MIT
