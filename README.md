# Loafer

**AI-assisted ETL/ELT pipelines from the command line.**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-486%20passing-brightgreen.svg)](https://github.com/lupppig/loafer)
[![Ruff](https://img.shields.io/badge/style-ruff-darkgreen.svg)](https://docs.astral.sh/ruff/)

---

## What Problem Does This Solve?

Loafer extracts data from databases, files, and APIs, transforms it according to your instructions, and loads it into your target system — all from a single YAML config file.

Describe **what** you want in plain English and Loafer generates the transformation code. Or write your own Python or SQL. Either way, you get:

- **Animated terminal spinners** — see each stage (extract → validate → transform → load) with live progress, row counts, and timing
- **Streaming by default** — no full dataset ever sits in memory
- **Three transform modes** — AI-generated, hand-written Python, or SQL
- **ETL and ELT** — transform before loading, or load raw then transform in-database
- **Cron scheduling** — run pipelines on a schedule with persistent job storage
- **Four LLM providers** — Gemini, Claude, OpenAI, Qwen

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
# With uv:
uv run loafer run pipeline.yaml

# With pip (after `pip install -e .`):
loafer run pipeline.yaml
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

Loafer follows a **Ports and Adapters** (Hexagonal) architecture. The core pipeline logic (agents, graph, runner) knows nothing about external systems. Connectors and LLM providers are pluggable adapters.

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Core (Ports)                               │
│                                                                     │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐         │
│  │ Extract  │──▶│ Validate │──▶│Transform │──▶│  Load    │         │
│  │  Agent   │   │  Agent   │   │  Agent   │   │  Agent   │         │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘         │
│       ▲                                              │              │
│       │              LangGraph StateGraph             │              │
│       └──────────────────────────────────────────────┘              │
│                                                                     │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
           ┌────────────────┼────────────────┐
           │                │                │
    ┌──────▼──────┐  ┌─────▼─────┐  ┌──────▼──────┐
    │  Adapters   │  │  Adapters │  │  Adapters   │
    │  (Sources)  │  │ (Targets) │  │    (LLM)    │
    │             │  │           │  │             │
    │ CSV         │  │ CSV       │  │ Gemini      │
    │ Excel       │  │ JSON      │  │ Claude      │
    │ PostgreSQL  │  │ PostgreSQL│  │ OpenAI      │
    │ MySQL       │  │ MongoDB   │  │ Qwen        │
    │ MongoDB     │  │           │  │             │
    │ REST API    │  │           │  │             │
    │ SQLite      │  │           │  │             │
    │ PDF         │  │           │  │             │
    └─────────────┘  └───────────┘  └─────────────┘
```

### Design Principles

- **Ports and Adapters** — Core agents import nothing from adapters. New connectors are added without touching pipeline logic.
- **Streaming-first** — Data flows through generators in chunks. Full datasets are never buffered in memory.
- **Config-driven** — Pipelines are defined in YAML and validated by Pydantic v2.
- **AI-assisted, not AI-dependent** — Three transform modes: AI (LLM-generated), custom (user `.py` file), and SQL.
- **Safety by default** — Generated code is AST-validated for dangerous imports before execution. SQL is parsed and validated via sqlglot.
- **Pure functions** — Agents are pure functions over `PipelineState`. No agent imports from another agent.

---

## For Users — End-to-End Guide

This section walks you through running Loafer from scratch. No prior knowledge needed.

### Prerequisites

- **Python 3.11 or later** — check with `python3 --version`

### Step 1: Clone and Install

**Option A: Using uv (recommended — fastest)**

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install
git clone https://github.com/lupppig/loafer.git
cd loafer
uv sync
```

**Option B: Using pip (no extra tools needed)**

```bash
# Clone
git clone https://github.com/lupppig/loafer.git
cd loafer

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate

# Install
pip install -e .
```

After installing with **Option A**, run commands with `uv run loafer ...`.
After installing with **Option B**, run commands with just `loafer ...`.

This guide uses `uv run loafer` by default. If you used pip, replace every `uv run loafer` with just `loafer`.

### Step 2: Set Up an LLM API Key

Loafer needs an LLM to generate transform code in AI mode. You can configure it **two ways**: via environment variable (quick), or inside your pipeline YAML (portable). Pick one provider:

| Provider | Env Variable | Default Model | Get Key |
|----------|-------------|---------------|---------|
| **Gemini** (default) | `GEMINI_API_KEY` | `gemini-2.5-flash` | [Google AI Studio](https://aistudio.google.com/apikey) |
| **Claude** | `ANTHROPIC_API_KEY` | `claude-sonnet-4-20250514` | [Anthropic Console](https://console.anthropic.com/) |
| **OpenAI** | `OPENAI_API_KEY` | `gpt-4o-mini` | [OpenAI Platform](https://platform.openai.com/api-keys) |
| **Qwen** | `DASHSCOPE_API_KEY` | `qwen-plus` | [DashScope](https://dashscope.console.aliyun.com/) |

#### Method A: Environment variable (quickest)

Export the key in your shell before running any pipeline:

```bash
export GEMINI_API_KEY="your-key-here"
```

That's it. Loafer picks it up automatically. No YAML changes needed.

#### Method B: Inside your pipeline YAML (recommended for projects)

Add an `llm` block to your `pipeline.yaml`:

```yaml
name: My First Pipeline

source:
  type: csv
  path: ./data.csv

target:
  type: json
  path: ./output.json

transform:
  type: ai
  instruction: "filter active rows, add grade column (A if score >= 90, else B)"

mode: etl

# ── LLM configuration ──
llm:
  provider: gemini          # or: claude, openai, qwen
  model: gemini-2.5-flash   # optional — defaults to provider's recommended model
  api_key: ${GEMINI_API_KEY}  # or paste the key directly (not recommended)
```

You can still use `${ENV_VAR}` syntax inside the YAML so the key never sits in plain text:

```yaml
llm:
  provider: claude
  api_key: ${ANTHROPIC_API_KEY}
```

#### Switching providers

Change the `provider` field — no other changes needed:

```yaml
llm:
  provider: openai
  model: gpt-4o
  api_key: ${OPENAI_API_KEY}
```

```yaml
llm:
  provider: qwen
  model: qwen-max
  api_key: ${DASHSCOPE_API_KEY}
```

#### No LLM? No problem.

If you use `transform.type: custom` (your own Python file) or `transform.type: sql`, no LLM call is made. You don't need any API key:

```yaml
transform:
  type: custom
  path: ./transform.py
# No llm block needed
```

You can also use AI mode with `bypass_ai: true` to skip the LLM call while still running a custom transform:

```yaml
transform:
  type: ai
  custom_path: ./transform.py
  bypass_ai: true                    # skips LLM, only runs custom
```

### Step 3: Run Your First Pipeline

Create a working directory:

```bash
mkdir ~/loafer-demo && cd ~/loafer-demo
```

**Create sample data** (`data.csv`):

```csv
id,name,email,score,status
1,Alice,alice@example.com,95.5,active
2,Bob,bob@example.com,88.0,inactive
3,Charlie,charlie@example.com,72.3,active
4,Diana,diana@example.com,91.0,active
5,Eve,eve@example.com,65.0,inactive
```

**Create the pipeline config** (`pipeline.yaml`):

```yaml
name: My First Pipeline

source:
  type: csv
  path: ./data.csv

target:
  type: json
  path: ./output.json

transform:
  type: ai
  instruction: "filter active rows, add grade column (A if score >= 90, else B)"

mode: etl
```

**Run it:**

```bash
uv run loafer run ~/loafer-demo/pipeline.yaml
```

You'll see a live progress bar for each stage. When it finishes:

```bash
cat ~/loafer-demo/output.json
```

Output should contain only Alice and Diana (the active rows), each with a `"grade": "A"` field.

### Step 4: Try a Database Source

If you have a running PostgreSQL:

```yaml
name: Postgres to CSV
source:
  type: postgres
  url: ${DATABASE_URL}
  query: "SELECT id, name, email, created_at FROM users"

target:
  type: csv
  path: ./users.csv

transform:
  type: ai
  instruction: "lowercase all email addresses, rename 'name' to 'full_name'"

mode: etl
```

```bash
export DATABASE_URL="postgresql://user:pass@localhost/mydb"
uv run loafer run ~/loafer-demo/db_pipeline.yaml
```

### Step 5: Try ELT Mode (Load Raw, Then Transform In-Database)

ELT loads raw data into the target database first, then generates SQL to create the output table. This requires a **PostgreSQL target**:

```yaml
name: ELT Pipeline
source:
  type: csv
  path: ./data.csv

target:
  type: postgres
  url: ${DATABASE_URL}
  table: processed_users
  write_mode: replace

transform:
  type: ai
  instruction: "select only active rows, add a grade column"

mode: elt
```

```bash
uv run loafer run ~/loafer-demo/elt_pipeline.yaml
```

This creates two tables in your database:
- `loafer_raw_postgres_<random>` — the raw staging table
- `processed_users` — the final transformed table

### Step 6: Use Custom Python Transforms (No LLM Needed)

If you prefer to write your own Python:

```yaml
name: Custom Transform Pipeline
source:
  type: csv
  path: ./data.csv
target:
  type: json
  path: ./output.json
transform:
  type: custom
  path: ./transform.py
mode: etl
```

Create `transform.py`:

```python
def transform(data):
    """Filter active rows and add a grade column."""
    return [
        {**row, "grade": "A" if float(row["score"]) >= 90 else "B"}
        for row in data
        if row["status"] == "active"
    ]
```

```bash
uv run loafer run ~/loafer-demo/custom_pipeline.yaml
```

No LLM call. No API key needed. Instant execution.

### Step 6b: Combine Custom + AI Transforms

You can run **both** custom and AI transforms in the same pipeline. The AI is shown your custom code so it doesn't duplicate or override it.

```yaml
name: Combined Transform Pipeline
source:
  type: csv
  path: ./data.csv
target:
  type: json
  path: ./output.json
transform:
  type: ai
  instruction: "clean null values, normalize phone numbers"
  custom_path: ./my_transform.py      # your custom logic
  custom_order: custom_first           # run custom first, then AI (default)
  review: true                         # show AI code and ask for confirmation
mode: etl
```

Data flows through both transforms:

| `custom_order` | Flow |
|---|---|
| `custom_first` (default) | raw data → custom transform → AI transform → output |
| `ai_first` | raw data → AI transform → custom transform → output |

**Bypass AI entirely** — set `bypass_ai: true` to skip the LLM call and only run your custom transform (no API key needed):

```yaml
transform:
  type: ai
  instruction: "this is ignored when bypass_ai is true"
  custom_path: ./my_transform.py
  bypass_ai: true                    # skip AI, only run custom
```

**Human review** — set `review: true` to see the AI-generated code with syntax highlighting before it executes:

```
⚠ Human Review Required
AI-generated transform code is ready for review.
Review the code below. If it looks correct, type 'y' to execute.

  1  def transform(data):
  2      return [
  3          {**row, "phone": re.sub(r'\D', '', row.get('phone', ''))}
  4          for row in data
  5          if row.get('phone')
  6      ]

Execute this code? [y/N]:
```

### Step 7: Schedule a Pipeline

```bash
# Run daily at 9am
uv run loafer schedule ~/loafer-demo/pipeline.yaml --cron "0 9 * * *"

# Start the scheduler in the background
uv run loafer start -d

# Check what's scheduled
uv run loafer list-schedules

# View scheduler status
uv run loafer status

# Stop the scheduler
uv run loafer stop
```

Jobs are persisted in SQLite (`~/.loafer/loafer_jobs.sqlite`) and survive restarts.

### Common Commands Reference

> **Note**: If you installed with pip (`pip install -e .`), replace `uv run loafer` with just `loafer` in every command below.

| Command | What it does |
|---------|-------------|
| `uv run loafer run pipeline.yaml` | Run a pipeline |
| `uv run loafer run pipeline.yaml --dry-run` | Test without loading |
| `uv run loafer run pipeline.yaml --verbose` | Print full traceback on errors |
| `uv run loafer validate pipeline.yaml` | Check config validity |
| `uv run loafer connectors` | List all available connectors |
| `uv run loafer init my-project` | Scaffold a new project interactively |
| `uv run loafer schedule pipeline.yaml --cron "0 9 * * *"` | Schedule daily at 9am |
| `uv run loafer schedule pipeline.yaml --interval "30m"` | Run every 30 minutes |
| `uv run loafer start -d` | Start scheduler in background |
| `uv run loafer status` | Check scheduler status |
| `uv run loafer list-schedules` | List all scheduled jobs |
| `uv run loafer unschedule <job-id>` | Remove a scheduled job |
| `uv run loafer stop` | Stop the background scheduler |
| `uv run loafer logs` | View scheduler logs |

---

## For Developers — Contributing Guide

### Development Setup

```bash
git clone https://github.com/lupppig/loafer.git
cd loafer

# With uv (recommended):
uv sync --all-extras

# With pip:
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### Running Tests

```bash
# All unit tests
uv run pytest tests/unit/ -v

# Specific module
uv run pytest tests/unit/agents/ -v

# With coverage
uv run pytest tests/unit/ --cov=loafer --cov-report=term-missing

# Integration tests (requires running Postgres and MongoDB)
uv run pytest tests/integration/ -v -m integration

# E2E tests
uv run pytest tests/e2e/ -v -m e2e

# Benchmark tests (gated behind --benchmark flag)
uv run pytest tests/unit/ --benchmark -v
```

### Linting & Formatting

```bash
# Check for lint errors
uv run ruff check .

# Auto-fix what's fixable
uv run ruff check . --fix

# Check formatting
uv run ruff format --check .

# Auto-format
uv run ruff format .
```

### Project Structure

```
loafer/
├── ports/                          # Abstract interfaces
│   ├── connector.py                # SourceConnector / TargetConnector ABCs
│   └── llm.py                      # LLMProvider ABC
│
├── adapters/                       # Concrete implementations
│   ├── sources/                    # CSV, Excel, Postgres, MySQL, Mongo, REST, SQLite, PDF
│   └── targets/                    # CSV, JSON, Postgres, Mongo
│
├── agents/                         # Pure functions — pipeline stages
│   ├── extract.py                  # Resolve connector, stream/read, build schema
│   ├── validate.py                 # Null rates, consistency, hard/soft failures
│   ├── transform.py                # Route to AI/custom/SQL runner
│   ├── load.py                     # Resolve target, write chunks, finalize
│   ├── load_raw.py                 # ELT-only: load raw data to staging table
│   └── transform_in_target.py      # ELT-only: LLM SQL → CREATE TABLE AS SELECT
│
├── connectors/                     # Registry and backward-compat re-exports
│   ├── registry.py                 # Single source of truth: type → connector
│   └── base.py                     # Re-exports from ports/
│
├── transform/                      # Transform execution engines
│   ├── ai_runner.py                # LLM code gen → validate → exec with retry
│   ├── custom_runner.py            # User .py file → validate → exec
│   └── sql_runner.py               # SQL validate → transpile → execute
│
├── llm/                            # LLM provider layer
│   ├── gemini.py                   # Google Gemini (google-genai SDK)
│   ├── claude.py                   # Anthropic Claude (anthropic SDK)
│   ├── openai.py                   # OpenAI (openai SDK)
│   ├── qwen.py                     # Alibaba Qwen (dashscope SDK)
│   ├── registry.py                 # Provider lookup table
│   ├── schema.py                   # Schema sampler (type inference)
│   └── prompt_builder.py           # Structured prompts for code/SQL generation
│
├── graph/                          # LangGraph state and pipeline graphs
│   ├── state.py                    # PipelineState TypedDict
│   ├── etl.py                      # ETL: extract → validate → transform → load
│   └── elt.py                      # ELT: extract → validate → load_raw → transform_in_target
│
├── core/                           # Cross-cutting concerns
│   └── destructive.py              # Destructive operation detection
│
├── runner.py                       # Composition root — config → state → graph execution
├── scheduler.py                    # APScheduler-based cron scheduling with SQLite persistence
├── daemon.py                       # Background daemon management (PID file, log tailing)
├── config.py                       # Pydantic v2 config models + YAML parsing
├── exceptions.py                   # Domain error hierarchy
├── logging.py                      # Structured logging (structlog)
└── cli.py                          # Typer CLI — run, validate, schedule, init, daemon

tests/
├── unit/                           # Fast, no external services
│   ├── agents/                     # Agent tests
│   ├── connectors/                 # Connector tests
│   └── ...                         # LLM, config, validator, scheduler, daemon, edge cases, benchmarks
├── integration/                    # Requires running databases
└── e2e/                            # End-to-end CLI tests
```

### Adding a New Connector

1. Create the adapter in `loafer/adapters/sources/` or `loafer/adapters/targets/`
2. Implement the `SourceConnector` or `TargetConnector` ABC from `loafer/ports/connector.py`
3. Register it in `loafer/connectors/registry.py`:
   ```python
   from loafer.adapters.sources.my_source import MySourceConnector as _My
   _register_source("my_type", _My)
   ```
4. Add a `_build_source` or `_build_target` case in `registry.py` to construct it from config
5. Write tests in `tests/unit/connectors/`

### Adding a New LLM Provider

1. Create `loafer/llm/my_provider.py` implementing `LLMProvider` from `loafer/ports/llm.py`
2. Register it in `loafer/llm/registry.py`:
   ```python
   def _register_my_provider() -> None:
       from loafer.llm.my_provider import MyProvider
       def _factory(**kwargs): ...
       register_provider("my_provider", _factory)
   _register_my_provider()
   ```
3. Write tests in `tests/unit/`

### Code Conventions

- **Type annotations** on every function signature and return type
- **No comments** unless explaining non-obvious behavior
- **Docstrings** for modules and public classes only
- **Pure functions** for agents — no side effects, no connector instantiation
- **Streaming** — generators over lists for data flow
- **Error handling** — domain-specific exceptions, never bare `except:`
- **Run `ruff check . --fix && ruff format .` before committing**

---

## Supported Connectors

### Sources

| Connector | Type | Streaming | Notes |
|-----------|------|-----------|-------|
| CSV | `csv` | Yes | UTF-8 with latin-1 fallback, malformed row skipping |
| Excel | `excel` | Yes | Formula values, unmerge cells, mixed type coercion |
| PostgreSQL | `postgres` | Yes | Server-side cursor, type conversion (Decimal→float, UUID→string, datetime→ISO 8601) |
| MySQL | `mysql` | Yes | fetchmany-based streaming, type conversion |
| MongoDB | `mongo` | Yes | ObjectId→string, nested docs passthrough |
| REST API | `rest_api` | Yes | Pagination, rate limit retry, Bearer auth |
| SQLite | `sqlite` | Yes | File-based, no server needed |
| PDF | `pdf` | Yes | Text and table extraction via pdfplumber |

### Targets

| Connector | Type | Streaming | Notes |
|-----------|------|-----------|-------|
| CSV | `csv` | Yes | Auto-create directories, None→empty string |
| JSON | `json` | Yes | Streaming JSON array write |
| PostgreSQL | `postgres` | Yes | Auto-create table, schema inference, batch rollback |
| MongoDB | `mongo` | Yes | Auto-create collection, write modes (append/error) |

---

## Running with Docker

### Build the image

```bash
docker build -f docker/Dockerfile -t loafer .
```

### Run a pipeline

```bash
docker run --rm \
  -v $(pwd)/my-pipeline:/configs \
  -v $(pwd)/my-pipeline:/data \
  -e GEMINI_API_KEY=your-key-here \
  loafer run /configs/pipeline.yaml
```

### Use docker-compose (with databases)

```bash
# Start Postgres, MongoDB, and Loafer
docker compose -f docker/docker-compose.yml up -d

# Run a pipeline inside the container
docker compose -f docker/docker-compose.yml exec loafer loafer run /app/examples/pipeline.quickstart.yaml

# Stop all services
docker compose -f docker/docker-compose.yml down
```

### Volume mounts

| Container path | Purpose |
|----------------|---------|
| `/configs` | Pipeline YAML config files |
| `/data` | Input data files and output files |
| `/root/.loafer` | Scheduler state (PID, logs, job store) |

---

## Configuration Reference

Every pipeline is defined in a single YAML file. All fields are validated at parse time by Pydantic v2.

### Top-Level Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | `""` | Human-readable pipeline name |
| `source` | object | *(required)* | Source connector configuration |
| `target` | object | *(required)* | Target connector configuration |
| `transform` | object / string | *(required)* | Transform config — shorthand string for AI mode |
| `mode` | `"etl"` \| `"elt"` | `"etl"` | ETL: transform before loading; ELT: load raw then transform in-target |
| `chunk_size` | int | `500` | Rows per batch |
| `streaming_threshold` | int | `10_000` | Auto-activate streaming above this row count |
| `destructive_filter_threshold` | float | `0.3` | Warn if transform drops more than this fraction |
| `validation` | object | | Data quality settings |
| `llm` | object | | LLM provider configuration |

### Transform Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `"ai"` \| `"custom"` \| `"sql"` | *(required)* | Transform mode |
| `instruction` | string | — | Natural language instruction (AI mode) |
| `path` | string | — | Path to `.py` file (custom mode) or SQL query (sql mode) |
| `custom_path` | string | `null` | Optional custom `.py` file to combine with AI |
| `custom_order` | `"custom_first"` \| `"ai_first"` | `"custom_first"` | Execution order when combining custom + AI |
| `bypass_ai` | bool | `false` | Skip AI entirely, only run custom transform |
| `review` | bool | `false` | Show AI-generated code and wait for confirmation before executing |

### Environment Variable Interpolation

Use `${ENV_VAR}` syntax anywhere in your config:

```yaml
source:
  type: postgres
  url: ${DATABASE_URL}

llm:
  provider: gemini
  api_key: ${GEMINI_API_KEY}
```

Missing variables raise a `ConfigError` at parse time — before any I/O happens.

---

## License

MIT
