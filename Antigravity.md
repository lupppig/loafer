# Loafer — Principal Engineer Build Prompt for Antigravity

> You are building **Loafer**, a production-grade, open-source CLI tool for running AI-assisted ETL and ELT pipelines. The user describes their transformation in plain English. Loafer handles extraction, validation, transformation, and loading — orchestrated as a stateful LangGraph agent graph, powered by Gemini for the MVP.
>
> You are a principal software engineer. Every decision you make accounts for correctness, edge cases, token efficiency, scalability, speed, and the long-term evolution of this codebase. You do not cut corners. You do not write throwaway code. Tests are not optional.

---

## Non-Negotiable Rules — Read Before Touching Anything

These apply to every single action you take in this repo. No exceptions.

**Package management**
- This project uses `uv` exclusively. Never use `pip install`, `pip freeze`, or `python -m venv`. Every dependency operation goes through `uv add`, `uv remove`, or `uv sync`. The lockfile is `uv.lock` and it is always committed.

**Git hygiene**
- Never run `git add .`. Ever. Stage only the files you actually created or modified in that commit. If you changed `loafer/agents/extract.py` and `tests/unit/agents/test_extract_agent.py`, stage exactly those two files and nothing else.
- `ANTIGRAVITY_PROMPT.md` must never be committed. It must never appear in `.gitignore` either. You simply never stage it. If you ever see it in `git status`, ignore it and move on.
- Commits are incremental. One logical unit of work per commit. A commit that scaffolds the project structure is one commit. A commit that implements the Extract Agent is another. Do not batch unrelated work.
- Commit messages are lowercase, imperative, and descriptive. They describe what the commit does, not what phase it belongs to.

  Correct examples:
  ```
  scaffold project structure and pyproject.toml
  add pipelinestate typeddict and config models
  implement csv source connector with streaming support
  add schema sampler with type inference
  fix null rate calculation in validate agent
  ```

 start a commit message with `feat:`, `fix:`, `chore:`, and never with `Phase 0`, `Phase 1`, or any phase label. No numbered phases in messages.

**Code cleanliness**
- No unnecessary comments. If the code is clear, there is no comment. If a comment is needed, it explains why, not what.
- No numbered comments. Never write `# 1. do this`, `# 2. then this`. Write clean code that reads in sequence.
- No TODO comments committed to the repo. If something is not implemented yet, it either raises `NotImplementedError` with a message or it does not exist yet.
- No commented-out code blocks committed to the repo.

**DRY — Don't Repeat Yourself**
- Any logic that appears in more than one place must be extracted. No exceptions.
- Connector resolution logic lives in exactly one place (`loafer/connectors/registry.py`). If you find yourself writing `if config.type == "postgres"` in more than one file, you have already violated DRY.
- Schema sampling logic lives in exactly one place (`loafer/llm/schema.py`). Agents do not inline their own sampling.
- Error formatting lives in exactly one place. A helper that constructs a `LoaferError` with standard context fields is not a luxury — it is a requirement.
- If you write the same test fixture setup in more than one test file, move it to `conftest.py`.

**Architecture — Ports and Adapters (Hexagonal)**

This codebase follows the Ports and Adapters pattern. Every contributor must understand this before writing a line of code.

The rule is simple: **the core domain never imports from infrastructure**.

```
loafer/core/          ← pure domain logic. No I/O. No framework imports.
loafer/ports/         ← abstract interfaces (ABCs). What the core needs from the outside.
loafer/adapters/      ← concrete implementations. Connectors, LLM clients, schedulers.
loafer/graph/         ← LangGraph wiring. Depends on core + ports only.
loafer/cli.py         ← entry point. Assembles adapters and hands them to the graph.
```

In practice this means:
- `loafer/agents/` is core. It imports from `loafer/graph/state.py` and `loafer/ports/` only. Never from `loafer/connectors/` or `loafer/llm/gemini.py`.
- `loafer/connectors/` is an adapter. It implements `SourceConnector` and `TargetConnector` ports. It knows nothing about agents.
- `loafer/llm/gemini.py` is an adapter. It implements `LLMProvider`. It knows nothing about the pipeline graph.
- `loafer/runner.py` is the composition root. It is the only place where adapters are instantiated and injected into the core. This is where `GeminiProvider`, `PostgresConnector`, and `CsvTargetConnector` are created.

**Why this matters for contributors:** Any contributor adding a new source connector writes one file in `loafer/adapters/sources/`, implements the `SourceConnector` port, registers it in `loafer/connectors/registry.py`, and is done. They never need to understand the agent graph. Any contributor adding a new LLM provider writes one file in `loafer/adapters/llm/`, implements `LLMProvider`, registers it in `loafer/llm/registry.py`, and is done.

Update the repository structure to reflect this:

```
loafer/
├── loafer/
│   ├── __init__.py
│   ├── cli.py
│   ├── runner.py                       # composition root
│   ├── exceptions.py
│   ├── config.py
│   ├── core/
│   │   └── destructive.py             # destructive operation detector
│   ├── ports/
│   │   ├── __init__.py
│   │   ├── connector.py               # SourceConnector + TargetConnector ABCs
│   │   └── llm.py                     # LLMProvider ABC + result dataclasses
│   ├── adapters/
│   │   ├── __init__.py
│   │   ├── sources/
│   │   │   ├── postgres.py
│   │   │   ├── mysql.py
│   │   │   ├── mongo.py
│   │   │   ├── csv_source.py
│   │   │   ├── excel_source.py
│   │   │   └── rest_api.py
│   │   ├── targets/
│   │   │   ├── postgres.py
│   │   │   ├── csv_target.py
│   │   │   └── json_target.py
│   │   └── llm/
│   │       ├── gemini.py
│   │       ├── schema.py
│   │       ├── prompt_builder.py
│   │       ├── code_validator.py
│   │       ├── sql_validator.py
│   │       └── registry.py
│   ├── agents/
│   │   ├── __init__.py
│   │   ├── extract.py
│   │   ├── validate.py
│   │   ├── transform.py
│   │   ├── load.py
│   │   └── transform_in_target.py
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── ai_runner.py
│   │   ├── custom_runner.py
│   │   └── sql_runner.py
│   ├── graph/
│   │   ├── __init__.py
│   │   ├── state.py
│   │   ├── etl.py
│   │   └── elt.py
│   ├── logging.py                     # structured logging setup
│   └── scheduler.py
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_config.py
│   │   ├── test_schema_sampler.py
│   │   ├── test_prompt_builder.py
│   │   ├── test_code_validator.py
│   │   ├── test_sql_validator.py
│   │   ├── test_destructive_detector.py
│   │   ├── agents/
│   │   │   ├── test_extract_agent.py
│   │   │   ├── test_validate_agent.py
│   │   │   ├── test_transform_agent.py
│   │   │   └── test_load_agent.py
│   │   └── adapters/
│   │       ├── test_postgres_source.py
│   │       ├── test_csv_source.py
│   │       └── test_rest_api_source.py
│   ├── integration/
│   │   ├── test_etl_pipeline.py
│   │   └── test_elt_pipeline.py
│   └── e2e/
│       ├── test_cli_run.py
│       └── test_cli_schedule.py
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── examples/
│   ├── pipeline.example.yaml
│   ├── etl_postgres_to_csv.yaml
│   └── elt_api_to_postgres.yaml
├── .env.example
├── .ruff.toml
├── mypy.ini
├── pyproject.toml
├── CONTRIBUTING.md
└── README.md
```

---

## Project Constraints You Must Never Violate

- **Token efficiency is a first-class concern.** Gemini never sees raw data. It only sees a schema sample — column names, inferred types, and a maximum of 5 representative rows. If the dataset is 10 million rows, Gemini still sees 5 rows. Build this into the architecture from day one, not as an afterthought.

- **The LLM layer is swappable.** The MVP uses Gemini. The architecture must support Claude, OpenAI, Qwen, and others without touching agent code. All LLM calls go through a provider-agnostic interface.

- **ETL and ELT are first-class citizens.** They are not the same pipeline with a flag flipped. They have fundamentally different graph shapes, different agent responsibilities, and different failure modes. Model them separately.

- **Streaming is not optional.** Any pipeline that cannot handle a dataset larger than available RAM is not production-ready. The extract-transform-load chain must be stream-capable from the start. Do not buffer full datasets unless the source type literally has no other option.

- **Every agent must be independently testable.** Agents are pure functions over `PipelineState`. If you cannot test an agent in isolation with a fixture state, the architecture is wrong.

- **Fail loudly and specifically.** Generic errors are unacceptable. Every failure must tell the user exactly what went wrong, which agent failed, what data looked like at the point of failure, and what they can do about it.

- **The CLI must feel professional.** Rich progress output, colored status, clear error messages. Not print statements.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.11+ |
| Agent orchestration | LangGraph |
| LLM (MVP) | Google Gemini 1.5 Flash (`google-generativeai`) |
| CLI | Typer + Rich |
| Scheduling | APScheduler 3.x (SQLite job store) |
| SQL parsing + validation | sqlglot |
| Structured logging | structlog |
| PostgreSQL | psycopg2-binary |
| MySQL | pymysql |
| MongoDB | pymongo |
| Excel | openpyxl |
| HTTP | httpx (async-capable) |
| Config | PyYAML + pydantic v2 |
| Env | python-dotenv |
| Testing | pytest + pytest-asyncio + pytest-cov |
| Code quality | ruff + mypy |
| Package manager | uv |
| Packaging | pyproject.toml (hatchling) |
| Containerisation | Docker + docker-compose |

---

## Repository Structure (Build to This)

```
loafer/
├── loafer/
│   ├── __init__.py
│   ├── cli.py                          # Typer entrypoint
│   ├── config.py                       # Pydantic config models + YAML parser
│   ├── exceptions.py                   # All custom exceptions
│   ├── llm/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── gemini.py
│   │   ├── schema.py
│   │   └── prompt_builder.py
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── base.py                     # TransformRunner ABC
│   │   ├── ai_runner.py                # Gemini-generated function runner
│   │   ├── custom_runner.py            # Custom Python file runner
│   │   ├── sql_runner.py               # SQL SELECT runner
│   │   ├── code_validator.py           # Static safety check for Python transforms
│   │   └── sql_validator.py            # sqlglot-based SQL safety check
│   ├── connectors/
│   │   ├── __init__.py
│   │   ├── base.py                     # SourceConnector + TargetConnector ABCs
│   │   ├── sources/
│   │   │   ├── postgres.py
│   │   │   ├── mysql.py
│   │   │   ├── mongo.py
│   │   │   ├── csv_source.py
│   │   │   ├── excel_source.py
│   │   │   └── rest_api.py
│   │   └── targets/
│   │       ├── postgres.py
│   │       ├── csv_target.py
│   │       └── json_target.py
│   ├── agents/
│   │   ├── __init__.py
│   │   ├── extract.py
│   │   ├── validate.py
│   │   ├── transform.py                # ETL transform agent
│   │   ├── load.py
│   │   └── transform_in_target.py     # ELT in-target transform agent
│   ├── graph/
│   │   ├── __init__.py
│   │   ├── state.py                    # PipelineState TypedDict
│   │   ├── etl.py                      # ETL StateGraph
│   │   └── elt.py                      # ELT StateGraph
│   ├── scheduler.py
│   └── runner.py                       # Pipeline entry point, mode router
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_config.py
│   │   ├── test_schema_sampler.py
│   │   ├── test_prompt_builder.py
│   │   ├── agents/
│   │   │   ├── test_extract_agent.py
│   │   │   ├── test_validate_agent.py
│   │   │   ├── test_transform_agent.py
│   │   │   └── test_load_agent.py
│   │   └── connectors/
│   │       ├── test_postgres_source.py
│   │       ├── test_csv_source.py
│   │       └── test_rest_api_source.py
│   ├── integration/
│   │   ├── test_etl_pipeline.py
│   │   └── test_elt_pipeline.py
│   └── e2e/
│       ├── test_cli_run.py
│       └── test_cli_schedule.py
├── examples/
│   ├── pipeline.example.yaml
│   ├── etl_postgres_to_csv.yaml
│   └── elt_api_to_postgres.yaml
├── .env.example
├── .ruff.toml
├── mypy.ini
├── pyproject.toml
├── CONTRIBUTING.md
└── README.md
```

---

## Phase 0 — Project Scaffold

**Goal:** A runnable, linted, typed project skeleton. Nothing functional yet. Everything in place.

### Tasks

1. Initialize the project with `uv init` and configure `pyproject.toml` using hatchling. Package name `loafer-etl`, command `loafer`. Python 3.11+ required. Add all dependencies via `uv add`, never `pip install`. Commit `pyproject.toml` and `uv.lock` together.

2. Configure `ruff` for linting and formatting. Configure `mypy` in strict mode. Both must pass on an empty project before any feature code is written.

3. Create `loafer/exceptions.py` with the full exception hierarchy upfront:

```python
class LoaferError(Exception): ...

class ConfigError(LoaferError): ...
class ConnectorError(LoaferError): ...
class ExtractionError(ConnectorError): ...
class LoadError(ConnectorError): ...
class ValidationError(LoaferError): ...
class TransformError(LoaferError): ...
class LLMError(LoaferError): ...
class LLMRateLimitError(LLMError): ...
class LLMInvalidOutputError(LLMError): ...
class SchedulerError(LoaferError): ...
class PipelineError(LoaferError): ...
```

4. Create `loafer/graph/state.py` with `PipelineState`. This is the single source of truth for all data flowing through the system:

```python
from typing import TypedDict, Iterator, Any
from loafer.config import SourceConfig, TargetConfig

class PipelineState(TypedDict):
    # Config
    source_config: SourceConfig
    target_config: TargetConfig
    transform_instruction: str
    mode: str                          # "etl" | "elt"
    chunk_size: int

    # Data (mutated per agent)
    raw_data: list[dict[str, Any]]
    transformed_data: list[dict[str, Any]]

    # Schema (set by Extract Agent, read by Transform Agent for LLM prompt)
    schema_sample: dict[str, Any]      # {column: {type, sample_values[]}}

    # Validation
    validation_report: dict[str, Any]
    validation_passed: bool

    # LLM
    generated_code: str                # last transform fn generated by LLM
    retry_count: int
    last_error: str | None

    # ELT specific
    raw_table_name: str | None         # table name where raw data was loaded
    generated_sql: str | None          # SQL generated for ELT in-target transform

    # Execution metadata
    rows_extracted: int
    rows_loaded: int
    duration_ms: dict[str, float]      # per-agent timing
    warnings: list[str]
    is_streaming: bool
    stream_iterator: Iterator | None   # live when streaming, None otherwise
```

5. Create `loafer/config.py` using Pydantic v2 models for every config section. Validation must happen at parse time, not at runtime inside agents. All connector configs are a `Union` type discriminated on `type`. The `transform` field is a discriminated union across three config shapes:

```python
class AITransformConfig(BaseModel):
    type: Literal["ai"] = "ai"
    instruction: str

class CustomTransformConfig(BaseModel):
    type: Literal["custom"]
    path: Path

    @field_validator("path")
    def path_must_exist(cls, v: Path) -> Path:
        if not v.exists():
            raise ValueError(f"transform file not found: {v}")
        return v

class SQLTransformConfig(BaseModel):
    type: Literal["sql"]
    query: str

TransformConfig = Annotated[
    AITransformConfig | CustomTransformConfig | SQLTransformConfig,
    Field(discriminator="type")
]
```

When `transform` is a plain string in YAML, coerce it automatically to `AITransformConfig(instruction=value)`. Invalid configs must raise `ConfigError` with a message that names the exact field.

6. Set up `tests/conftest.py` with shared fixtures:
   - `minimal_etl_state()` — a fully populated `PipelineState` with 10 rows of fixture data, postgres source config, CSV target config
   - `minimal_elt_state()` — same but ELT mode, postgres target
   - `mock_llm_provider()` — a mock `LLMProvider` that returns a hardcoded valid transform function
   - `sample_schema()` — a realistic schema sample with 5 columns, mixed types

### Tests for Phase 0

- `test_config.py`: parse a valid YAML for each source type, assert no errors. Parse configs with missing required fields, assert `ConfigError` is raised with the field name in the message. Parse configs with wrong types (e.g. `chunk_size: "abc"`), assert `ConfigError`.
- `test_exceptions.py`: assert the exception hierarchy is correct (isinstance checks).

---

## Phase 1 — LLM Provider Layer

**Goal:** A provider-agnostic LLM interface. Gemini implemented. Schema sampler built. Prompt builder built. Nothing else in the system calls Gemini directly — ever.

### Tasks

1. Create `loafer/llm/base.py`:

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class TransformPromptResult:
    code: str                   # the generated Python function as a string
    raw_response: str           # full LLM response for debugging
    token_usage: dict[str, int] # prompt_tokens, completion_tokens, total_tokens

@dataclass
class ELTSQLResult:
    sql: str
    raw_response: str
    token_usage: dict[str, int]

class LLMProvider(ABC):
    @abstractmethod
    def generate_transform_function(
        self,
        schema_sample: dict,
        instruction: str,
        previous_error: str | None = None,
        previous_code: str | None = None,
    ) -> TransformPromptResult: ...

    @abstractmethod
    def generate_elt_sql(
        self,
        target_schema: dict,
        raw_table_name: str,
        instruction: str,
        previous_error: str | None = None,
    ) -> ELTSQLResult: ...
```

2. Create `loafer/llm/schema.py` — the token efficiency layer. This is critical:

```python
def build_schema_sample(
    data: list[dict],
    max_sample_rows: int = 5,
    max_string_length: int = 100,
) -> dict[str, dict]:
    """
    Takes raw data and returns a compact schema representation safe to send to an LLM.
    Never sends full data. Never sends more than max_sample_rows rows per column.
    Truncates long string values. Infers types.

    Output shape:
    {
        "column_name": {
            "inferred_type": "string | integer | float | boolean | datetime | null | mixed",
            "nullable": true,
            "sample_values": ["val1", "val2", ...],  # max 5, truncated if long
            "null_count": 3,
            "total_count": 100
        }
    }
    """
```

    Edge cases to handle:
    - Empty dataset — return empty schema, do not raise
    - All-null column — inferred_type is "null"
    - Mixed types in same column — inferred_type is "mixed", list actual types found
    - Nested dicts/lists (from MongoDB/REST API) — represent as "object" or "array", show one sample only
    - Datetime strings — detect common ISO formats and label as "datetime"
    - Very large number of columns (100+) — still process all, token count is on schema not data

3. Create `loafer/llm/prompt_builder.py`:

```python
def build_etl_transform_prompt(
    schema_sample: dict,
    instruction: str,
    previous_error: str | None = None,
    previous_code: str | None = None,
) -> str:
    """
    Builds the ETL transform prompt. If previous_error is present,
    includes the error and previous_code in the prompt for correction.
    The prompt must instruct Gemini to return ONLY a Python function
    named `transform` with signature:
        def transform(data: list[dict]) -> list[dict]
    No markdown. No explanation. No imports beyond stdlib.
    """

def build_elt_sql_prompt(
    target_schema: dict,
    raw_table_name: str,
    instruction: str,
    previous_error: str | None = None,
) -> str:
    """
    Builds the ELT in-target SQL prompt.
    Instructs Gemini to return a single SQL SELECT statement (no DDL, no DML other than SELECT).
    The result will be used to create a new transformed table via CREATE TABLE AS SELECT.
    """
```

4. Create `loafer/llm/gemini.py` implementing `LLMProvider`:
   - Use `gemini-1.5-flash` — fast and cheap, appropriate for transform generation
   - Implement exponential backoff for rate limit errors (`LLMRateLimitError`)
   - Parse the response and strip any accidental markdown fences before returning
   - Log token usage to `PipelineState.duration_ms` (extend it or add a `token_usage` field to state)
   - If the response is empty or unparseable, raise `LLMInvalidOutputError` with the raw response attached

5. Create a code safety validator in `loafer/llm/code_validator.py`:

```python
def validate_transform_function(code: str) -> tuple[bool, str | None]:
    """
    Static analysis on the generated transform function before execution.
    Returns (is_safe, error_message).

    Must reject:
    - Code containing: import os, import sys, import subprocess, __import__,
      eval, exec, open(), __builtins__, socket, urllib, requests, httpx
    - Code that does not define a function named `transform`
    - Code where `transform` does not have exactly one parameter
    - Code that is not valid Python syntax (use ast.parse)
    - Code longer than 200 lines (suspiciously long)

    Must allow:
    - All stdlib data manipulation: re, json, datetime, math, decimal, uuid, itertools
    - Type annotations
    - Helper functions defined above `transform`
    """
```

### Tests for Phase 1

- `test_schema_sampler.py`:
  - Empty list → empty dict
  - 1000 rows → exactly 5 sample values per column
  - All-null column → inferred_type "null", nullable True
  - Mixed int/string column → inferred_type "mixed"
  - Nested dict value → inferred_type "object"
  - Long string value (500 chars) → truncated to max_string_length in sample_values
  - Column with datetime strings → inferred_type "datetime"

- `test_prompt_builder.py`:
  - ETL prompt contains schema sample serialized correctly
  - ETL prompt with previous_error includes the error text and previous code
  - ELT prompt contains raw_table_name and target schema
  - No prompt exceeds a sane token estimate (use tiktoken or character heuristic)

- `test_code_validator.py`:
  - Valid transform function → (True, None)
  - Code with `import os` → (False, message mentions "os")
  - Code with `eval(` → (False, mentions "eval")
  - Code with no `transform` function → (False, mentions "transform not defined")
  - Code with syntax error → (False, mentions syntax)
  - Code with `transform(a, b)` (wrong signature) → (False, mentions parameter count)
  - Valid function with helper functions above it → (True, None)

- `test_sql_validator.py`:
  - Valid SELECT → (True, None)
  - `DROP TABLE orders` → (False, mentions "Drop")
  - `DELETE FROM users WHERE 1=1` → (False, mentions "Delete")
  - `SELECT id FROM orders; DELETE FROM users` → (False, two statements detected)
  - `UPDATE orders SET status = 'paid'` → (False, mentions "Update")
  - `INSERT INTO orders VALUES (1)` → (False, mentions "Insert")
  - `CREATE TABLE foo AS SELECT 1` → (False, mentions "Create")
  - Malformed SQL → (False, mentions syntax)
  - SELECT with subquery containing DROP in a string literal → (True, None) — AST does not see it as a real DROP node
  - SELECT with multiple columns, WHERE clause, ORDER BY → (True, None)

- `test_gemini.py` (use `unittest.mock` to mock the Gemini SDK):
  - Successful response → returns `TransformPromptResult` with code and token usage
  - Rate limit error → retries with backoff, eventually raises `LLMRateLimitError`
  - Response wrapped in markdown fences → fences are stripped before returning
  - Empty response → raises `LLMInvalidOutputError`

---

## Phase 2 — Connectors

**Goal:** All source and target connectors implemented. Each connector is a streaming-first interface. Connectors know nothing about agents or LangGraph.

### Source Connector Interface

```python
from abc import ABC, abstractmethod
from typing import Iterator, Any
from loafer.config import SourceConfig

class SourceConnector(ABC):
    def __init__(self, config: SourceConfig) -> None: ...

    @abstractmethod
    def connect(self) -> None:
        """Establish connection. Raise ConnectorError on failure."""

    @abstractmethod
    def disconnect(self) -> None: ...

    @abstractmethod
    def stream(self, chunk_size: int) -> Iterator[list[dict[str, Any]]]:
        """
        Yield chunks of rows. Each chunk is a list of dicts.
        Must be a generator — do not load full dataset into memory.
        """

    def read_all(self) -> list[dict[str, Any]]:
        """
        Convenience method. Collects all chunks into a single list.
        Used only for small datasets or schema sampling.
        Default implementation works for all connectors.
        """
        return [row for chunk in self.stream(chunk_size=1000) for row in chunk]

    @abstractmethod
    def count(self) -> int | None:
        """
        Return total row count if cheap to compute, else None.
        Used to decide streaming vs in-memory mode.
        For REST APIs where count requires a full fetch, return None.
        """

    def __enter__(self): self.connect(); return self
    def __exit__(self, *_): self.disconnect()
```

### Source Implementations

**PostgreSQL / MySQL connector edge cases:**
- Connection string is malformed → `ConfigError` at connect time, not at stream time
- Query times out → `ExtractionError` with the query and timeout value in the message
- Query returns 0 rows → valid, yield no chunks, log a warning to state
- Query returns a column named the same as a Python builtin (e.g. `type`, `id`) → allowed, pass through as-is
- Connection is dropped mid-stream → attempt one reconnect, then raise `ExtractionError`
- Numeric types from PostgreSQL (Decimal) → convert to float in the connector, not in the transform agent
- UUID columns → convert to string
- Date/datetime columns → convert to ISO 8601 string (consistent with schema sampler expectations)

**MongoDB connector edge cases:**
- Collection does not exist → `ExtractionError` with collection name
- Filter document is invalid JSON → `ConfigError`
- Documents have inconsistent schemas (common in Mongo) → allowed, do not flatten, pass through as-is
- ObjectId fields → convert `_id` and any ObjectId field to string
- Nested documents → pass through as-is, schema sampler handles them
- Binary fields → skip (log warning), do not try to serialize

**CSV connector edge cases:**
- File does not exist → `ExtractionError` with the path
- File is empty (0 rows, or header only) → yield no chunks, log warning
- File has inconsistent column counts across rows → skip malformed rows, log count of skipped rows as warning
- File encoding is not UTF-8 → attempt latin-1 fallback, log if fallback was used
- File has no header row → raise `ConfigError` asking user to specify `has_header: false` and provide column names
- Very large CSV (multi-GB) → stream with Python's `csv` module, never load into memory

**Excel connector edge cases:**
- File is corrupted → `ExtractionError`
- Sheet name specified but does not exist → `ExtractionError` with available sheet names listed
- Cells with formulas → read computed value, not formula
- Merged cells → unmerge before reading, fill merged area with the cell value
- Mixed types in a column (Excel allows this) → convert all to string for that column, log warning

**REST API connector edge cases:**
- Non-200 response → `ExtractionError` with status code and response body
- Response is not JSON → `ExtractionError` with Content-Type in message
- Response is JSON but not a list or object with a data key → configurable `response_key` field in config, raise `ConfigError` if key not found
- Pagination: detect common patterns (`next`, `next_cursor`, `page`, `offset`) — support all of them via config
- Rate limiting (429) → back off and retry with Retry-After header if present
- SSL errors → `ExtractionError` with note to check `verify_ssl: false` config option
- Timeout → configurable, default 30s, raise `ExtractionError` on timeout
- Auth: Bearer token only for MVP. Token is never logged.

### Target Connector Interface

```python
class TargetConnector(ABC):
    def __init__(self, config: TargetConfig) -> None: ...

    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def disconnect(self) -> None: ...

    @abstractmethod
    def write_chunk(self, chunk: list[dict[str, Any]]) -> int:
        """Write a chunk. Return number of rows written."""

    @abstractmethod
    def finalize(self) -> None:
        """Called after all chunks are written. Flush buffers, close files, commit txns."""

    def __enter__(self): self.connect(); return self
    def __exit__(self, *_): self.finalize(); self.disconnect()
```

### Target Implementations

**PostgreSQL target edge cases:**
- Table does not exist → create it. Infer column types from the first chunk. Use `TEXT` for unknown types. Never use `VARCHAR(n)` — use `TEXT`.
- Table already exists with different schema → raise `LoadError` with diff of schemas, do not silently truncate or alter
- Table already exists with same schema → append by default, truncate if `write_mode: replace` in config
- Insert batch fails → rollback the batch only, retry once, then raise `LoadError` with the offending rows
- Column value is `None` → insert as NULL
- Column value is a dict or list → serialize to JSON string before insert
- Very wide rows (100+ columns) → batch size should be reduced automatically (100 rows instead of 500)

**CSV target edge cases:**
- Output directory does not exist → create it
- File already exists → overwrite by default, raise error if `write_mode: error` in config
- Streaming to CSV: write header on first chunk, do not re-write on subsequent chunks
- None values → write as empty string (standard CSV behavior)
- Values containing commas or newlines → quote them (Python's csv module handles this)

**JSON target edge cases:**
- Streaming to JSON: write a JSON array incrementally — open `[`, write each chunk's rows as comma-separated objects, close `]`. Do not buffer the full array.
- Datetime objects in rows → serialize to ISO 8601 string before writing
- Non-serializable types (Decimal, UUID from transforms) → convert to str/float in the target, do not let `json.dumps` fail silently

### Tests for Phase 2

- Each connector has its own test file
- Use `pytest` fixtures with real SQLite for postgres tests (or `pytest-postgresql` for full fidelity)
- Use `tmp_path` fixture for CSV/JSON/Excel tests
- Mock `httpx` responses for REST API tests
- Each edge case listed above must have a corresponding test
- Test `stream()` by collecting all chunks and asserting total row count matches source
- Test `write_chunk()` + `finalize()` sequence, then read back the output and assert correctness

---

## Phase 3 — Agents

**Goal:** All four agents implemented. Each is a pure function over `PipelineState`. No agent imports from another agent. Agents do not instantiate connectors — connectors are injected via the runner.

### Agent Signature Convention

```python
from loafer.graph.state import PipelineState

def extract_agent(state: PipelineState) -> PipelineState:
    ...
```

LangGraph nodes must return the updated state. Do not mutate state in place — return a new dict with updated fields (or use `TypedDict` spread).

### Extract Agent (`loafer/agents/extract.py`)

Responsibilities:
1. Resolve the correct `SourceConnector` from `state.source_config.type`
2. Call `connector.count()` — if count exceeds `streaming_threshold` (or count is None), set `state.is_streaming = True`
3. If not streaming: call `connector.read_all()`, set `state.raw_data`
4. If streaming: set `state.stream_iterator = connector.stream(state.chunk_size)` — do NOT consume it here
5. Build `state.schema_sample` using `build_schema_sample()` from the llm layer. If streaming: peek the first chunk for schema, put it back at the front of the iterator.
6. Set `state.rows_extracted` (from count or by consuming iterator — prefer count)
7. Record timing in `state.duration_ms["extract"]`

Edge cases:
- Source returns 0 rows → set `raw_data = []`, set a warning, set `validation_passed = False` with reason "empty source"
- Connector fails to connect → raise `ExtractionError`, do not swallow
- Schema sample fails (e.g. all rows are empty dicts) → set `schema_sample = {}`, add warning

### Validate Agent (`loafer/agents/validate.py`)

Responsibilities:
1. If `state.raw_data` is empty and `state.stream_iterator` is None → set `validation_passed = False`, add reason to report
2. Check null rate per column against config threshold
3. Check schema consistency: if >10% of rows are missing a column that exists in the schema sample, flag it
4. Check for columns whose inferred type is "mixed" — add soft warning, do not fail
5. Set `state.validation_report` with per-column stats
6. Set `state.validation_passed = True` if no hard failures
7. Record timing

Hard failures (block pipeline):
- Source is empty (0 rows)
- Any column exceeds `max_null_rate` AND `strict: true` in validation config
- Schema sample is empty

Soft warnings (log, continue):
- Mixed type columns
- Column null rate above 50% (when `strict: false`)
- Schema inconsistency below 10% of rows

### Transform Agent — ETL (`loafer/agents/transform.py`)

This is the most complex agent. It supports three transform modes. The mode is resolved from `state.transform_config.type` before any execution begins.

**Transform mode resolution**

```python
match state.transform_config.type:
    case "ai":     run AiTransformRunner
    case "custom": run CustomTransformRunner
    case "sql":    run SqlTransformRunner
```

Each runner lives in `loafer/transform/` and implements the `TransformRunner` ABC:

```python
class TransformRunner(ABC):
    @abstractmethod
    def run(self, state: PipelineState) -> PipelineState: ...
```

The agent instantiates the correct runner and delegates. It does not contain mode-specific logic itself.

**AI mode (`loafer/transform/ai_runner.py`)**

- Build the LLM prompt using `prompt_builder.build_etl_transform_prompt()`
- Call `state.llm_provider.generate_transform_function()`
- Run `code_validator.validate_transform_function()` — if invalid, treat as LLM error and trigger retry
- Execute the generated function in a restricted `exec` context
- On failure: increment `state.retry_count`, set `state.last_error`, set `state.generated_code`
  - LangGraph conditional edge routes back to the agent if `retry_count < 3`
  - If `retry_count >= 3`, raise `TransformError` with all three attempts and errors attached
- Token usage across all retries is accumulated in state

**Custom Python mode (`loafer/transform/custom_runner.py`)**

- Load the `.py` file at `state.transform_config.path`
- Run `code_validator.validate_transform_function()` on it — same safety rules as AI mode
- If the file does not define a `transform` function with the correct signature, raise `TransformError` immediately — no retry
- Execute the function against the data
- No LLM call. No retry loop. One attempt. If it fails, the error is the user's to fix.

**SQL mode (`loafer/transform/sql_runner.py`)**

- Read the SQL from `state.transform_config.query`
- Run `sql_validator.validate_transform_sql()` — see full spec below
- Substitute `{{source}}` using parameterized identifiers, never string formatting
- Use `sqlglot.transpile()` to translate the SQL to the correct target dialect before execution
- For ETL mode: execute on a temporary in-memory representation or a scratch table, read results back into `state.transformed_data`
- For ELT mode: wrap in `CREATE TABLE {output} AS (...)` and execute directly on the target

**SQL validator (`loafer/transform/sql_validator.py`)**

This is not optional hardening. It runs on every SQL transform, every time, before any database connection is touched.

```python
import sqlglot
import sqlglot.expressions as exp

def validate_transform_sql(sql: str) -> tuple[bool, str | None]:
    try:
        statements = sqlglot.parse(sql)
    except sqlglot.errors.ParseError as e:
        return False, f"invalid SQL syntax: {e}"

    if len(statements) != 1:
        return False, f"exactly one SELECT statement required, got {len(statements)}"

    statement = statements[0]

    if not isinstance(statement, exp.Select):
        return False, f"only SELECT is allowed, got {type(statement).__name__}"

    disallowed = (
        exp.Drop, exp.Delete, exp.Update, exp.Insert,
        exp.Create, exp.AlterTable, exp.Command, exp.Truncate,
    )
    for node in statement.walk():
        if isinstance(node, disallowed):
            return False, f"disallowed operation in statement: {type(node).__name__}"

    return True, None
```

The validator uses AST analysis, not string matching. `DROP` embedded inside a comment or a subquery string will not fool it. Any non-SELECT is rejected before it reaches the database.

The `{{source}}` substitution uses `psycopg2.sql.Identifier` for PostgreSQL and the equivalent for each target dialect. The table name is never interpolated as a raw string.

**Critical implementation details (all modes)**

- The transform function receives `list[dict]` and must return `list[dict]`. If it returns anything else, raise `TransformError`.
- If transform returns fewer rows than input, that is valid — filtering is expected behavior.
- If transform returns 0 rows, add a warning but do not fail.
- If the function raises an exception, capture the full traceback. AI mode feeds it back to Gemini. Custom mode surfaces it directly to the user.
- Cap total transform execution time at 60 seconds (configurable). Raise `TransformError` if exceeded.
- If streaming: apply the transform function chunk by chunk via `state.stream_iterator`.



### Load Agent (`loafer/agents/load.py`)

Responsibilities:
1. Resolve the correct `TargetConnector` from `state.target_config.type`
2. If not streaming: write `state.transformed_data` in chunks of `state.chunk_size`
3. If streaming: consume the stream iterator (which has been transformed chunk-by-chunk) and write each chunk
4. Call `connector.finalize()` after all chunks are written
5. Set `state.rows_loaded`
6. Record timing

Edge cases:
- Write fails mid-stream → do not silently discard. Record how many rows were written before failure, include in `LoadError`.
- Target connector raises on `finalize()` → surface the error with rows_loaded count so user knows partial data may exist
- `transformed_data` is empty (transform filtered everything) → still call finalize, log warning

### Transform-in-Target Agent — ELT (`loafer/agents/transform_in_target.py`)

Only used in ELT mode. At this point, raw data is already in the target.

Responsibilities:
1. Query the target for the schema of `state.raw_table_name`
2. Build the ELT SQL prompt with `prompt_builder.build_elt_sql_prompt()`
3. Call LLM to get a SQL SELECT statement
4. Validate: must be a SELECT, must not contain DROP/DELETE/UPDATE/INSERT/TRUNCATE/ALTER/CREATE — reject and retry if so
5. Wrap the SELECT in `CREATE TABLE {output_table_name} AS ({sql})` and execute on target
6. Set `state.rows_loaded` from the new table's row count
7. Record timing

Edge cases:
- LLM returns DDL or DML instead of SELECT → retry with explicit error
- SQL is syntactically valid but fails on the target (e.g. references a column that doesn't exist) → capture DB error, retry with schema + error
- Output table already exists → raise `LoadError` unless `write_mode: replace`

### Tests for Phase 3

For each agent, write tests against `conftest.py` fixtures:

- Extract Agent:
  - Small dataset (< threshold) → `is_streaming = False`, `raw_data` populated
  - Large dataset (> threshold) → `is_streaming = True`, `stream_iterator` is not None
  - Connector raises `ExtractionError` → agent propagates it, does not swallow
  - 0 rows → `raw_data = []`, warning added to state

- Validate Agent:
  - Valid data → `validation_passed = True`
  - Empty data → `validation_passed = False`
  - Column null rate exceeds threshold with `strict: true` → `validation_passed = False`
  - Column null rate exceeds threshold with `strict: false` → warning, still passes

- Transform Agent:
  - AI mode, valid LLM response → `transformed_data` populated
  - AI mode, LLM returns code with `import os` → code_validator catches it, retry triggered
  - AI mode, LLM returns code that raises on execution → retry triggered with traceback in prompt
  - AI mode, three consecutive failures → `TransformError` raised with all attempts attached
  - AI mode, transform returns 0 rows → warning added, no error raised
  - Custom mode, valid `.py` file with correct signature → executed, no LLM call made
  - Custom mode, file does not exist → `TransformError` immediately, no retry
  - Custom mode, file defines `transform` with wrong signature → `TransformError` immediately
  - Custom mode, function raises at runtime → `TransformError` with traceback, no retry
  - Custom mode, function contains `import os` → code_validator rejects it before execution
  - SQL mode, valid SELECT → validated, transpiled, executed
  - SQL mode, statement is `DROP TABLE orders` → `TransformError` before any DB connection
  - SQL mode, statement is `SELECT ...; DELETE FROM users` → two statements detected, rejected
  - SQL mode, `{{source}}` substituted via parameterized identifier, never string format
  - SQL mode, query references non-existent column → DB error captured, `TransformError`
  - SQL mode, `sqlglot.transpile` called with correct source and target dialects

- Load Agent:
  - Writes all rows → `rows_loaded` matches `len(transformed_data)`
  - Streaming mode → writes in chunks, `rows_loaded` is accurate
  - Target connector raises on `write_chunk` → `LoadError` with partial count

---

## Phase 4 — LangGraph Graphs

**Goal:** ETL and ELT pipeline graphs compiled and runnable. Conditional edges for retry logic. Proper terminal states.

### ETL Graph (`loafer/graph/etl.py`)

```python
from langgraph.graph import StateGraph, END

def build_etl_graph() -> CompiledGraph:
    graph = StateGraph(PipelineState)

    graph.add_node("extract", extract_agent)
    graph.add_node("validate", validate_agent)
    graph.add_node("transform", transform_agent)
    graph.add_node("load", load_agent)

    graph.set_entry_point("extract")
    graph.add_edge("extract", "validate")

    graph.add_conditional_edges(
        "validate",
        lambda state: "transform" if state["validation_passed"] else END,
    )

    graph.add_conditional_edges(
        "transform",
        lambda state: "load" if not state["last_error"] else
                      "transform" if state["retry_count"] < 3 else END,
    )

    graph.add_edge("load", END)

    return graph.compile()
```

### ELT Graph (`loafer/graph/elt.py`)

```python
def build_elt_graph() -> CompiledGraph:
    graph = StateGraph(PipelineState)

    graph.add_node("extract", extract_agent)
    graph.add_node("validate", validate_agent)
    graph.add_node("load_raw", load_raw_agent)          # loads untransformed data
    graph.add_node("transform_in_target", transform_in_target_agent)

    graph.set_entry_point("extract")
    graph.add_edge("extract", "validate")

    graph.add_conditional_edges(
        "validate",
        lambda state: "load_raw" if state["validation_passed"] else END,
    )

    graph.add_edge("load_raw", "transform_in_target")

    graph.add_conditional_edges(
        "transform_in_target",
        lambda state: END if not state["last_error"] else
                      "transform_in_target" if state["retry_count"] < 3 else END,
    )

    return graph.compile()
```

### Runner (`loafer/runner.py`)

The runner is the single entry point. It:
1. Parses the config (file or CLI flags) into a validated config object
2. Resolves the LLM provider from config (Gemini for MVP)
3. Builds initial `PipelineState` from config
4. Selects and compiles the correct graph (ETL or ELT)
5. Invokes the graph with `graph.invoke(initial_state)`
6. Returns the final state for the CLI to format and display

The runner catches all `LoaferError` subclasses and re-raises them with pipeline context attached (rows extracted, which agent failed, timing).

### Tests for Phase 4

- `test_etl_pipeline.py` (integration):
  - Full ETL run from CSV source to JSON target with mock LLM → assert output file exists and rows match
  - Validation failure → pipeline terminates at validate node, no output written
  - Transform retry → mock LLM fails twice, succeeds on third → assert `retry_count == 2` in final state
  - Transform permanent failure → assert `TransformError` raised after 3 attempts

- `test_elt_pipeline.py` (integration):
  - Full ELT run with postgres target and mock LLM SQL generation → assert output table exists
  - In-target SQL failure → retry, eventual failure → assert error state

---

## Phase 5 — CLI

**Goal:** A professional CLI using Typer and Rich. Every command works. Output is clean and informative.

### Commands

```
loafer run [config_file] [--source] [--target] [--transform] [--mode] [--chunk-size]
           [--api-token] [--api-pages] [--sheet] [--dry-run] [--verbose]

loafer schedule <config_file> [--cron] [--interval] [--name]
loafer jobs
loafer jobs cancel <job_id>
loafer logs <job_id> [--tail]
loafer validate <config_file>
loafer connectors
```

### CLI Implementation Notes

- Use `rich.console.Console` for all output. Never use `print()`.
- Use `rich.progress.Progress` for streaming progress bars.
- Use `rich.table.Table` for `loafer jobs` output.
- Use `rich.panel.Panel` for the run summary at the end (rows extracted, rows loaded, duration per agent, token usage, warnings).
- All errors print to stderr via `Console(stderr=True)`. Exit code 1 on any `LoaferError`.
- `--dry-run` runs extract, validate, and transform but skips load. Prints a preview of the first 20 transformed rows as a Rich table.
- `--verbose` prints each agent's start/end and the generated transform code.
- `loafer validate` parses the config and runs the Pydantic validator only. Does not connect to any source or target. Prints "Config is valid" or a table of errors.

### Run Summary Output (example)

```
╭─ Loafer Pipeline Complete ──────────────────────────────────────╮
│  Mode          ETL                                              │
│  Source        PostgreSQL (orders, 42,801 rows)                 │
│  Target        CSV → ./output/orders.csv                        │
│                                                                 │
│  Extract       1.2s                                             │
│  Validate      0.1s    ✓ passed                                 │
│  Transform     3.4s    ✓ passed (1 retry)                       │
│  Load          2.1s                                             │
│                                                                 │
│  Rows loaded   41,923 (878 filtered by transform)               │
│  Token usage   prompt: 412   completion: 187   total: 599       │
│                                                                 │
│  Warnings      2                                                │
│  • Column `phone`: 34% null rate                                │
│  • Column `notes`: mixed types detected                         │
╰─────────────────────────────────────────────────────────────────╯
```

### Tests for Phase 5

- `test_cli_run.py` using `typer.testing.CliRunner`:
  - Valid config file → exit code 0, output contains "Pipeline Complete"
  - Missing config file → exit code 1, error mentions the path
  - Invalid config (missing required field) → exit code 1, error names the field
  - `--dry-run` → exit code 0, "Load skipped" in output, no output file created
  - `loafer validate` with valid config → exit code 0
  - `loafer validate` with invalid config → exit code 1, table of errors

---

## Phase 6 — Scheduling

**Goal:** APScheduler-backed job scheduling. Jobs persist across restarts. CLI commands work correctly.

### Implementation

- Job store: SQLite at `~/.loafer/jobs.db`
- Scheduler runs as a background process spawned by `loafer schedule`
- Each job stores: job_id, name, config_path, schedule (cron or interval), last_run, next_run, last_status, last_error
- Logs per job written to `~/.loafer/logs/<job-name>/<timestamp>.log`
- `loafer jobs` reads from the SQLite job store directly and renders a Rich table
- `loafer logs <job-id>` tails the most recent log file

Edge cases:
- Config file referenced by a scheduled job is deleted → job fails with `ConfigError`, logs the error, does not crash the scheduler
- Two jobs with the same name → raise `SchedulerError`, do not silently overwrite
- System clock changes (DST, NTP sync) → APScheduler handles this correctly by design, but test it
- Machine restarts → scheduler must auto-resume on next `loafer schedule` invocation (job store persistence)

### Tests for Phase 6

- Schedule a job, list it, cancel it, assert it no longer appears
- Schedule a job with a cron that already passed → next_run is in the future (next occurrence)
- Duplicate job name → `SchedulerError`

---

## Logging

Loafer has two distinct logging audiences and they must never be conflated.

**End-user logs** are what appears in the terminal and in scheduled job log files. They are human-readable, actionable, and terse. They tell the user what happened, how long it took, and what to do if something went wrong. Rich handles rendering for terminal output. For scheduled jobs, these are written as plain text to `~/.loafer/logs/<job-name>/<timestamp>.log`.

**Debug logs** are structured JSON, written to `~/.loafer/debug.log`. They contain machine-readable context: agent name, pipeline run ID, timestamps, row counts, token usage, connector type, exception tracebacks, retry attempts, generated code, and any internal state mutations. These are for debugging failed pipelines, not for reading in the terminal.

Use `structlog` for all internal logging. Configure it at startup in `loafer/logging.py`:

```python
import structlog

def configure_logging(debug: bool = False) -> None:
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if debug:
        structlog.configure(
            processors=[
                *shared_processors,
                structlog.processors.JSONRenderer(),
            ],
            logger_factory=structlog.WriteLoggerFactory(
                file=open(Path.home() / ".loafer" / "debug.log", "a")
            ),
        )
    else:
        structlog.configure(
            processors=[
                *shared_processors,
                structlog.dev.ConsoleRenderer(),
            ],
        )
```

Every agent binds a logger at the start of its execution with its name and the pipeline run ID:

```python
log = structlog.get_logger().bind(agent="extract", run_id=state["run_id"])
log.info("starting extraction", source_type=state["source_config"]["type"])
```

**What must be logged at each stage:**

Extract Agent:
- Source type and connection target (never log passwords or tokens)
- Row count and whether streaming mode activated
- Schema sample column count
- Any warnings about empty sources or schema failures

Validate Agent:
- Per-column null rates
- Whether validation passed or failed, and the exact reason if failed

Transform Agent:
- Which transform mode was selected (ai, custom, sql)
- For AI mode: token usage per attempt, retry count
- Generated code at DEBUG level only (it can be large)
- Execution time
- Row delta (input rows vs output rows)

Load Agent:
- Target type
- Rows written, batch count
- Any partial write on failure

**Logging rules:**
- Never log raw data rows, even at DEBUG level. Log schema, counts, and column names only.
- Never log connection strings, API tokens, or any credential. Log the host and database name only.
- Every log entry from inside an agent must include `run_id` and `agent` as bound context.
- Exception tracebacks are logged at ERROR level with full context. They are never swallowed.

**Tests for logging:**
- Configure structlog in test mode to capture log output
- Assert that after a successful extract, an `info` entry with `agent="extract"` and a row count exists
- Assert that credentials never appear in any log output (scan log output for known test password strings)
- Assert that a transform failure logs the retry count and error at each attempt

---

## Human-in-the-Loop — Destructive Operation Detection

If Loafer is running a transform that will destructively modify or delete data, the user must explicitly confirm before execution proceeds. This applies to all three transform modes.

**What counts as destructive:**
- Dropping columns that exist in the source schema
- Filtering that removes more than 30% of rows (configurable threshold via `destructive_filter_threshold` in config)
- Any transform that results in zero output rows
- SQL transforms containing `DELETE`, `DROP`, `TRUNCATE`, `UPDATE` — caught by the SQL validator, but also flagged here before that validator even runs
- Custom Python transforms that reference `del`, `pop`, or `.clear()` on the input data (detected via AST scan)
- In ELT mode: `CREATE TABLE ... AS SELECT` that would overwrite an existing table (`write_mode: replace`)

**How detection works:**

Create `loafer/core/destructive.py`:

```python
from dataclasses import dataclass
from enum import Enum

class DestructiveReason(str, Enum):
    COLUMN_DROP = "column_drop"
    HIGH_ROW_FILTER = "high_row_filter"
    ZERO_OUTPUT_ROWS = "zero_output_rows"
    UNSAFE_SQL = "unsafe_sql"
    UNSAFE_CODE = "unsafe_code"
    OVERWRITE_TARGET = "overwrite_target"

@dataclass
class DestructiveWarning:
    reason: DestructiveReason
    detail: str
    severity: str   # "warn" | "block"
```

The detector runs in two phases:

**Static detection (before any execution):**
- For AI mode: after Gemini generates the function, scan it for `del`, `.pop()`, `.clear()`, `.remove()` applied to the input list or its elements. If found, add a `DestructiveWarning` with `severity="warn"`.
- For SQL mode: run the SQL validator. Any disallowed statement adds a `DestructiveWarning` with `severity="block"` before sqlglot even parses deeply.
- For custom mode: scan the loaded file's AST for the same patterns.
- For any mode: if `write_mode: replace` is set on the target and the target already exists, add a `DestructiveWarning` with `severity="warn"`.

**Runtime detection (after transform executes, before load):**
- Compare `len(transformed_data)` to `len(raw_data)`. If more than `destructive_filter_threshold`% of rows were removed, add a `DestructiveWarning` with `severity="warn"`.
- If `len(transformed_data) == 0`, add a `DestructiveWarning` with `severity="block"` (user must explicitly confirm to write zero rows to the target).
- Compare output column names to input column names. Any column present in input but absent in output is flagged as `COLUMN_DROP`.

**Confirmation flow:**

When any `DestructiveWarning` is detected:

```
⚠  Loafer detected potentially destructive operations in this pipeline:

   • Column drop detected: 3 columns present in source will not appear in output
     (dropped: user_password_hash, internal_notes, raw_payload)

   • High row filter: transform removed 68% of rows (source: 42,801 → output: 13,696)
     Threshold is 30%. Set destructive_filter_threshold in config to adjust.

Do you want to continue? [y/N]:
```

Rules for the confirmation flow:
- `severity="block"` items are listed first, `severity="warn"` items second.
- If `--yes` flag is passed to `loafer run`, skip confirmation and proceed. Log that the flag was used.
- If the pipeline is running as a scheduled job (non-interactive), skip confirmation and proceed. Log that auto-confirm was used due to non-interactive mode.
- If the user answers `N` or presses Ctrl+C, exit with code 0 and log `"pipeline cancelled by user at destructive confirmation"`.
- The confirmation prompt is rendered by the CLI layer, not the agent. The agent adds `DestructiveWarning` objects to `PipelineState.destructive_warnings`. The CLI checks this field after the transform agent runs and before invoking the load agent.

Add to `PipelineState`:

```python
destructive_warnings: list[DestructiveWarning]
auto_confirmed: bool
```

**Tests for destructive detection:**
- AI transform that drops a column → `DestructiveWarning` with `COLUMN_DROP` added to state
- Transform that filters 80% of rows → `DestructiveWarning` with `HIGH_ROW_FILTER` added
- Transform that returns 0 rows → `DestructiveWarning` with `ZERO_OUTPUT_ROWS`, `severity="block"`
- Custom Python file containing `.pop()` on input → warning added before execution
- SQL with `DELETE` → `UNSAFE_SQL` warning added before SQL validator runs
- `--yes` flag bypasses prompt → `auto_confirmed = True` in state, no prompt rendered
- Scheduled job → auto-confirms and logs it
- User answers `N` → pipeline exits 0, nothing written

---

## Docker

Docker is required. It serves two purposes: local development with real infrastructure (postgres, mongo) and production deployment of the scheduler.

### `docker/Dockerfile`

```dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY loafer/ ./loafer/

ENTRYPOINT ["uv", "run", "loafer"]
```

The image is for running `loafer` commands only — not for development. Keep it slim.

### `docker/docker-compose.yml`

```yaml
services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: loafer
      POSTGRES_PASSWORD: loafer
      POSTGRES_DB: loafer_dev
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  mongo:
    image: mongo:6
    environment:
      MONGO_INITDB_ROOT_USERNAME: loafer
      MONGO_INITDB_ROOT_PASSWORD: loafer
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db

  loafer:
    build:
      context: ..
      dockerfile: docker/Dockerfile
    env_file: ../.env
    volumes:
      - ../examples:/app/examples
      - loafer_data:/root/.loafer
    depends_on:
      - postgres
      - mongo

volumes:
  postgres_data:
  mongo_data:
  loafer_data:
```

### Usage

```bash
# spin up local dev infrastructure
docker compose -f docker/docker-compose.yml up postgres mongo -d

# run a pipeline inside the container
docker compose -f docker/docker-compose.yml run loafer run examples/etl_postgres_to_csv.yaml

# run the scheduler as a long-running service
docker compose -f docker/docker-compose.yml up loafer
```

### Rules

- The `Dockerfile` never copies `.env` into the image. Secrets are injected at runtime via `env_file` or environment variables.
- The `loafer_data` volume persists scheduled jobs and logs across container restarts. This is non-negotiable for scheduled pipelines.
- Add a `.dockerignore` that excludes: `.env`, `tests/`, `*.pyc`, `__pycache__`, `.git`, `ANTIGRAVITY_PROMPT.md`.
- Integration tests that require postgres or mongo must use `docker compose` to spin up real instances. No SQLite fakes for database integration tests. Add a `pytest` mark `@pytest.mark.integration` and document that integration tests require `docker compose up postgres mongo`.

---

## Phase 7 — Hardening and Edge Cases

**Goal:** Make the system robust for real-world usage. These are the things that break in production.

### Items to address

1. **Graceful shutdown.** `SIGINT` (Ctrl+C) during a pipeline run should: stop extraction, flush any buffered data to the target if possible, print partial results, exit cleanly. Never leave a partial write without logging it.

2. **Config secrets.** Connection strings in YAML often contain passwords. Loafer must support environment variable interpolation in config files:
   ```yaml
   source:
     url: ${DATABASE_URL}
   ```
   Resolve at parse time. Raise `ConfigError` if an interpolated variable is not set.

3. **Connector connection pooling.** For pipelines that process multiple tables or run on a schedule, re-establishing a database connection each time is slow. Connectors should support connection reuse within a single pipeline run.

4. **Large schema samples for wide tables.** If a table has 200 columns, the schema sample sent to Gemini could be large. Cap at 50 columns in the sample. Log a warning that only 50 of N columns were sampled.

5. **LLM output drift.** Sometimes Gemini returns a valid function that subtly changes column names (e.g. lowercases them). After applying the transform, compare output column names to input column names. If columns were renamed unexpectedly, add a warning. Do not fail.

6. **Concurrent pipeline runs.** `loafer run` invoked twice at the same time against the same target file or table. Detect this via a file lock (`~/.loafer/locks/<pipeline_hash>.lock`) and raise `PipelineError` if already running.

7. **Disk space check.** Before writing to a file target, estimate output size (rows * avg row size from schema sample). If estimated size exceeds 90% of available disk space, warn the user before proceeding.

8. **Encoding consistency.** All internal data processing uses UTF-8. Any connector that produces non-UTF-8 data must re-encode before passing to the pipeline. This is a connector responsibility, not an agent responsibility.

9. **Very deeply nested JSON (REST API sources).** Limit nesting depth to 5 levels in the schema sample. Beyond that, represent as "deeply nested object" and note it in the warning.

10. **Gemini context window.** The transform prompt plus code must not exceed Gemini Flash's context window. Prompt builder must estimate token count before sending. If over budget, reduce `max_sample_rows` to 3, then 1, then raise `LLMError` with guidance to simplify the instruction.

---

## Phase 8 — Multi-LLM Abstraction (Scaffold Only for MVP)

**Goal:** The architecture is ready for Claude, OpenAI, and Qwen. No implementations yet — just the wiring.

1. Add `llm_provider` to pipeline config:
   ```yaml
   llm:
     provider: gemini           # gemini | claude | openai | qwen
     model: gemini-1.5-flash    # provider-specific model name
     api_key: ${GEMINI_API_KEY} # resolved from env
   ```

2. Create `loafer/llm/registry.py`:
   ```python
   def get_provider(config: LLMConfig) -> LLMProvider:
       match config.provider:
           case "gemini": return GeminiProvider(config)
           case "claude": raise NotImplementedError("Claude provider coming soon")
           case "openai": raise NotImplementedError("OpenAI provider coming soon")
           case "qwen": raise NotImplementedError("Qwen provider coming soon")
           case _: raise ConfigError(f"Unknown LLM provider: {config.provider}")
   ```

3. All agents receive the provider via `PipelineState` — not via import. This means providers can be swapped per-pipeline in the future.

### Tests for Phase 8

- `get_provider("gemini")` → returns `GeminiProvider`
- `get_provider("claude")` → raises `NotImplementedError`
- `get_provider("unknown")` → raises `ConfigError`

---

## Phase 9 — Full Test Suite Pass

Before marking the MVP complete, the following must all pass with zero failures:

```bash
pytest tests/unit/ -v --cov=loafer --cov-report=term-missing
pytest tests/integration/ -v
pytest tests/e2e/ -v
```

Run them via uv:

```bash
uv run pytest tests/unit/ -v --cov=loafer --cov-report=term-missing
uv run pytest tests/integration/ -v
uv run pytest tests/e2e/ -v
```

Coverage requirements:
- `loafer/llm/` → 95% minimum
- `loafer/agents/` → 95% minimum
- `loafer/connectors/` → 90% minimum
- `loafer/graph/` → 90% minimum
- `loafer/cli.py` → 80% minimum

Linting and types:
```bash
uv run ruff check .
uv run mypy loafer/
```

Both must pass clean. No `type: ignore` comments unless justified in a comment explaining why.

---

## Definition of MVP Complete

The MVP is complete when all of the following are true:

1. `loafer run pipeline.yaml` works end to end for all four source types (postgres, mongo, csv, REST API) to all three target types (csv, json, postgres)
2. ETL and ELT modes both work
3. Streaming mode activates correctly and handles a dataset of 500k rows without OOM
4. The LLM never receives raw data — only schema samples
5. Transform retries work — three failures surface a `TransformError` with all attempts
6. `loafer schedule` persists jobs across restarts
7. All tests pass. Coverage thresholds met.
8. `ruff` and `mypy` pass clean.
9. The run summary output is rendered correctly in the terminal
10. `loafer validate` correctly catches every invalid config before any I/O happens
11. Destructive operations surface a confirmation prompt before any data is written
12. Structured debug logs are written to `~/.loafer/debug.log` on every run
13. `docker compose up postgres mongo` + `loafer run` works end to end in a container
14. No logic is duplicated across two files. Any DRY violation found during review is a blocker.
15. The Ports and Adapters boundary is clean — `loafer/agents/` imports nothing from `loafer/adapters/`

---

## A Note on What You Are Building

Loafer is not a script. It is not a wrapper. It is a production-grade system that will run on real data, in real infrastructure, on a schedule, potentially unattended.

Every agent failure is a user's data pipeline failing silently at 2am. Every token you waste is a cost on someone's API bill. Every unhandled edge case is a corrupted CSV someone has to clean by hand. Every duplicated block of logic is a bug waiting to be fixed in one place but not the other. Every credential that leaks into a log is a security incident.

The architecture pattern exists so that a new contributor can add a DuckDB connector in an afternoon without reading a single agent file. The logging exists so that when a scheduled pipeline fails at 3am you know exactly which row, which column, and which agent caused it.

Build it like you'll be the one on call when it breaks.