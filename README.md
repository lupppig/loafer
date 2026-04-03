# Loafer

> A CLI-first, declarative ETL/ELT pipeline tool driven by YAML.

## Overview

Modern data pipelines often become bogged down by repetitive boilerplate and fragile scripts. Loafer solves this by treating data movement and transformation as configuration. 

Loafer is a CLI-first tool that allows you to extract data from databases, files, and APIs, apply powerful transformations (both custom and AI-generated), and load the results into target systems — all configured within a single, highly readable YAML file.

**Key Idea:** Describe your pipeline in YAML, run it from the CLI, and let Loafer handle the execution.

## Features

- **Declarative Configuration:** Define your inputs, outputs, and processing logic in simple YAML. No complex workflow orchestrators required.
- **ETL & ELT Support:** Choose between Extract-Transform-Load (in-memory/streaming Python transformations) or Extract-Load-Transform (in-database SQL executions).
- **AI-Powered Transformations:** Optionally use natural language to auto-generate complex Python transforms or target-database SQL using modern LLMs (Gemini, OpenAI, Claude).
- **Streaming & Batch Processing:** Automatically switches to memory-efficient streaming for large datasets based on configurable thresholds.
- **Data Quality Guards:** Built-in validation steps to catch malformed data or high null-rates before they hit your target.
- **Developer-Friendly & Extensible:** Use custom Python files for advanced transformations when declarative logic isn't enough.

## Installation

### Via pip (PyPI)
The easiest way to install Loafer is via pip:
```bash
pip install loafer-etl
```

### Via Docker (GHCR)
Loafer is officially published to the GitHub Container Registry. Available tags include specific versions (e.g. `0.2.0`, `0.2`) and `latest`.

```bash
docker pull ghcr.io/lupppig/loafer:latest
```

To run Loafer using Docker, mount your current working directory so Loafer can access your configuration and local files:
```bash
docker run --rm -v $(pwd):/workspace -w /workspace ghcr.io/lupppig/loafer:latest run pipeline.yaml
```

### From Source
To contribute or use the latest unreleased features:
```bash
git clone https://github.com/lupppig/loafer.git
cd loafer
pip install -e .
```

## Quick Start

Create a single YAML file describing your pipeline. This example extracts orders from PostgreSQL, normalizes the data via AI, and saves it to a clean CSV.

**`pipeline.yaml`**
```yaml
name: Daily Orders Pipeline
mode: etl

source:
  type: postgres
  url: ${DATABASE_URL}
  query: "SELECT * FROM orders WHERE created_at >= NOW() - INTERVAL '1 day'"

target:
  type: csv
  path: ./output/clean_orders.csv
  write_mode: overwrite

transform:
  type: ai
  instruction: >
    Drop cancelled orders, normalize currency to USD, and 
    combine first_name and last_name into full_name.

llm:
  # Supported providers: gemini, openai, claude, qwen
  provider: gemini
  model: gemini-2.5-flash
  api_key: ${GEMINI_API_KEY}
```

Run the pipeline from your terminal:
```bash
export DATABASE_URL="postgresql://user:pass@localhost/db"
export GEMINI_API_KEY="your-api-key"

loafer run pipeline.yaml
```

**Expected Outcome:** 
Loafer will connect to the Postgres database, extract the past day's orders, execute the transformation to clean the data, and successfully write the normalized output to `./output/clean_orders.csv`.

## How It Works

Loafer pipelines consist of three main stages, executed as a directed graph:

1. **Extract:** Loafer connects to your source (e.g., a SQL database, CSV, or REST API) and pulls the data. Large datasets are automatically chunked and streamed to prevent memory exhaustion.
2. **Transform:** The data is manipulated according to your YAML instructions. This can be AI-generated Python code running in a safe isolated context, a custom Python script you provide, or skipped entirely for simple EL (Extract-Load) operations.
3. **Load:** The transformed data is pushed to your designated target connector (e.g. CSV, Snowflake, Postgres) adhering to your specified write mode (append, overwrite).

In **ELT mode**, the steps differ slightly: data is first loaded raw into the target database, and transformations are executed as native SQL queries against the target engine.

## CLI Usage

Loafer provides a streamlined CLI tailored for day-to-day data engineering.

**Command Syntax:**
```bash
loafer <command> [options]
```

**Common Commands:**
- `loafer run <config.yaml>`: Execute a pipeline defined in the given YAML file.
  - `--dry-run`: Extracts and transforms data without writing to the target.
  - `--verbose`: Prints detailed execution logs and agent outputs.
- `loafer validate <config.yaml>`: Check a configuration file for syntax and connection errors without running the pipeline.
- `loafer connectors`: List all available source and target connectors.

## Project Structure

```text
loafer/
├── cli.py             # CLI entrypoints and command routing
├── config.py          # Configuration parsing and validation
├── runner.py          # Pipeline execution logic and LangGraph orchestration
├── connectors/        # Integrations (sources and targets)
│   ├── sources/          # Postgres, CSV, REST API, Excel, etc.
│   └── targets/          # Postgres, CSV, Snowflake, etc.
├── transform/         # Transformation engines (AI, custom, SQL)
├── graph/             # LangGraph state management and pipeline DAGs
├── llm/               # LLM provider integrations (Gemini, Claude, OpenAI)
└── agents/            # Individual workflow nodes (extract, transform, load)
```

## Configuration (YAML)

Loafer pipelines are driven by a single YAML configuration file. Here is the structure:

```yaml
# Pipeline metadata
name: User Sync Pipeline
mode: etl  # Supports 'etl' or 'elt'

# Extract configuration
source:
  type: rest_api
  url: "https://api.example.com/users"
  method: GET

# Load configuration
target:
  type: postgres
  url: ${TARGET_DB_URL}
  table: users_dim

# Transform logic
transform:
  type: custom
  path: ./transforms/clean_users.py

# Optional: LLM Configuration (required if using type: ai or mode: elt)
llm:
  # Supported providers: gemini, openai, claude, qwen
  provider: gemini
  model: gemini-2.5-flash
  api_key: ${GEMINI_API_KEY}

# Performance & Validation
chunk_size: 1000
streaming_threshold: 10000

validation:
  strict: true
  max_null_rate: 0.1
```

## Development

Setting up Loafer for local development requires `uv` (or standard `pip` / `hatch`).

1. **Clone and Install dependencies:**
   ```bash
   git clone https://github.com/yourusername/loafer.git
   cd loafer
   # If using uv for fast dependencies:
   uv sync
   # Or using standard pip:
   pip install -e ".[dev]"
   ```

2. **Run Tests:**
   Loafer uses `pytest` for unit and integration testing.
   ```bash
   pytest
   ```

3. **Code Style:**
   This project uses standard Python linting and formatting tools. We recommend running `ruff` prior to committing:
   ```bash
   ruff check .
   ruff format .
   ```

## Contributing

Contributions are highly encouraged! Whether it’s a new data connector, a bug fix, or an improvement to the documentation, we'd love your help.

1. Fork the repository.
2. Create a new branch for your feature (`git checkout -b feature/amazing-connector`).
3. Commit your changes with clear messages.
4. Open a Pull Request against the `main` branch.

Keep things simple and ensure any new features include appropriate tests.

## Links

- **GitHub Repository**: [https://github.com/lupppig/loafer](https://github.com/lupppig/loafer)
- **PyPI Package**: [https://pypi.org/project/loafer-etl/](https://pypi.org/project/loafer-etl/)

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
