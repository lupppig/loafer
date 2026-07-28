# Loafer

Loafer is an open-source, YAML-first ETL/ELT engine for moving and transforming data from the
command line.

Define a source, transformation, and target; validate the pipeline; then run it locally, in Docker,
or from a scheduler. Transformations can use SQL, custom Python, multi-step pipelines, or optional
LLM-generated artifacts.

> **Project status:** Loafer currently ships as a CLI engine with a local scheduler/daemon. The
> multi-tenant API, distributed workers, web operations dashboard, and terminal dashboard are under
> active development. The `/studio` web route is a product preview, not a connected control plane.

## What works today

- Declarative ETL and ELT pipelines with Pydantic validation
- PostgreSQL, MySQL, MongoDB, SQLite, CSV, Excel, REST, and PDF sources
- PostgreSQL, MongoDB, CSV, and JSON targets
- SQL, custom Python, AI-generated, and multi-step transforms
- PostgreSQL and MongoDB upserts
- Cursor-based incremental extraction with local state
- Local scheduling, daemon management, run summaries, and logs
- Optional Gemini, OpenAI, Claude, and Qwen providers
- Resource-limited Python transform subprocesses on Linux and macOS

Source and target connectors process chunks, but some ETL transform paths still materialize a full
run. Do not assume bounded memory for 30–100M-row jobs yet. See
[Production readiness](PRODUCTION_READINESS.md) for the verified limits and release gates.

## Install

Python 3.11 or newer is required.

```bash
pip install loafer-etl
loafer --version
```

Or run the published CLI image:

```bash
docker pull ghcr.io/lupppig/loafer:latest
docker run --rm \
  -v "$(pwd):/workspace" \
  -w /workspace \
  ghcr.io/lupppig/loafer:latest run pipeline.yaml
```

Mount pipeline files under `/workspace`, not `/app`; `/app` is reserved by the image.

## Quick start

Create `pipeline.yaml`:

```yaml
name: daily_orders
mode: etl

source:
  url: ${DATABASE_URL}
  query: SELECT * FROM orders

transform:
  query: |
    SELECT *
    FROM {{source}}
    WHERE status = 'paid'

target:
  path: ./output/orders.json
  write_mode: overwrite

incremental:
  column: updated_at
  initial: "1970-01-01"
```

Run it:

```bash
export DATABASE_URL="postgresql://user:password@localhost/app"
loafer validate pipeline.yaml
loafer run pipeline.yaml
```

Loafer infers connector and transform types from URLs, file extensions, and configuration fields.
Use an explicit `type` when inference would be ambiguous.

## Transform options

### SQL

SQL ETL transforms run through DuckDB and reference the incoming dataset as `{{source}}`.

```yaml
transform:
  query: SELECT id, email FROM {{source}} WHERE email IS NOT NULL
```

### Custom Python

```yaml
transform:
  path: ./transforms/clean.py
```

### AI-assisted

AI is an authoring tool, not the data plane. Loafer sends bounded schema/sample context to the
configured provider, validates the generated artifact, then executes it locally or in the target
engine.

```yaml
transform:
  instruction: Normalize currency to USD and remove cancelled orders

llm:
  provider: gemini
  model: gemini-2.5-flash
  api_key: ${GEMINI_API_KEY}
```

### Multi-step

```yaml
transform:
  - name: tag_active
    path: ./transforms/tag_active.py
  - name: keep_active
    query: SELECT * FROM {{source}} WHERE is_active = true
```

Each step receives the previous step's output. See
[`examples/pipelines/multi_step_transform.yaml`](examples/pipelines/multi_step_transform.yaml).

## CLI

```text
loafer run <pipeline.yaml>
loafer validate <pipeline.yaml>
loafer connectors
loafer schedule <pipeline.yaml>
loafer list-schedules
loafer start
loafer status
loafer logs
loafer stop
loafer init
```

Use `loafer <command> --help` for command-specific options.

## Self-hosted platform direction

The production architecture separates clients, control plane, and data plane:

```text
Next.js web/BFF ─┐
                 ├─ Better Auth ─ control-plane API ─ PostgreSQL/outbox ─ NATS JetStream
CLI / TUI ───────┘                                                        ├─ ETL workers
                                                                          └─ browser workers
```

The web dashboard and planned terminal dashboard will use the same API, permissions, run events,
metrics, and logs. Workers will run independently so startups can deploy the stack on one host
while larger installations can scale and isolate worker pools.

The full stack is not shipped yet. Until the API, durable queue, tenant authorization, worker
leases, and recovery tests exist, use the CLI/Docker path for bounded workloads and do not expose
Studio as a production operations surface.

The planned web source uses Crawlee for Python with HTTP/Parsel and Playwright execution profiles.
It will support bounded crawling, authorized authenticated sessions, JavaScript rendering, and
download/PDF discovery behind isolated workers. This is roadmap architecture, not a shipped
connector.

Deployment targets:

- **Startup:** Docker Compose with separate web/API, scheduler, worker, PostgreSQL, NATS, and
  object-storage services; browser workers are enabled only when needed.
- **Production:** externally managed state services and horizontally scaled API/worker replicas.
- **Enterprise:** SSO, external secret management, audit export, isolated worker pools, policy
  controls, backup/restore, and documented upgrade windows.

Implementation guidance lives in the repository skills:

- `loafer-web-ui`
- `loafer-cli-tui`
- `loafer-auth`
- `loafer-api-design`
- `loafer-engine`
- `loafer-web-scraping`
- `loafer-workers`
- `loafer-self-hosting`

## Development

```bash
git clone https://github.com/lupppig/loafer.git
cd loafer
uv sync

uv run pytest tests/unit -q
uv run ruff check loafer tests

cd web
npm install
npm run dev
npm run lint
npm run typecheck
npm run build
npm start
```

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Changes to data correctness,
security boundaries, connectors, or recovery behavior require tests that exercise the relevant
failure path.

## Open-source readiness

The project uses the MIT license and accepts connector, engine, documentation, UI, deployment, and
reliability contributions. Production and scale claims must be backed by reproducible full-pipeline
tests rather than connector-only benchmarks.

- [GitHub](https://github.com/lupppig/loafer)
- [PyPI](https://pypi.org/project/loafer-etl/)
- [Security policy](SECURITY.md)
- [Code of conduct](CODE_OF_CONDUCT.md)
- [License](LICENSE)
