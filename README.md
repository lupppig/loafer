# Loafer

Loafer is an open-source ETL/ELT engine for defining data pipelines in YAML and running them from
the command line or through a self-hosted control plane.

It exists to make serious data movement understandable: a pipeline says where data comes from,
how it changes, where it goes, and what correctness rules apply. The same definition can run
locally during development or as a durable job handled by independent workers.

Loafer currently supports:

- PostgreSQL, MySQL, MongoDB, SQLite, CSV, Excel, REST, and PDF sources
- PostgreSQL, MongoDB, CSV, and JSON targets
- SQL, custom Python, multi-step, and optional AI-authored transforms
- incremental cursors, validation, schema-drift policies, quarantine, and upserts
- bounded row-local ETL and in-database ELT
- durable schedules, retries, cancellation, checkpoints, and role-isolated workers
- a self-hosted API and authentication boundary

The CLI engine and distributed runtime are implemented. The connected operations dashboard,
distributed object storage, OCR/web-crawl workers, and native multi-pipeline DAGs are still in
development. See [Production readiness](PRODUCTION_READINESS.md) before choosing a workload or
making scale guarantees.

## Quick start

Python 3.11 or newer is required.

```bash
pip install loafer-etl
loafer --version
```

To run the included no-API-key example from a clone:

```bash
git clone https://github.com/lupppig/loafer.git
cd loafer
uv sync

uv run loafer validate examples/pipelines/04_bypass_ai.yaml
uv run loafer run examples/pipelines/04_bypass_ai.yaml --local --yes
```

`--local` is deliberate: local execution must be requested explicitly. Durable jobs are submitted
with `loafer enqueue` and executed by a worker.

## A pipeline at a glance

```yaml
name: daily_orders
mode: etl
chunk_size: 5000

source:
  type: postgres
  url: ${SOURCE_DATABASE_URL}
  query: SELECT * FROM orders

transform:
  type: custom
  path: ./transforms/normalize_order.py

target:
  type: postgres
  url: ${WAREHOUSE_DATABASE_URL}
  table: analytics.orders
  write_mode: upsert
  key: order_id

incremental:
  column: updated_at
  initial: "1970-01-01"

execution:
  transform_class: row_local
  schema_drift: quarantine
  quarantine_path: ./rejected/orders.json

validation:
  required_columns: [order_id, updated_at]
  on_failure: quarantine
```

The important declaration is `transform_class`. Use `row_local` only when each output row depends
on data in its current batch. Joins, aggregates, windows, sorts, and whole-dataset deduplication are
global work and must use ELT pushdown or the materialized compatibility path.

## Architecture

Loafer has one execution engine with two ways into it:

```text
Local development
  CLI ───────────────────────────────────────────────┐
                                                     │
Durable deployment                                  ▼
  Browser → Next.js auth/BFF → loaferd          application service
  CLI/automation ────────────────┘                    │
                                  PostgreSQL          ▼
                              metadata + outbox → relay → NATS JetStream
                                                           │
                                               role-isolated workers
                                                           │
                                      extract → validate → transform → load
                                                           │
                                              sources, targets, artifacts
```

The metadata database is the authority for run state: PostgreSQL in a distributed deployment and
SQLite in the embedded profile. NATS carries small job notifications, not pipeline definitions,
credentials, or row data. A worker resolves the immutable pipeline version and its allowed secret
references only after claiming a fenced lease.

### Architectural decisions

| Decision | Why it matters |
|---|---|
| YAML is the pipeline contract | Pipelines stay reviewable, versionable, and runnable without a UI. Durable runs refer to an immutable snapshot of that contract. |
| ETL and ELT are separate execution paths | Moving rows through Python and transforming inside a database have different safety, memory, and transaction semantics. Hiding that difference creates false guarantees. |
| Transform semantics are explicit | `row_local`, `global_relational`, and `materialized` tell the engine whether batching is correct. A chunk size alone cannot make a global transform stream safely. |
| AI authors artifacts; it is not the data plane | Providers receive bounded schema/sample context. Generated code or SQL is validated, then executed by the normal engine; durable row-local runs also version the artifact. Full datasets are not sent to an LLM. |
| Durable state lives in metadata, not the queue | Runs, stages, leases, checkpoints, events, and retries remain correct when a transport message is duplicated or lost. |
| Dispatch uses a transactional outbox | Creating a run and recording that it needs dispatch happen in one database transaction. The relay can publish again safely. |
| Workers use leases and fencing tokens | A stalled worker cannot resume later and overwrite progress made by its replacement. Queue acknowledgements follow durable run state. |
| Infrastructure sits behind ports | Metadata, queues, object storage, connectors, and runtime services implement narrow interfaces so the engine can be tested without production infrastructure. |
| Secrets stay at process boundaries | Stored pipeline versions keep secret references. The control plane and browser do not expose resolved credentials, and transform subprocesses receive a restricted environment. |
| Schema changes are explicit operations | Services verify the metadata schema at startup; only `loafer metadata migrate` changes it. Startup order cannot silently mutate production data. |

These decisions are constraints, not decoration. A contribution that bypasses them—for example,
putting run state only in NATS or calling an LLM with a full dataset—changes Loafer's reliability
or security model and needs explicit design discussion.

## Execution guarantees

Declared row-local runs keep one bounded batch in flight, validate every batch, record rolling
reconciliation data, and check cancellation at safe boundaries. CSV and JSON publish through a
run-scoped temporary file. PostgreSQL row-local loads use a staging table and a final transaction.

Other paths have narrower guarantees:

- materialized transforms hold the full transformed dataset in memory;
- MongoDB does not yet have the row-local staging protocol;
- append publication can duplicate rows after an ambiguous target commit unless the workload is
  idempotent;
- Python subprocess limits reduce resource risk but are not a complete multi-tenant sandbox;
- cursor polling is supported, but source-native CDC, first-class SCD policies, and native DAGs
  are not.

Measured benchmark artifacts live in [`benchmarks/results/`](benchmarks/results/README.md). A
benchmark result applies only to its recorded pipeline shape, limits, image, and hardware.

## Repository guide

| Path | Responsibility |
|---|---|
| `loafer/core/` | State machines, roles, identifiers, batching rules, and sandbox policy |
| `loafer/application/` | Use cases and composition for local and durable execution |
| `loafer/ports/` | Interfaces the application expects from infrastructure |
| `loafer/adapters/` | PostgreSQL/SQLite metadata, JetStream, object storage, and runtime adapters |
| `loafer/connectors/` | Source and target implementations plus connector registration |
| `loafer/graph/`, `loafer/transform/` | ETL/ELT orchestration and transform runners |
| `loafer/control_plane/` | Authenticated HTTP API, domain services, and repositories |
| `web/` | Next.js site, authentication, and browser-to-API boundary |
| `docker/` | Hardened images and the self-hosted Compose topology |
| `tests/` | Unit, integration, end-to-end, security, and contract tests |

Dependency direction matters: core logic must not import concrete infrastructure. Composition
belongs at the application, CLI, daemon, or deployment boundary.

## Development

```bash
git clone https://github.com/lupppig/loafer.git
cd loafer
uv sync

uv run pytest tests/unit -q
uv run ruff check loafer tests
uv run ruff format --check loafer tests
```

For the web project:

```bash
cd web
npm ci
npm run lint
npm run typecheck
npm run build
```

The rendered demo is committed under `web/public/media`; its source and rendering instructions are
in [`video/`](video/README.md). Deployment starts with [the Compose file](docker/docker-compose.yml)
and [the environment template](.env.example).

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Changes to correctness,
recovery, security boundaries, or connector behavior need tests that exercise the failure path,
not only the happy path.

## Project links

- [Changelog](CHANGELOG.md)
- [Security policy](SECURITY.md)
- [Code of conduct](CODE_OF_CONDUCT.md)
- [PyPI package](https://pypi.org/project/loafer-etl/)
- [MIT license](LICENSE)
