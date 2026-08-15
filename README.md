# Loafer

Loafer is an open-source, YAML-first ETL/ELT engine for moving and transforming data from the
command line.

Define a source, transformation, and target; validate the pipeline; then run it locally, in Docker,
or from a scheduler. Transformations can use SQL, custom Python, multi-step pipelines, or optional
LLM-generated artifacts.

> **Project status:** Loafer ships a CLI engine, durable single-node scheduling/recovery, and the
> authenticated multi-tenant `loaferd` HTTPS control plane. The transactional outbox, JetStream
> dispatch, and role-isolated ETL/document/browser worker runtime are implemented; the connected
> web operations dashboard and terminal dashboard remain under active development. The
> `/studio` route is still a product preview.

## What works today

- Declarative ETL and ELT pipelines with Pydantic validation
- PostgreSQL, MySQL, MongoDB, SQLite, CSV, Excel, REST, and PDF sources
- PostgreSQL, MongoDB, CSV, and JSON targets
- SQL, custom Python, AI-generated, and multi-step transforms
- PostgreSQL and MongoDB upserts
- Cursor-based incremental extraction with local state
- Local scheduling, daemon management, run summaries, and logs
- SQLite/PostgreSQL run metadata, fenced worker leases, durable batch checkpoints, replayable
  temporary output, and monotonic run events
- Transactional-outbox JetStream dispatch with role-isolated consumers, coupled lease/ack
  heartbeats, graceful drain, retry, concurrency limits, and poison quarantine
- Better Auth sessions, organizations, invitations, device login, scoped automation keys, and
  short-lived JWT/JWKS exchange through the Next.js authentication boundary
- Stateless `loaferd` `/api/v1` resources and commands with workspace roles, audit events,
  idempotency, SSE reconnect, secret references, OpenAPI, and HTTPS-only clients
- Optional Gemini, OpenAI, Claude, and Qwen providers
- Resource-limited Python transform subprocesses on Linux and macOS
- Declared row-local ETL with bounded batches, per-batch validation, schema policies,
  reconciliation checksums, atomic CSV/JSON publication, and transactional PostgreSQL staging

Bounded execution is opt-in because applying a global transform independently to chunks changes its
meaning. Undeclared transforms and local SQL ETL still use the materialized compatibility path. Do
not assume bounded memory for 30–100M-row jobs until the workload has passed the reproducible
full-pipeline benchmark for its row width and transform class. See
[Production readiness](PRODUCTION_READINESS.md) for the verified limits and release gates.

The `v0.4.0` release baseline's four-column custom identity path completed 1M rows at roughly 1.28
GiB peak process-tree RSS, while a 10M run crossed a 2 GiB safety limit and was terminated without
publishing output. Treat 1M narrow rows as a measured case, not a general guarantee; wider rows,
other transforms, and concurrent runs require their own capped benchmark. The versioned reports and
environment provenance are in
[`benchmarks/results/`](benchmarks/results/README.md).

The declared row-local four-column identity workload has passed a clean production-image 30M-row
gate at 118.23 MiB peak process-tree RSS under a 512 MiB cap, with exact row-count/SHA-256
reconciliation and no temporary output. See
[`30m-row-local.json`](benchmarks/results/30m-row-local.json).

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
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -v "$(pwd):/workspace" \
  -w /workspace \
  ghcr.io/lupppig/loafer:latest run pipeline.yaml --local
```

Mount pipeline files under `/workspace`, not `/app`; `/app` is reserved by the image.
For services, pin `LOAFER_IMAGE` and `LOAFER_WEB_IMAGE` to released digests and use the checked-in
`docker/docker-compose.yml` platform profile. It runs PostgreSQL, explicit one-shot metadata and
storage initialization, `loaferd`, the web and Better Auth boundary, one scheduler, and one durable
worker, each with a non-root, read-only application runtime. Selective `daemon`, `web`, `scheduler`,
and `worker` profiles are also available; `web` brings `loaferd` with it, because the browser
boundary cannot serve without the API it fronts.

Both published ports bind to host loopback and must sit behind a trusted TLS reverse proxy. The
browser reaches the web service over that proxy; the web service reaches `loaferd` across the
private compose network, which is why the profile sets `LOAFERD_TRUST_INTERNAL_NETWORK=1`. That flag
is the browser-side counterpart of `loaferd --behind-tls-proxy`: it permits a plaintext `http://`
hop between two processes you control and forwards the original scheme, and it is never selected
automatically. Set `BETTER_AUTH_DATABASE_URL`; the embedded SQLite fallback is for local development
and a production container refuses to start without it.

## Metadata schema rollout

Prepare durable metadata explicitly before starting `loaferd` or a durable worker. For PostgreSQL,
set the authoritative URL and run the migration as a one-shot deployment job:

```bash
export LOAFER_METADATA_URL="postgresql://loafer:secret@postgres/loafer"
loafer metadata migrate
```

Run the same command without `LOAFER_METADATA_URL` to prepare the embedded SQLite profile. Service
startup never runs DDL: it checks the installed schema version and exits with an actionable error
when migration has not run or the database belongs to a newer Loafer release.

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
loafer run pipeline.yaml --local
```

Loafer infers connector and transform types from URLs, file extensions, and configuration fields.
Use an explicit `type` when inference would be ambiguous.

## Python application interface

The CLI and local scheduler use the same application service available to Python callers:

```python
from loafer.application import RunRequest, get_local_application

service = get_local_application()
result = service.run_pipeline.run(RunRequest(config_path="pipeline.yaml", auto_confirm=True))

print(result.status, result.snapshot.rows_loaded)
```

`RunResult` and streamed `RunEvent` values are JSON-round-trippable, sanitized contracts. They do
not contain source rows, credentials, connectors, iterators, or live LLM provider objects. The
legacy `loafer.runner.run_pipeline()` API remains available as a compatibility facade.

## Bounded row-local execution

Declare `row_local` only when every output row depends on rows in the current batch, such as maps,
filters, normalization, or independent enrichment:

```yaml
mode: etl
chunk_size: 5000

source:
  type: csv
  path: ./input/orders.csv

transform:
  type: custom
  path: ./transforms/normalize_order.py

target:
  type: json
  path: ./output/orders.json
  write_mode: overwrite

execution:
  transform_class: row_local
  schema_drift: fail # fail | evolve | quarantine | coerce
  # quarantine_path: ./output/rejected.json

validation:
  required_columns: [id, amount]
  column_types:
    id: string
  max_null_rate: 0.1
  strict: true
  on_failure: fail # fail | quarantine
```

This path never populates full-run `raw_data` or `transformed_data`. It emits a `BatchEnvelope` for
each batch, validates every row, keeps rolling row/byte/checksum totals, checks cancellation at safe
boundaries, and generates an AI transform artifact once per run before reusing it for every batch.
Rejected rows are written with batch and reason metadata when quarantine is configured.

Current publication guarantees:

| Target | Declared row-local behavior |
|---|---|
| JSON / CSV | Run-scoped temporary file; atomically renamed only after every batch succeeds |
| PostgreSQL `replace` | Hidden run-scoped table; final table replacement occurs in one transaction and deterministic replay replaces it again |
| PostgreSQL `error` | Hidden run-scoped table; create-once rename occurs in one transaction; retry after an ambiguous success requires reconciliation |
| PostgreSQL `append` | Hidden run-scoped table; all rows merge in one transaction; retry after a target-commit/checkpoint gap is at-least-once and can duplicate rows |
| PostgreSQL `upsert` | Hidden run-scoped table; keyed merge occurs in one transaction and deterministic replay is idempotent by the declared key |
| MongoDB | Rejected at config validation until a tested staging/merge publication protocol exists |

SQL is classified as `global_relational`; joins, aggregates, sorts, windows, and large
deduplication must use ELT pushdown or a spill-capable engine rather than per-batch execution.
`loafer validate` exposes the selected delivery guarantee in the execution plan.

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
  model: gemini-3.6-flash
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

## PDF extraction limits

The native PDF source streams page records with file/page provenance and table provenance:

```yaml
source:
  type: pdf
  path: ./documents/report.pdf
  extract_tables: true
  max_pages: 500
  max_file_size_mb: 100
  page_timeout_seconds: 30
  total_timeout_seconds: 300
  page_failure_policy: fail # fail | skip
```

`skip` records a redacted page diagnostic while continuing with later pages. OCR is not
implemented; `ocr_applied` remains `false` in provenance.

## CLI

```text
loafer login --auth-url https://loafer.example.com
loafer enqueue <pipeline.yaml> --command-key <idempotency-key>
loafer run <pipeline.yaml> --local
loafer enqueue <pipeline.yaml> --local --command-key <idempotency-key>
loafer worker [--once] [--role etl|document|browser]
loafer relay [--once]
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

Remote `loafer enqueue` calls `loaferd` over HTTPS and requires `LOAFER_API_URL`,
`LOAFER_AUTH_URL`, and `LOAFER_WORKSPACE_ID`. `loafer login` stores the device-session credential in
the operating-system keyring and exchanges it for short-lived API JWTs. There is no Unix-socket
protocol and no silent local fallback. Use `--local` explicitly for embedded compatibility mode.

`loafer schedule` and the scheduler daemon only enqueue durable run commands; start `loafer worker`
as a separate process to execute them. The embedded profile defaults to SQLite under `~/.loafer`
and supports one scheduler and one worker. Set `LOAFER_METADATA_URL` to a PostgreSQL URL for the
authoritative platform store and `LOAFER_OBJECTS_PATH` to choose the local artifact root.

## Self-hosted platform direction

The production architecture separates clients, control plane, and data plane:

```text
Browser ─ Next.js BFF / Better Auth ─┐
CLI / automation ─ signed JWT ───────┴─ HTTPS `/api/v1` `loaferd`
                                              └─ PostgreSQL/outbox ─ NATS JetStream
                                                                        ├─ ETL workers
                                                                        └─ browser workers
```

The web dashboard and planned terminal dashboard will use the same API, permissions, run events,
metrics, and logs. Workers will run independently so startups can deploy the stack on one host
while larger installations can scale and isolate worker pools.

The control interface, authentication boundary, tenant authorization, durable state, transactional
outbox relay, and role-isolated JetStream workers are implemented. Distributed object storage,
specialized crawl/OCR capabilities, and the connected operator UI are not. Do not expose Studio as
a production operations surface yet.

The planned web source uses Crawlee for Python with HTTP/Parsel and Playwright execution profiles.
It will support bounded crawling, authorized authenticated sessions, JavaScript rendering, and
download/PDF discovery behind isolated workers. This is roadmap architecture, not a shipped
connector.

Planned database expansion treats ClickHouse, MariaDB, TiDB, Tiger Data/TimescaleDB, CouchDB, and
TigerGraph as separately tested connectors or capability profiles. Protocol compatibility with
MySQL or PostgreSQL is not treated as proof of equivalent behavior.

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
- `loafer-document-extraction`
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
- [Changelog](CHANGELOG.md)
- [License](LICENSE)
