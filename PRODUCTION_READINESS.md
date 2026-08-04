# Loafer production-readiness assessment

## Executive decision

Loafer is a credible, well-tested MVP for small and moderate single-node ETL/ELT jobs. It is not
yet safe to claim production-grade execution for 30–100M+ row ETL workloads.

The main reason is architectural rather than compute capacity: source connectors yield chunks, but
all ETL transform modes materialize those chunks into a single Python list. The target load then
chunks that already-materialized list. Memory therefore grows with the full transformed dataset.

The honest public positioning today is:

> An open-source, CLI-first AI-assisted ETL/ELT engine with connector-level chunking, sandboxed
> transforms, incremental cursors, and deterministic SQL/Python options. Large-scale, resumable
> end-to-end streaming is under active development.

## What the project is accomplishing

Loafer turns a compact YAML definition into a complete data movement and transformation run:

```text
source → extraction → schema-aware validation → transformation → target
```

It serves four audiences through one engine:

- Data engineers define repeatable YAML pipelines, schedules, incremental cursors, and write modes.
- Data analysts use SQL or plain-English transformations without building orchestration boilerplate.
- Data scientists use custom Python and schema/sample exploration.
- Backend engineers use a CLI-friendly, adapter-based system that can be embedded behind an API.

AI is not the data plane. It generates a transform artifact from a bounded schema sample; the
artifact executes against data. This is the right product boundary and should remain non-negotiable.

## Current architecture

| Layer | Current responsibility |
|---|---|
| Configuration | Pydantic models, YAML parsing, environment substitution, type inference |
| Composition | Runner creates provider state and invokes separate ETL/ELT graphs |
| Orchestration | LangGraph agents for extract, validate, transform, and load |
| Transforms | AI-generated Python, custom Python, DuckDB SQL, multi-step chains |
| Connectors | Ports with PostgreSQL, MySQL, MongoDB, REST, file, SQLite, and PDF adapters |
| Safety | SQL/code validation, destructive-operation confirmation, subprocess resource limits |
| Operations | Rich/Typer CLI, APScheduler daemon, local run and watermark state |
| Web | Next.js App Router marketing and MDX documentation site plus a control-plane product preview |

This separation is strong enough to evolve without a rewrite, but several boundaries must tighten:

- Agents currently resolve adapters through the concrete registry.
- `PipelineState` mixes configuration, live iterators, connector/provider objects, data, and run
  metadata, so it cannot serve as durable workflow state.
- “Streaming” is a source/target adapter feature rather than an end-to-end execution contract.

## Verified strengths

- The repository suite contains 673 passing tests with 50 skips at the Phase 0 closeout.
- Clean-room wheel and production Docker-image smoke tests execute version/help, connector
  discovery, configuration validation, and a real pipeline successfully.
- ETL and ELT are modeled as separate graphs.
- Source adapters expose a chunk iterator and database sources use streaming cursors.
- LLM prompts receive a schema sample rather than a full dataset.
- Multiple LLM providers implement a common interface.
- Custom and generated Python run in a separate process with CPU/memory/time limits on POSIX.
- Incremental extraction advances its cursor after a completed run.
- PostgreSQL and MongoDB support upsert modes.
- CSV and JSON targets publish same-directory temporary files atomically after successful
  finalization and preserve the previous output on failure.
- PostgreSQL target and ELT identifiers use driver-native composition; incremental cursor
  identifiers escape their engine-specific quote character.
- The CLI exposes stage-aware Rich output and human-readable errors.
- The website production build completes successfully.

These strengths justify shipping an early release for bounded workloads with a documented
operational envelope.

## Release blockers for 30–100M+ ETL

### P0 — End-to-end materialization outside the declared row-local path

`loafer/transform/__init__.py::materialize_input_rows` drains every source chunk into one list.
Undeclared/materialized AI, custom Python, SQL, and multi-step ETL runners still use it.
`loafer/agents/load.py` then writes from `transformed_data`, which is also a full-run list. Declared
`row_local` custom, AI, and custom/AI pipelines bypass this graph and keep one bounded batch in
flight.

Impact:

- Peak memory scales with all input rows plus copies/snapshots and all output rows.
- The sandbox duplicates or serializes the full dataset into a child process.
- A 30M-row run can require tens of gigabytes even for narrow rows.
- The 512MB sandbox default conflicts with large materialized transforms.

Required correction:

- Introduce a batch envelope and `TransformRunner.transform_batch()` contract for row-local work.
- Keep `Iterator[Batch]` flowing from extract through validate and transform into load.
- Remove `raw_data`/`transformed_data` from the high-volume path.
- Generate AI code once per run, validate/version it once, execute it per batch.
- Route global relational work to in-target SQL or a spill-capable execution engine.

### P0 — No durable checkpoint/resume protocol

The graph is invoked in-process and state contains live iterators, providers, and connectors. The
incremental cursor is a local JSON file beside the YAML configuration.

Impact:

- A worker or host failure restarts the run from the beginning.
- Concurrent schedules can race on local state.
- The UI cannot reliably reconnect to or control a run.
- There is no partition lease, heartbeat, cancellation, or failed-batch retry.

Required correction:

- Persist run, stage, partition, batch, event, artifact, and checkpoint records in a transactional
  metadata database.
- Store only serializable control-plane state.
- Give workers leases and cooperative cancellation at batch boundaries.
- Advance checkpoints only after the corresponding target effect is durable.

### P0 — Database partial outputs outside staged targets can appear final

CSV and JSON targets now publish atomically and discard unpublished temporary files on failure.
Declared row-local PostgreSQL runs now commit batches only to a hidden run-scoped table and publish
with one final transaction. The legacy direct PostgreSQL adapter still commits every small insert
batch, and MongoDB has no equivalent staging protocol.

Impact:

- Failed database runs leave partially loaded rows.
- A retry may duplicate data in append mode.
- “Pipeline failed” does not mean “target unchanged.”

Required correction:

- Use staging tables plus transactional merge/swap for replace/upsert flows.
- Define per-target guarantees: at-most-once, at-least-once with idempotency, or exactly-once effect.
- Expose partial-write state and recovery instructions.

### Phase 0 completed — SQL identifier composition

PostgreSQL target table, schema, column, conflict-key, and index identifiers now use
`psycopg2.sql.Identifier`/`SQL` composition. ELT create/drop/count paths use the same composition,
configured table names are parsed as `name` or `schema.name`, and incremental cursor identifiers
escape PostgreSQL/SQLite double quotes or MySQL backticks.

Verified behavior:

- schema-qualified table existence checks bind schema and table separately;
- adversarial table, column, conflict-key, CTAS, and cursor identifiers remain quoted identifiers;
- ELT replace/drop/create/count executes in one transaction and rolls back on PostgreSQL errors.

Keep live PostgreSQL integration coverage in the release matrix so driver/server behavior remains
verified in addition to unit-level composition tests.

### P0 — Scale claims are not measured end to end

The opt-in benchmarks measure individual CSV sources, CSV/JSON targets, and schema sampling. The
stress script creates million-row pipelines but does not establish a reliable child-process peak
RSS bound, and its identity transform currently materializes the dataset.

Required correction:

- Benchmark 1M, 10M, 30M, and 100M complete pipelines.
- Record peak RSS, throughput, CPU, disk/spill, bytes, checkpoint/recovery time, and correctness.
- Test worker kill, source disconnect, target disconnect, disk full, schema drift, cancellation,
  and retries.
- Publish hardware/container/dependency versions and the supported workload envelope.

Use the resource-capped full-pipeline harness rather than the legacy parent-process RSS estimate:

```bash
uv run python benchmarks/full_pipeline.py \
  --rows 1000000 \
  --rss-limit-mb 2048 \
  --report benchmarks/results/1m.json

uv run python benchmarks/full_pipeline.py \
  --rows 10000000 \
  --rss-limit-mb 2048 \
  --report benchmarks/results/10m.json

uv run python benchmarks/full_pipeline.py \
  --rows 30000000 \
  --chunk-size 10000 \
  --rss-limit-mb 512 \
  --sandbox-memory-mb 256 \
  --timeout-seconds 3600 \
  --report benchmarks/results/30m-row-local.json
```

The harness generates deterministic input, executes the real CLI and transform subprocess, samples
the complete Linux process group, enforces RSS/time limits, and verifies output row count and
SHA-256 equality. A limit-triggered result is evidence of an unsupported workload, not a benchmark
success.

Release baseline measured on 2026-07-30 from the clean `v0.4.0` revision
`9007746634da06b77fea5d6246bb35c3dff2a978`. The production Dockerfile was built into image
`sha256:0efbede1d2ef2a043a29ef1bdd12f9cbd46ff3d91793e63ae314633f82d4aafe`, running Python
3.11.15 on Linux x86-64. The workload used chunk size 1,000, a four-column deterministic CSV, and
the current custom identity transform:

| Requested rows | Pipeline wall time | Peak process-tree RSS | Result | Artifact |
|---:|---:|---:|---|---|
| 1,000,000 | 13.36s | 1,310.1 MiB | exact row count/SHA-256 | [`1m.json`](benchmarks/results/1m.json) |
| 10,000,000 | 19.54s before cutoff | 2,056.7 MiB | terminated at 2 GiB; no final/temp output | [`10m.json`](benchmarks/results/10m.json) |
| 30,000,000 row-local | 2,028.26s | 118.23 MiB | exact row count/SHA-256; clean production image | [`30m-row-local.json`](benchmarks/results/30m-row-local.json) |

Input generation time is excluded from pipeline wall time. These results demonstrate full-run
materialization in the Phase 0 path and bounded memory in the declared Phase 2 row-local path.
Environment, limits, provenance caveats, and interpretation are recorded with the
[versioned benchmark artifacts](benchmarks/results/README.md).

## High-priority production gaps

### P1 — Data quality is complete only on the declared row-local path

Declared row-local runs validate every batch and aggregate column/null/rejected metrics with
quarantine output. The legacy materialized graph remains primarily schema-sample based, and neither
path yet provides declared uniqueness or referential-integrity checks.

Extend the same contract to global/materialized plans and add uniqueness and referential-integrity
policies where their storage requirements are explicit.

### P1 — Schema drift policy remains implicit outside row-local execution

Declared row-local runs expose schema versions and `fail | evolve | quarantine | coerce` policy.
Legacy target behavior is still inferred from the first row/chunk, so later columns and type
changes can fail or be silently coerced depending on the adapter.

Extend the declared schema contract to global/materialized execution and add compatibility tests
for representative wide and nested records.

### P1 — The sandbox is a resource limiter, not a complete isolation boundary

The subprocess inherits the host environment and is not a container/microVM security boundary.
Windows falls back to an in-process thread whose timeout cannot terminate the transform.

For a hosted multi-tenant product, execute untrusted transforms in network-disabled containers or
microVMs with read-only filesystems, UID isolation, syscall restrictions, and output limits.

### P1 — Operations are local-machine oriented

APScheduler, local daemon state, filesystem logs, and JSON cursor files are appropriate for the CLI
MVP. They are not a distributed scheduler or a multi-user control plane.

Add a server-side API, authenticated workspaces, durable run metadata, workers, secret references,
audit logs, and a standard telemetry pipeline.

### P1 — Advanced orchestration remains a roadmap

Whole-database inspection/orchestration, dependency graphs, cross-table joins, parallel table
execution, and interactive explore mode are product goals but are not present in the current
codebase. Treat them as roadmap items until implementation and tests exist.

## Product UI direction

The UI should be a control plane over the same engine, never a browser-side fork of the engine.

The included `/studio` preview demonstrates the intended shape:

- Pipeline graph plus YAML editing rather than a canvas-only builder.
- Source, transformation, and target configuration in one workspace.
- A bounded dry-run/preview that is visibly distinct from production execution.
- Versioned generated artifacts and explicit execution contracts.
- Run rows/bytes, status, duration, warnings, and rejected data.
- Persona-friendly views for engineers, analysts, scientists, and backend users.
- Honest labels for sample telemetry and the unconnected runtime API.

The minimum backend required before this can be a production feature:

```text
authenticated UI
  → control-plane API
  → config/version/run metadata database
  → durable queue and workers
  → data-plane engine
  → append-only run events
  → SSE/WebSocket event stream back to UI
```

Secrets stay server-side and the browser sees connection references only.

## Phased implementation roadmap

These phases are dependency order, not calendar promises. Keep `main` releasable after every phase,
and do not build a dependent runtime surface on a mocked contract. Marketing and design work may
run ahead using clearly labeled fixtures, but production execution must follow this critical path:

```text
baseline
  → engine/application boundaries
  → bounded and correct data plane
  → durable metadata and single-node recovery
  → authenticated multi-tenant API
  → distributed workers
  → connected Web/CLI/TUI
  → advanced ingestion and connectors
  → scale, operations, and enterprise hardening
```

### Phase 0 — Stabilize the public alpha

**Goal:** ship the current CLI honestly while removing correctness defects that would corrupt or
partially publish data.

Deliver:

- keep the CLI, wheel, Docker image, unit suite, and clean-room artifact smoke tests green;
- document the supported connector/transform matrix and a conservative workload envelope;
- replace unsafe SQL identifier interpolation with adapter-native identifier composition;
- write file outputs to run-scoped temporary paths and publish atomically;
- expose partial database-write behavior until staging protocols replace it;
- add reproducible 1M and 10M full-pipeline correctness/RSS benchmarks;
- keep `/studio` and all synthetic metrics visibly labeled as previews.

Exit gate:

- built wheel and image execute the smoke pipeline;
- failure never leaves a file at its final path;
- adversarial SQL identifier tests pass;
- benchmark results and current limitations are published.

**Current status:** complete. Identifier composition and atomic CSV/JSON publication are covered by
adversarial/failure tests; 673 repository tests pass with 50 skips; and the clean-room wheel and
production Docker-image smoke pipelines pass. The resource-capped 1M/10M matrix was rerun from the
clean, tagged `v0.4.0` revision in its production image: 1M narrow identity rows complete at about
1.28 GiB while 10M exceeds a 2 GiB process-tree budget without publishing final or temporary
output. The versioned reports and environment provenance are published under
[`benchmarks/results/`](benchmarks/results/README.md).

### Phase 1 — Separate engine, application, and clients

**Goal:** make the CLI, future API, scheduler, and workers call the same application boundary
without importing one another.

Deliver:

- define serializable plan, batch, run-result, event, cancellation, checkpoint, and secret ports;
- move run/validate/list-connector use cases out of Typer commands and `runner.py` orchestration;
- keep the engine free of Typer, Rich, React, HTTP, tenant-session, and queue concerns;
- make the CLI a thin local client of the application service;
- remove live connectors, iterators, and LLM providers from any state intended for persistence;
- preserve current YAML and CLI behavior with compatibility tests.

Exit gate:

- one end-to-end pipeline executes through the new application interface;
- the CLI fixtures produce equivalent results and errors;
- import-boundary tests prevent engine-to-client/API dependencies;
- all durable contract types serialize and round-trip.

**Current status:** complete. `ExecutionPlan`, `BatchEnvelope`, `Checkpoint`, `RunEvent`,
`RunSnapshot`, and `RunResult` are strict JSON-round-trippable contracts; cancellation,
checkpoint, secret, event, and generated-code review behavior is expressed through ports. The
`RunPipeline` application use case now owns plan/run orchestration, while `loafer/engine.py` owns
the in-process ETL/ELT graph and `runner.py` is a compatibility facade. The CLI and local scheduler
call the same application service. A real CSV → custom transform → JSON pipeline passes through
that interface, import tests keep client frameworks out of the engine, and the repository suite is
green with 683 passed and 50 skipped.

### Phase 2 — Build the bounded, correct data plane

**Goal:** make memory and publication behavior a property of the execution contract rather than a
connector implementation detail.

Deliver:

- introduce the batch envelope and `transform_batch` path for row-local transforms;
- keep bounded batches flowing extract → validate → transform → stage/load;
- generate/version AI transform artifacts once per run, not per batch;
- classify global joins, sorts, windows, aggregates, and large deduplication for pushdown or a
  spill-capable engine;
- validate every batch and aggregate quality metrics with reject/quarantine output;
- add schema versions and explicit `fail | evolve | quarantine | coerce` drift policies;
- add target staging/merge/swap protocols and documented delivery guarantees;
- harden the existing PDF source for bounded page batches, text/table provenance, file/page/time
  limits, and explicit page failure policy; continue to label OCR as unimplemented.

Exit gate:

- a 30M-row row-local pipeline stays within a documented fixed RSS bound;
- input/output/rejected counts and checksums reconcile;
- cancellation and target failure do not publish a false success or final partial output;
- native PDF text/table fixtures prove page provenance, limits, and failure reporting.

**Current status:** implementation and clean production-image verification complete. Declared
row-local custom, AI, and custom/AI pipeline transforms now use `BatchEnvelope` units without
populating full-run `raw_data` or `transformed_data`; AI artifacts are generated and versioned once
per run. Every batch receives
schema-drift and validation policy, quarantine metadata, row/byte/checksum reconciliation,
cooperative cancellation, and a final checkpoint only after atomic CSV/JSON publication or a
PostgreSQL staging-table transaction. PostgreSQL replace/create-once/append/upsert modes now hide
all batches until final swap or merge; live-database tests cover atomic visibility, failure cleanup,
append, and idempotent keyed upsert. Target and transform failure tests prove that existing output
is preserved and temporary output is discarded. Native PDF fixtures cover text, ruled tables,
provenance, file/page limits, enforced page timeout, and fail/skip reporting; OCR remains explicitly
unimplemented.

The production-image exit run processed 30,000,000 deterministic rows in 2,028.26 seconds with
118.23 MiB peak process-tree RSS under a 512 MiB cap, exact input/output row and SHA-256
reconciliation, atomic publication, and no temporary output. Its
[`30m-row-local.json`](benchmarks/results/30m-row-local.json) report pins source revision
`b2d474b`, image ID `sha256:70d60d4c…`, Python 3.11.15, container limits, and disk-backed storage.
MongoDB remains intentionally rejected by the row-local path until it implements staging/merge
publication rather than partial direct batch effects. PostgreSQL append is at-least-once across an
ambiguous target-commit/checkpoint gap; deterministic keyed upsert is replay-safe. Local/global SQL
remains classified as global relational work and is not presented as bounded row-local execution.

### Phase 3 — Add durable metadata and single-node recovery

**Goal:** make runs observable and resumable before adding distributed transport.

Deliver:

- persist immutable pipeline versions, runs, stages, partitions, batches, events, artifacts,
  checkpoints, schedules, and outbox records;
- implement explicit run/stage/batch state machines with monotonic event sequences;
- separate the scheduler process from execution and make commands idempotent;
- add leases, fencing tokens, heartbeats, cooperative cancellation, and retry categories;
- add an object-storage port for logs, documents, generated artifacts, and temporary outputs;
- use PostgreSQL as the authoritative platform store;
- optionally provide SQLite behind the same repository port for local/embedded, single-process
  development with one scheduler/worker and no HA or NATS claims.

Exit gate:

- killing the single worker at each commit boundary resumes from the last durable checkpoint;
- stale fencing tokens cannot update a run;
- migrations work from an empty database and the previous supported schema;
- SQLite and PostgreSQL contract tests pass for the capabilities each profile advertises.

**Current status:** implementation and single-node recovery verification complete for declared
row-local, single-partition runs. Versioned migrations persist immutable pipeline versions, runs,
stages, partitions, batches, events, artifacts, checkpoints, schedules, and outbox records behind
one SQLAlchemy metadata interface. PostgreSQL uses row locks for concurrent claims and is the
authoritative platform profile; SQLite is explicitly restricted to one scheduler and one worker
without HA or NATS claims. Run, stage, and batch state machines reject impossible transitions,
events allocate contiguous per-run sequences, run creation and schedule firing are idempotent, and
every worker mutation requires its active lease and fencing token.

The bounded data plane now stages transformed batches in object storage before atomically
committing batch metadata, checkpoint, event, and outbox state. Worker-kill tests terminate after
each of three batch commit boundaries and prove that a newly fenced worker replays committed
artifacts, skips the durable source offset, and publishes the exact output. Empty-database,
previous-schema upgrade, rollback/re-apply, constraint, cancellation, schedule, and stale-token
contracts pass on SQLite; the migration, event, claim, and fencing contracts also pass against a
live PostgreSQL 16 database. Recovery remains limited to stable offset-ordered, row-local,
single-partition execution. Materialized/global transforms still restart as whole runs, and the
bundled filesystem object adapter is not a distributed object store.

### Phase 4 — Build authentication, tenancy, and the control-plane API

**Goal:** expose safe multi-tenant application use cases without running data work in HTTP
processes.

Deliver:

- integrate Better Auth for users, sessions, organizations, invitations, and bootstrap admin;
- model Loafer workspaces, environments, permissions, connections, secret references, and audit
  events separately from Better Auth;
- enforce organization/workspace scope in policies, repositories, constraints, and PostgreSQL RLS
  defense in depth;
- publish `/api/v1` OpenAPI and generated clients;
- ship read-only pipeline/run/event/log APIs first, then idempotent validate, create-run, cancel,
  retry, backfill, connection-test, and schedule commands;
- stream persisted run events with SSE sequence, reconnect, heartbeat, and gap behavior;
- support browser sessions, CLI device login, and scoped automation credentials.

Exit gate:

- the authorization matrix and cross-tenant guessed-ID tests pass;
- CSRF, origin, session rotation/revocation, token expiry, and rate-limit tests pass;
- no response, log, event, or OpenAPI schema exposes secret values;
- HTTP requests enqueue/use application commands and never execute pipelines inline.

**Current status:** the Phase 4 control-plane baseline is implemented. Better Auth 1.6.25 owns
users, verified sessions, organizations, invitations, platform-admin bootstrap, device login,
scoped expiring automation keys, and Ed25519 JWT/JWKS issuance. The Next.js BFF validates browser
sessions and API keys before minting a 15-minute audience-bound token; `loaferd` validates that
token over HTTPS and never accepts unsigned tenant headers or cookie-authenticated mutations.

Metadata schema v3 adds Loafer-owned workspaces, environments, permissions, connections with
opaque secret references, durable control commands, and audit events. Every resource repository
query carries an explicit workspace predicate, guessed cross-tenant IDs return 404, role policy is
enforced in the application service, and PostgreSQL installs workspace RLS policies as deployment
defense in depth. The versioned API exposes pipelines, runs, logs/events, connections, schedules,
validate, create-run, cancel, retry, backfill, and connection-test commands. Run creation only
persists queued worker work; the HTTP process does not invoke a pipeline.

The OpenAPI document and generated browser types are checked into source and regenerated in CI.
The Python CLI and web application use typed HTTPS clients over the same `/api/v1` contract, SSE
supports sequence IDs, reconnect, heartbeats, and gap notices, and remote CLI credentials are kept
in the operating-system keyring. Contract tests cover the authorization matrix, guessed IDs,
origin/cookie boundaries, token audience/expiry, rate limits, idempotency, secret redaction, and
HTTPS-only clients. Better Auth integration tests cover disabled public signup, trusted origins,
secure cookies, session replacement/revocation, and credential rate limiting. Metadata v3 and its
RLS policies pass against live PostgreSQL 16.

Distributed consumers for validation, backfill, and connection-test control commands remain Phase
5 work. Those endpoints durably accept work now but do not execute it inside `loaferd`. Production
deployments use PostgreSQL and a non-owner application role configured for RLS; the built-in
SQLite auth/metadata profiles remain development and single-node options.

### Phase 5 — Introduce distributed workers and NATS JetStream

**Goal:** scale and isolate execution without making the queue the source of truth.

Deliver:

- publish opaque job IDs from the PostgreSQL transactional outbox;
- use JetStream durable pull consumers with explicit acknowledgements behind a queue port;
- run scheduler, ETL workers, document workers, and browser workers as separate roles/pools;
- enforce leases, fencing, heartbeats, tenant/environment concurrency, backpressure, retry, and
  poison-job quarantine;
- give jobs short-lived least-privilege secret access and isolated transform sandboxes;
- keep local CLI/embedded execution available without NATS.

Exit gate:

- duplicate delivery, lost acknowledgement, queue restart, metadata outage, worker kill, and
  graceful drain tests produce no silent loss;
- browser/document load cannot starve ordinary ETL consumers;
- stale workers cannot publish checkpoints or terminal states;
- queue messages contain no configs, credentials, rows, HTML, PDFs, or browser state.

### Phase 6 — Connect Web, CLI, and professional TUI

**Goal:** make every client a useful view over the same authenticated API and event model.

Deliver:

- turn the Next.js Studio preview into an organization/workspace-scoped application;
- add pipeline authoring with guided, YAML, and lineage views plus bounded asynchronous previews;
- add connections, environments, schedules, runs, backfills, quality, artifacts, members, and
  settings;
- render extract → validate → transform → load → verify from real sequenced events;
- provide detailed searchable redacted logs, trace correlation, metrics, retries, and recovery
  actions;
- make CLI choose explicit local or API mode through one client contract;
- add a keyboard-first TUI dashboard using the same run/event APIs;
- evolve the public landing page with the artsy infrastructure/3D direction, static fallbacks,
  reduced motion, and honest capability labels.

Exit gate:

- author → validate → run → observe → cancel/retry works end to end in Web, CLI, and TUI;
- tenant switching and permission-denied states are tested;
- SSE reconnect/gap recovery is correct;
- accessibility, reduced-motion, WebGL-unavailable, empty, disconnected, and failure states pass.

### Phase 7 — Add advanced documents and web crawling

**Goal:** deliver sophisticated ingestion through isolated, versioned source capabilities.

Deliver:

- implement the `$loafer-document-extraction` contract for uploaded and crawler-discovered files;
- add native-first automatic PDF extraction with page-level OCR fallback for scanned/mixed files;
- package OCR separately, keep providers behind a port, and report extraction quality explicitly;
- add document upload/artifact references, password secret references, preview, quarantine, and
  reprocessing workflows;
- integrate Crawlee for Python with HTTP/Parsel and Playwright execution profiles;
- add bounded crawl scope, authentication profiles, frontiers, canonicalization, politeness,
  anti-SSRF controls, checkpoints, and download discovery;
- route downloaded PDFs into document workers while preserving crawl provenance.

Exit gate:

- the versioned document corpus passes native text, tables, scanned, mixed, encrypted, malformed,
  oversized, cancellation, retry, and isolation cases;
- controlled crawl fixtures pass static, JavaScript, login expiry, pagination, infinite scroll,
  redirects, deduplication, downloaded PDF, resume, and SSRF cases;
- the UI never parses documents or runs browsers in the Next.js process.

### Phase 8 — Expand database connectors

**Goal:** add connectors only through the stable capability and batch contracts.

Deliver in this order unless customer evidence changes priority:

1. ClickHouse source/target with native bulk paths and pushdown.
2. MariaDB compatibility as an explicitly tested connector/profile rather than an unverified MySQL
   alias.
3. TiDB as a separately tested MySQL-protocol connector/profile with TiDB-specific transaction,
   DDL, type, distributed execution, and retry behavior.
4. Tiger Data—Tiger Cloud and self-hosted TimescaleDB—as a separately tested PostgreSQL-protocol
   connector/profile with hypertable, time-partition, compression, retention, and bulk-I/O
   capabilities.
5. CouchDB source/target with bookmark-based pagination, revision/conflict semantics, and bulk APIs.
6. TigerGraph as a graph-specific source/target with explicit vertex, edge, schema, query, and
   loading-job contracts.

For every connector, publish discovery, partitioning, incremental cursor, pushdown, bulk load,
staging, merge/upsert, schema drift, and delivery capabilities. Do not expose unsupported UI
options.

Exit gate:

- pinned live-service integration and failure tests pass;
- MariaDB, TiDB, and Tiger Data pass compatibility suites separate from MySQL and PostgreSQL;
- TigerGraph vertex/edge mapping, loading-job, query, retry, and partial-failure semantics are
  tested independently from relational connectors;
- secrets are redacted and identifiers/values are composed safely;
- retry, checkpoint, schema drift, and partial-publication behavior is documented;
- bulk connectors pass the relevant 10M/30M full-pipeline matrix before scale claims.

### Phase 9 — Self-hosting, scale, and enterprise hardening

**Goal:** provide one operable architecture from startup Compose to production clusters.

Deliver:

- ship pinned non-root images and a one-command Compose profile for web/API, PostgreSQL, NATS,
  scheduler, worker, and object storage;
- add health/readiness, migrations, first-admin bootstrap, secure defaults, resource profiles, and
  backup/restore;
- add OpenTelemetry metrics/traces/logs, dashboards, alerts, quotas, retention, and audit export;
- add Kubernetes/Helm, rolling-upgrade/version-skew rules, worker drain, and restore testing;
- add enterprise OIDC SSO, external secret managers, isolated worker pools, network policy, and
  air-gapped guidance;
- run the reproducible 1M/10M/30M/100M performance and failure matrix.

Exit gate:

- clean install, restart, upgrade, backup/restore, queue/database/object-store outage, and
  horizontal-scale tests pass;
- tenant isolation, secret redaction, image/SBOM/provenance, and dependency-advisory gates pass;
- the complete 100M-row definition below passes on a published infrastructure profile.

## What to implement next

Finish the bounded data-plane gate before starting durable metadata:

1. Extend the pinned production-image curve to 1M/10M and representative wide-row/custom-transform
   workloads; the 30M narrow identity gate is complete.
2. Keep MongoDB blocked until an equivalent tested staging/merge protocol exists, and extend the
   PostgreSQL live failure matrix to connection loss during final publication.
3. Add a spill-capable local global-relational plan with explicit disk/memory/temp limits, while
   continuing to prefer ELT pushdown.
4. Expand schema evolution compatibility tests across supported targets and representative wide
   or nested records.

Do not add Better Auth, PostgreSQL run metadata, NATS, or distributed workers until the bounded
single-node data-plane contract is real.

## Definition of the 100M-row claim

Loafer can responsibly claim support only when a reproducible full-pipeline run:

- processes 100M rows within a documented infrastructure profile;
- keeps peak memory within a fixed bound independent of row count;
- produces correct row counts and checksums;
- survives worker termination and resumes from a committed checkpoint;
- never publishes partial output as success;
- emits complete redacted operational telemetry;
- obeys cancellation and timeout requests;
- documents unsupported global transform classes and delivery semantics.
