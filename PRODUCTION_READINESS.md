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

- The unit suite contains 578 passing tests with 8 skips in the current working tree.
- ETL and ELT are modeled as separate graphs.
- Source adapters expose a chunk iterator and database sources use streaming cursors.
- LLM prompts receive a schema sample rather than a full dataset.
- Multiple LLM providers implement a common interface.
- Custom and generated Python run in a separate process with CPU/memory/time limits on POSIX.
- Incremental extraction advances its cursor after a completed run.
- PostgreSQL and MongoDB support upsert modes.
- The CLI exposes stage-aware Rich output and human-readable errors.
- The website production build completes successfully.

These strengths justify shipping an early release for bounded workloads with a documented
operational envelope.

## Release blockers for 30–100M+ ETL

### P0 — End-to-end materialization

`loafer/transform/__init__.py::materialize_input_rows` drains every source chunk into one list.
AI, custom Python, SQL, and multi-step ETL runners use it. `loafer/agents/load.py` then writes from
`transformed_data`, which is also a full-run list.

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

### P0 — Partial outputs can appear final

CSV and JSON targets open the final path directly. PostgreSQL commits every small insert batch and
also commits table creation/index changes separately.

Impact:

- Failed file runs leave truncated or invalid files at the requested output path.
- Failed database runs leave partially loaded rows.
- A retry may duplicate data in append mode.
- “Pipeline failed” does not mean “target unchanged.”

Required correction:

- Write file/object outputs to run-scoped temporary locations and atomically publish on success.
- Use staging tables plus transactional merge/swap for replace/upsert flows.
- Define per-target guarantees: at-most-once, at-least-once with idempotency, or exactly-once effect.
- Expose partial-write state and recovery instructions.

### P0 — SQL identifier construction is unsafe

PostgreSQL target table, column, key, and index identifiers are interpolated into SQL strings.
ELT table creation/drop paths follow the same pattern. Cursor values are bound safely, but cursor
identifiers are string-quoted manually.

Impact:

- Unusual or reserved identifiers break.
- User-controlled configuration can cross the intended identifier boundary.
- Schema-qualified names are handled inconsistently.

Required correction:

- Use `psycopg2.sql.Identifier`/`SQL` composition throughout.
- Parse schema and table separately.
- Validate config identifiers and add adversarial tests.

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

## High-priority production gaps

### P1 — Data quality is sample-based

Validation is primarily computed from the schema sample. It cannot prove whole-run null rates,
type consistency, uniqueness, referential integrity, or rejected-row counts.

Build batch-level validation with aggregated run metrics and a quarantine output.

### P1 — Schema drift policy is implicit

Target schema is inferred from the first row/chunk. Later columns and type changes can fail or be
silently coerced depending on the adapter.

Add declared contracts, schema versions, and explicit `fail | evolve | quarantine | coerce`
policies.

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

## Recommended delivery plan

### Milestone A — Honest public alpha

- Document a conservative row/memory envelope.
- Call the existing behavior connector-level chunking.
- Fix unsafe SQL identifiers and atomic file publication.
- Add full-pipeline 1M/10M memory benchmarks.
- Keep the Studio labeled as a preview.

### Milestone B — Bounded single-node engine

- Introduce batch envelopes and row-local streaming transforms.
- Add batch validation and reject/quarantine support.
- Add artifact versioning and schema versions.
- Add staging/commit protocols for targets.
- Prove bounded memory at 30M rows.

### Milestone C — Resumable execution

- Add durable run/partition/batch/checkpoint metadata.
- Add cancellation, leases, retries, and recovery tests.
- Support safe incremental lookback/compound cursors.
- Prove recovery without silent loss under injected failures.

### Milestone D — Control plane

- Add authentication, authorization, workspaces, secret references, and audit.
- Expose pipeline validation/versioning and read-only run APIs first.
- Connect run events to the Studio.
- Add connection testing, previews, scheduling, approvals, retries, and backfills.

### Milestone E — Horizontal scale

- Add partition planning and worker concurrency.
- Add native bulk I/O and object-storage intermediates.
- Add warehouse pushdown and spill-capable global transforms.
- Prove the 100M-row matrix across supported source/target combinations.

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
