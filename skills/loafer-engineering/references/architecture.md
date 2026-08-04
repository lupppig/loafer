# Loafer architecture

## Contents

- Product model
- Current execution path
- Module ownership
- Implemented surface
- Roadmap gaps
- Architectural risks

## Product model

Loafer is a CLI-first, declarative ETL/ELT engine. A user supplies a YAML pipeline that defines a
source, transform, target, quality policy, optional incremental cursor, sandbox limits, and LLM
provider. The engine supports deterministic custom Python and SQL transforms plus AI-generated
Python or SQL. AI is an authoring aid; data movement must not depend on sending a dataset to an LLM.

The intended product has two planes and multiple independently deployable processes:

- **Data plane workers**: extract, validate, transform, and load partitions.
- **Control plane service**: author configurations, validate, schedule, observe, approve, retry,
  and govern runs.
- **Clients**: the CLI and web UI call the control-plane API; neither embeds a second engine.
- **Distributed transport**: NATS JetStream carries opaque job/task IDs from a PostgreSQL outbox to
  isolated worker pools; local CLI execution does not require it.

Keep these planes separate. Package the core engine as a reusable library with no Typer, HTTP, or
React dependencies. Run workers separately from the API so they can scale, restart, and receive
different network/secret policies. See
[platform-architecture.md](platform-architecture.md) for the target deployment boundary.

The Next.js App Router application under `web/` serves marketing, MDX documentation, and a clearly
labeled product preview. Its Better Auth boundary owns users, sessions, organizations,
invitations, device authorization, automation keys, and JWT/JWKS issuance. Its server-only BFF
validates browser credentials and forwards short-lived tokens to the same HTTPS `loaferd` API used
by CLI and automation clients; it does not execute pipelines.

## Current execution path

```text
YAML
  → config.load_config
  → application.RunPipeline creates a credential-free ExecutionPlan
  → engine._build_initial_state
  → declared row_local ETL: bounded data plane
      → source batch → schema/validation → prepared transform_batch → staged file target
      → BatchEnvelope + rolling reconciliation → atomic publication → final checkpoint
  → otherwise engine selects ETL or ELT LangGraph
      → extract agent resolves source adapter and samples schema
      → validate agent applies sample-based checks
      → ETL: materialized transform runner → load target adapter
      → ELT: load_raw target adapter → in-target SQL transform
  → engine persists the local incremental cursor after graph completion
  → application emits sanitized RunEvent / RunResult contracts

Durable single-node mode adds a separate path around that application use case:

```text
enqueue/scheduler → immutable pipeline version + idempotent run command + outbox
worker claim → expiring lease + fencing token → bounded engine execution
batch output object → transactional batch/checkpoint/event commit → attempt-local target
worker restart → replay committed objects → skip durable source offset → final publication
```
```

`PipelineState` mixes configuration, data, execution metadata, live iterators, provider objects, and
connectors. It is explicitly an ephemeral in-process coordination object, not a durable workflow
record. Persistence and client surfaces use the credential-free contracts in `loafer/contracts.py`.

## Module ownership

| Area | Ownership |
|---|---|
| `loafer/config.py` | Pydantic schema, environment substitution, auto-detection |
| `loafer/contracts.py` | Serializable execution plans, batch metadata, events, snapshots, and results |
| `loafer/core/` | Destructive-change policy, sandbox process, incremental state |
| `loafer/ports/` | Connector, LLM, cancellation, checkpoint, secret, event, and review interfaces |
| `loafer/adapters/` | Database/file/API source and target implementations |
| `loafer/connectors/registry.py` | Connector registration and construction |
| `loafer/agents/` | LangGraph stage functions |
| `loafer/transform/` | AI, Python, SQL, and multi-step execution |
| `loafer/graph/` | Separate ETL and ELT topology |
| `loafer/data_plane.py` | Bounded row-local batch orchestration and atomic publication |
| `loafer/engine.py` | In-process graph composition and execution |
| `loafer/application/` | Plan, run, validate, and connector-listing use cases |
| `loafer/runner.py` | Backward-compatible Python facade over the application service |
| `loafer/cli.py` | Typer/Rich user experience |
| `loafer/control_plane/` | HTTPS `loaferd`, tenant policy, durable commands, SSE, and typed client |
| `loafer/scheduler.py`, `daemon.py` | Local APScheduler lifecycle; runs call the application service |
| `web/` | Next.js web control-plane shell, marketing site, and MDX documentation |

## Implemented surface

Verify before relying on it, but the repository currently contains:

- PostgreSQL, MySQL, MongoDB, CSV, Excel, REST, SQLite, and PDF sources.
- PostgreSQL, MongoDB, CSV, and JSON targets.
- ETL and ELT LangGraph flows.
- AI, custom Python, DuckDB SQL, and multi-step transforms.
- Gemini, Claude, OpenAI, and Qwen provider adapters.
- Cursor-based incremental extraction with a local JSON state file.
- Postgres and Mongo upsert modes.
- Declared row-local ETL through bounded batches with schema policies, per-batch validation,
  quarantine, checksums, cancellation, and atomic CSV/JSON publication.
- Python transform subprocess sandboxing on supported operating systems.
- CLI validation, connector listing, runs, scheduling, daemon management, logs, and initialization.
- Versioned SQLite/PostgreSQL metadata, state machines, sequenced events, leases/fencing,
  idempotent commands, outbox records, filesystem object storage, and single-node bounded-batch
  recovery through a separately runnable worker.
- Better Auth-backed identity, Loafer-owned tenant/permission metadata, an HTTPS-only `/api/v1`
  control plane, OpenAPI plus generated browser types, typed CLI/web clients, and persisted SSE
  event streaming.
- Unit, integration, end-to-end, smoke, and opt-in benchmark tests.

## Roadmap gaps

At the time this reference was written, searches found no whole-database inspector/orchestrator,
dependency DAG, cross-table join model, parallel table executor, interactive explore command,
NATS worker transport, full web crawler/browser source, connected operator Studio, or professional
TUI. The authenticated runtime API and tenant-aware authorization now exist; distributed control
command consumers and end-user application screens are the next boundaries.

Re-check the repository because this reference is a snapshot, not a substitute for inspection.

## Architectural risks

- Undeclared/materialized AI, custom, SQL, and multi-step ETL still drain the source stream through
  `materialize_input_rows()`; only explicitly declared row-local work uses the bounded data plane.
- The legacy graph path remains sample-validated; the bounded row-local path validates every batch.
- Local JSON watermark state remains on the synchronous legacy path; durable worker runs use the
  metadata checkpoint store, while SQLite is intentionally limited to one scheduler and worker.
- CSV and JSON targets publish atomically, but the local watermark/state file does not yet use the
  same publication protocol.
- PostgreSQL target/ELT identifiers are safely composed, but every future SQL adapter and
  identifier-bearing source feature must preserve that boundary.
- Postgres target commits individual insert batches, so a later failure leaves partial writes.
- The graph state contains non-serializable objects, preventing straightforward durable LangGraph
  checkpointing.
- The Studio remains a product preview even though the server-side BFF and control-plane clients
  exist; Phase 6 connects its screens to real tenant-scoped resources and events.
