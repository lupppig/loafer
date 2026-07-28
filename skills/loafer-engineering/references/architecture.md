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

The Next.js App Router application under `web/` currently serves marketing, MDX documentation, and
a clearly labeled product preview; it has no authenticated runtime API.

## Current execution path

```text
YAML
  → config.load_config
  → runner._build_initial_state
  → runner selects ETL or ELT LangGraph
  → extract agent resolves source adapter and samples schema
  → validate agent applies sample-based checks
  → ETL: transform runner → load target adapter
  → ELT: load_raw target adapter → in-target SQL transform
  → runner persists incremental cursor after graph completion
```

`PipelineState` mixes configuration, data, execution metadata, live iterators, provider objects, and
connectors. It is an in-process coordination object, not a durable workflow record.

## Module ownership

| Area | Ownership |
|---|---|
| `loafer/config.py` | Pydantic schema, environment substitution, auto-detection |
| `loafer/core/` | Destructive-change policy, sandbox process, incremental state |
| `loafer/ports/` | Connector and LLM interfaces |
| `loafer/adapters/` | Database/file/API source and target implementations |
| `loafer/connectors/registry.py` | Connector registration and construction |
| `loafer/agents/` | LangGraph stage functions |
| `loafer/transform/` | AI, Python, SQL, and multi-step execution |
| `loafer/graph/` | Separate ETL and ELT topology |
| `loafer/runner.py` | Composition root and invocation |
| `loafer/cli.py` | Typer/Rich user experience |
| `loafer/scheduler.py`, `daemon.py` | Local APScheduler service lifecycle |
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
- Python transform subprocess sandboxing on supported operating systems.
- CLI validation, connector listing, runs, scheduling, daemon management, logs, and initialization.
- Unit, integration, end-to-end, smoke, and opt-in benchmark tests.

## Roadmap gaps

At the time this reference was written, searches found no whole-database inspector/orchestrator,
dependency DAG, cross-table join model, parallel table executor, interactive explore command,
durable distributed run store, NATS worker transport, full web crawler/browser source,
runtime web API, tenant-aware authorization, or operator UI connected to the engine.

Re-check the repository because this reference is a snapshot, not a substitute for inspection.

## Architectural risks

- The source stream is drained by `materialize_input_rows()` into a list for AI, custom, SQL, and
  multi-step ETL transforms. The ETL load agent then writes from `transformed_data`.
- Sample-based validation does not validate every partition.
- Local JSON watermark state is not sufficient for concurrent or distributed workers.
- File targets expose partial output at the final path during a failed run.
- Several SQL statements interpolate table/column identifiers directly.
- Postgres target commits individual insert batches, so a later failure leaves partial writes.
- The graph state contains non-serializable objects, preventing straightforward durable LangGraph
  checkpointing.
- The UI cannot safely start or observe runs until a server-side control-plane API exists.
