---
name: loafer-engineering
description: Build, review, debug, benchmark, document, or design Loafer, the Python CLI-first AI-assisted ETL/ELT engine and its Next.js web interface. Use for changes to pipeline configuration, connectors, web scraping/crawling, LangGraph agents, transform runners, NATS JetStream workers, sandboxing, incremental state, scheduling, CLI UX, data-plane scaling, observability, landing-page design, or the Loafer operator UI; especially when assessing production readiness or 30–100M+ row workloads.
---

# Loafer Engineering

Treat Loafer as a data system whose correctness is measured at dataset boundaries, not as an
ordinary Python application. Preserve bounded memory, replay safety, target consistency, precise
failure reporting, credential isolation, and operator visibility.

## Route focused work

Use the narrow skill when the task is owned by one surface:

- `$loafer-web-ui` for the browser dashboard and operator experience.
- `$loafer-cli-tui` for commands, terminal dashboards, and non-interactive output.
- `$loafer-auth` for Better Auth, sessions, organizations, SSO, CLI login, and service identities.
- `$loafer-api-design` for control-plane resources, authentication, tenancy, and events.
- `$loafer-engine` for connectors, transforms, batch execution, and data correctness.
- `$loafer-web-scraping` for HTTP/browser crawling, authentication, frontiers, discovered PDFs,
  and crawl safety.
- `$loafer-workers` for queues, leases, isolation, retries, and worker lifecycle.
- `$loafer-self-hosting` for containers, Compose, Kubernetes, upgrades, and open-source releases.

Use this skill for cross-cutting changes, production-readiness reviews, or work spanning multiple
boundaries.

## Start every task

1. Read [references/architecture.md](references/architecture.md) before changing the engine.
2. Read [references/platform-architecture.md](references/platform-architecture.md) for API,
   tenancy, scheduler, worker, deployment, or persistence work.
3. Read [references/scale-and-reliability.md](references/scale-and-reliability.md) for execution,
   connector, benchmark, security, or production-readiness work.
4. Read [references/connectors.md](references/connectors.md) before adding or changing a connector.
5. Read [references/product-ui.md](references/product-ui.md) for web or operator workflow work.
6. Inspect `git status --short`. Preserve unrelated and pre-existing changes.
7. Trace the complete path affected by the request: config → composition root → graph/agent →
   port → adapter → state/error/reporting → tests.

Treat every roadmap statement in the references as intent, not proof. Search for its implementation
and tests first.

## Protect the architectural boundaries

- Keep pure policies in `loafer/core/`.
- Define external contracts in `loafer/ports/`.
- Keep database, file, API, and LLM implementations in adapters.
- Resolve connector types only through `loafer/connectors/registry.py`.
- Instantiate providers and graphs in `loafer/runner.py`.
- Keep agents focused on state transitions. Do not add connector-selection branches to agents.
- Keep ETL and ELT graphs separate; share utilities only where their semantics truly match.
- Perform auto-detection only in `loafer/config.py`, and always respect explicit `type` values.
- Pass only schema metadata and at most five representative rows to an LLM. Never send a full
  dataset, credentials, connection strings, or unrestricted source values.

When current code violates a desired boundary, state the violation and either repair it within
scope or avoid deepening it.

## Classify transforms before implementation

Assign each transform one execution contract:

- **Row-local**: map/filter each chunk independently. Stream end to end with bounded memory.
- **Partition-local**: require a declared partition key and bounded partition state.
- **Global relational**: joins, sorts, windows, aggregations, or deduplication. Push down to the
  target/source engine or use a spill-capable engine and intermediate storage.
- **AI-generated**: generate a versioned transform artifact from a bounded schema sample, validate
  it, then execute it under one of the contracts above. Do not regenerate code per data chunk.

Reject “streaming” implementations that collect chunks into a list. At 30–100M rows, the unit of
work is a partition or batch, never the entire dataset.

## Implement engine changes

1. Define invariants and failure semantics before code:
   - What is the replay/idempotency behavior?
   - Where is the commit boundary?
   - What happens after partial writes?
   - Can the operation resume, and from which checkpoint?
   - Which row counts and watermarks are authoritative?
2. Extend Pydantic config with educational validation errors.
3. Change the narrowest port needed; update every adapter that implements it.
4. Keep live iterators and connections out of durable/checkpointed state.
5. Emit structured stage, batch, row, byte, duration, retry, and error metadata.
6. Add unit tests for policy and edge cases, integration tests for real adapter behavior, and an
   end-to-end test for the user-visible contract.
7. For scale claims, add a benchmark that measures peak resident memory and throughput across the
   full pipeline, not only a source or target connector in isolation.

## Implement connector changes

- Quote identifiers with adapter-specific safe composition APIs; never interpolate user-controlled
  table or column names directly into SQL.
- Use server-side cursors or native bulk export/import paths for databases.
- Keep chunk sizes configurable and record effective sizes.
- Make `finalize()` and `disconnect()` safe after failures.
- Specify transaction scope explicitly. Do not claim exactly-once delivery without a transactional
  source-to-target protocol.
- Write file outputs to a temporary path and atomically rename on successful completion.
- Detect schema drift between chunks and apply an explicit policy: fail, evolve, quarantine, or
  coerce. Never silently drop new fields.
- Redact secrets in errors and logs.

## Implement UI work

Treat the CLI and UI as clients of a versioned application service. Neither client owns execution
logic, worker lifecycle, durable scheduling, or credentials.

- Put runtime access behind an authenticated API/service boundary. The browser must never receive
  database credentials or LLM keys.
- Scope every persisted resource and authorization decision to an organization and workspace.
- Use server-sent events or WebSockets for run events; derive displays from persisted run state.
- Visualize extract → validate → transform → load as real stage state with timestamps and metrics,
  never as a decorative animation disconnected from the event stream.
- Provide a detailed, searchable, redacted log/event view with stage, worker, severity, trace ID,
  sequence number, retry attempt, and download controls.
- Design one shared workspace with persona-friendly entry points:
  - Data engineers: YAML/DAG, schedules, deployment, lineage, retries, backfills.
  - Analysts: guided source/target selection, SQL and previews, quality results.
  - Data scientists: samples, schema/profile exploration, Python/SQL notebooks, reproducibility.
  - Backend engineers: API/CLI parity, webhooks, logs, environment and secret references.
- Always expose dry-run/preview separately from execution.
- Require explicit confirmation for destructive actions and production promotion.
- Never fabricate live run state. Label mock or local-only experiences clearly.

Keep 3D or illustrated elements purposeful: use them in onboarding, empty states, and compact run
stage metaphors. Respect reduced motion and never trade operational density or accessibility for
decoration. Avoid generic AI gradients and ungrounded dashboard metrics.

## Review production readiness

Report findings in severity order with exact file and line evidence. Distinguish:

- **Implemented and verified**
- **Implemented with a limiting contract**
- **Roadmapped/documented only**
- **Missing for the claimed workload**

Treat unbounded materialization, unsafe SQL identifiers, partial-output visibility, missing
checkpoint/resume, lack of cancellation, and unmeasured memory as release blockers for a
30–100M-row claim. Use the readiness gates in
[references/scale-and-reliability.md](references/scale-and-reliability.md).

## Validate changes

Use the project-native commands:

```bash
uv run pytest tests/unit -q
uv run ruff check loafer tests
uv run mypy loafer
```

For web changes:

```bash
cd web
npm run build
npx eslint <changed-files>
```

Run integration, end-to-end, Docker smoke, or volume benchmarks when the change touches those
contracts. Report pre-existing failures separately from regressions.

## Keep claims honest

Use “connector-level chunking” for the current ETL behavior until transform and load remain bounded
across the full run. Reserve “production-ready at 100M+ rows” for a measured, reproducible full
pipeline benchmark with bounded memory, recovery tests, and documented infrastructure.
