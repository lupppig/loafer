# Product UI

## Contents

- Product boundary
- Public visual direction
- Information architecture
- Core workflows
- API contract
- Safety and accessibility
- Delivery sequence

## Product boundary

Build the UI as a control plane over the existing engine. Do not execute Python transforms,
database queries, or secret handling in the browser. Add an authenticated server that owns project
state, secret references, validation, run creation, event persistence, and worker coordination.

The current `web/` application is a Next.js marketing/docs site. A visual studio added there without
an API is a prototype and must be labeled accordingly.

## Public visual direction

Use `$loafer-web-ui` and its `visual-direction.md` reference for the landing page. Build an artsy,
tactile data-foundry identity with purposeful 3D flow and occasional mascot illustration. Avoid
generic AI gradients, glass-card sameness, and visual cloning of Supabase, Databricks, or Firebase.
Keep primary content server-rendered and make WebGL progressive, measurable, and optional.

## Information architecture

Use an organization and workspace-level shell:

```text
Organization switcher
Workspace switcher
Overview
Pipelines
  Pipeline editor
  Versions
  Schedules
Runs
  Run detail
  Logs/events
Connections
Data explorer
Quality
Artifacts
Members and roles
Settings
```

Keep pipeline authoring in three synchronized views where feasible:

- Guided form for analysts and first-time users.
- YAML/SQL/Python editor for engineers and scientists.
- Read-only graph/lineage view for shared understanding.

Do not make a free-form node canvas the only authoring method. It is slow for repeatable,
version-controlled pipeline work.

## Core workflows

### Create and validate

1. Choose source connection and dataset/query.
2. Inspect a bounded sample and schema profile.
3. Choose ETL or ELT with a plain-language explanation of compute location.
4. Author one or more transformations.
5. Preview generated code/SQL and show its artifact hash/version.
6. Run a bounded dry-run with row-level diffs and quality results.
7. Configure target, write mode, incremental cursor, schedule, and limits.
8. Save a version and promote it to an environment.

### Operate

Show status, current stage, partitions, rows/bytes, throughput, ETA confidence, checkpoints,
warnings, compute use, and recent events. Provide cancel, retry failed partitions, resume,
backfill, compare runs, and open output actions according to authorization.

Represent a live run as a stage sequence driven by persisted events:

```text
Extracting → Validating → Transforming → Loading → Verifying
```

Each stage exposes queued/running/succeeded/failed/cancelled state, start/end time, duration,
rows/bytes, attempts, worker, and checkpoint. A 3D conveyor, pipeline, or mascot may reinforce this
sequence, but the accessible DOM, text status, metrics, and logs remain authoritative.

### Diagnose

Present the failing stage and partition first. Include sanitized error, last successful checkpoint,
input/output/rejected counts, schema diff, retry history, transform artifact, and suggested safe
actions. Keep raw logs available but secondary.

Provide a detailed log section with:

- stage, worker, severity, timestamp, event sequence, trace ID, and retry filters;
- full-text search, pause-follow, wrap, copy, and redacted download;
- reconnect from the last event sequence without gaps or duplicates;
- links from an error summary to the exact correlated event;
- explicit notices when payloads, secrets, or samples were redacted.

## API contract

Use durable resources rather than shelling out directly from HTTP request handlers:

```text
POST   /api/v1/pipelines/validate
GET    /api/v1/pipelines
POST   /api/v1/pipelines
GET    /api/v1/pipelines/{id}/versions
POST   /api/v1/runs
GET    /api/v1/runs/{id}
POST   /api/v1/runs/{id}/cancel
POST   /api/v1/runs/{id}/retry
GET    /api/v1/runs/{id}/events
GET    /api/v1/connections
POST   /api/v1/connections/{id}/test
POST   /api/v1/previews
```

Make mutations idempotent where practical. Use opaque IDs and workspace authorization. Stream
events with sequence numbers so clients can reconnect without losing state.

Never return secret values. Connection resources expose display metadata and capability flags only.
Require organization/workspace context on every resource and enforce it on the server. Never rely
on a workspace ID supplied by the browser without authorization against the authenticated subject.

## Safety and accessibility

- Distinguish draft, preview, staging, and production visually and textually.
- Show estimated scope before destructive or expensive operations.
- Require typed confirmation or a reviewed approval for replace/drop/backfill operations.
- Make every interaction keyboard accessible with visible focus.
- Do not encode run state by color alone.
- Use semantic tables for run history and schema, live regions for run updates, and reduced-motion
  behavior.
- Avoid fake metrics, fake customer counts, or fake live status in a production surface.
- Use restrained depth, illustration, and animation. Avoid generic AI gradients. Prefer a neutral
  operational palette, sharp typography, dense data tables, and one memorable mascot or 3D system.

## Delivery sequence

1. Define tenant, run, event, config, and worker persistence contracts.
2. Add authentication, organization/workspace authorization, and audit events.
3. Expose read-only pipeline and run endpoints.
4. Build authenticated overview, live stage visualization, logs, and run detail.
5. Add connection management using secret references.
6. Add versioned pipeline editor and server-side validation.
7. Add dry-run previews and approvals.
8. Add schedules, retries, backfills, quality, and lineage.

The first production UI should prioritize run visibility and safe pipeline configuration over a
large visual DAG editor.
