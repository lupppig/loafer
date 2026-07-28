# Multi-tenant platform architecture

## Contents

- Deployment model
- Package boundaries
- Tenant model
- Durable execution
- Security boundaries
- Self-hosting baseline
- Delivery sequence

## Deployment model

Build Loafer as a self-hosted control plane with independently scalable workers:

```text
browser / CLI
      ↓ authenticated versioned API
control-plane service
      ├── metadata database
      ├── secret references
      ├── scheduler
      └── durable queue port
             └── NATS JetStream in the distributed profile
             ↓ leases and heartbeats
       isolated workers
             ↓
       sources and targets
```

Do not run pipelines inside an HTTP request or a web process. Do not make the CLI a subprocess
dependency of the API. Both clients call application use cases; workers call the same engine
library through an explicit job contract.

## Package boundaries

Evolve toward these dependency directions:

| Package/process | Owns | Must not own |
|---|---|---|
| Engine library | config contracts, plans, transforms, connector ports, batch execution | HTTP, UI, Typer, tenant sessions |
| Application service | pipeline/run use cases, policy, authorization hooks | connector-specific execution loops |
| CLI | commands, local display, API/local client selection | duplicated orchestration |
| Control-plane API | auth, validation, persistence, idempotent commands, event reads | long-running data movement |
| Scheduler | due-run creation and concurrency policy | pipeline execution |
| Worker | lease, heartbeat, execution, checkpoints, event emission | user/session management |
| Web UI | safe authoring and operations experience | credentials or executable data-plane logic |

Introduce boundaries incrementally. First extract stable use-case interfaces from `cli.py` and
`runner.py`; do not rewrite the engine and platform simultaneously.

## Tenant model

Use a hierarchy of:

```text
user/service account
  ↔ organization membership and role
organization
  └── workspace
        ├── environments
        ├── connections and secret references
        ├── pipelines and immutable versions
        ├── schedules
        ├── runs, stages, batches, and events
        └── artifacts and audit records
```

Put `organization_id` and `workspace_id` on all tenant-owned records. Enforce tenant scope in the
repository/query layer and authorization policy, not only in route handlers. Use opaque IDs,
unique constraints scoped to the workspace, and deny-by-default roles. Record actor, action,
resource, tenant, request ID, and outcome in immutable audit events.

Never treat a multi-tenant control plane as permission to share customer data-plane access. Give
workers the minimum short-lived credentials required for one job and isolate untrusted transforms.

## Durable execution

Persist immutable pipeline versions and create each run from one exact version. The scheduler
creates a run record and enqueues its ID transactionally. A worker claims it with an expiring
lease, sends heartbeats, and advances only durable batch checkpoints.

Use append-only, monotonically sequenced run events. Derive UI timelines from persisted state and
events. Define state machines for run, stage, and batch statuses; reject impossible transitions.
Make create-run, cancel, retry, and schedule commands idempotent.

Separate retries:

- infrastructure retry reuses the same config and transform artifact;
- failed-batch retry resumes from a committed checkpoint;
- manual rerun creates a new run linked to the original;
- backfill declares an explicit range and concurrency budget.

Use PostgreSQL as the authoritative run/crawl state and transactional outbox. JetStream transports
opaque job/task IDs to durable pull consumers; it does not replace the metadata state machine,
leases/fencing, crawl frontier, checkpoints, event read model, or object storage. Keep the local CLI
profile operable without NATS.

## Security boundaries

- Authenticate humans and service accounts; support SSO later without coupling core execution.
- Authorize every resource access against organization/workspace membership.
- Encrypt secrets at rest and return references only.
- Redact credentials and sampled values before events leave the worker.
- Use worker pools and network policies to separate tenants and environments.
- Execute custom/generated code in isolated, resource-limited sandboxes.
- Require CSRF protection for cookie sessions and rate limits for commands and log streams.
- Sign or hash immutable config and transform artifacts.

## Self-hosting baseline

Offer a small production topology that startups can operate:

- one stateless API/UI service;
- one PostgreSQL metadata database;
- one NATS JetStream service for the distributed profile, accessed through a queue port;
- one scheduler process;
- one or more worker processes;
- object storage for artifacts, logs, and temporary outputs;
- OpenTelemetry-compatible metrics and traces.

Provide health/readiness endpoints, migrations, seeded administrator setup, backup/restore
instructions, resource estimates, and an upgrade path. A single Docker Compose deployment may run
these services on one host while preserving the process boundaries used for horizontal scale.

## Delivery sequence

1. Extract engine/application interfaces without changing current CLI behavior.
2. Define tenant-aware metadata schema and run/stage/event state machines.
3. Add API auth, workspace authorization, and read-only resources.
4. Add durable run creation, queue, worker lease, heartbeat, cancellation, and event emission.
5. Make the CLI choose local or API mode through one client contract.
6. Connect UI run history, live stages, logs, and pipeline validation.
7. Add safe connection management, schedules, retries, backfills, and environment promotion.
8. Harden sandbox isolation, audit, quotas, backup/restore, and multi-worker recovery.

Do not call the platform production-grade until tenant-isolation tests, worker-kill recovery,
authorization tests, secret-redaction tests, and full-pipeline volume benchmarks pass.
