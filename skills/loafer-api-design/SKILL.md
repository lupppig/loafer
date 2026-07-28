---
name: loafer-api-design
description: Design, implement, review, or document Loafer's versioned multi-tenant control-plane API. Use for authentication, organizations, workspaces, pipelines, PDF/document and web-crawl sources, asynchronous previews, crawl frontiers, versions, connections, secrets, schedules, runs, events, metrics, logs, quality, artifacts, authorization, idempotency, pagination, OpenAPI, SSE, and webhooks.
---

# Loafer API Design

Build a durable application boundary for both web and CLI clients. Keep pipeline execution outside
HTTP request processes.

## Start

1. Use `$loafer-auth` for identity, sessions, organizations, SSO, and machine credentials.
2. Use `$loafer-document-extraction` for document source, preview, artifact, and quality semantics.
3. Read [references/resource-model.md](references/resource-model.md).
4. Inspect current engine/application boundaries and persistence before choosing endpoints.
5. Define the resource, actor, tenant, state transition, idempotency behavior, and audit event.
6. Preserve API compatibility or version the breaking change.

## Enforce the control-plane boundary

- Make handlers authenticate, authorize, validate, call one application use case, and serialize.
- Convert the verified Better Auth session/token into one immutable request `AuthContext`.
- Put business transitions in application services, not routes, ORM hooks, or clients.
- Enqueue run IDs transactionally; never execute a pipeline in an HTTP request.
- Store secret references only and never return secret values.
- Generate an OpenAPI contract and typed clients for the web and CLI.

## Design tenant-safe resources

- Scope tenant-owned records to `organization_id` and `workspace_id`.
- Resolve scope from the authenticated subject and authorized membership.
- Enforce tenant filters and composite uniqueness in repositories/database constraints.
- Use opaque IDs and deny by default.
- Record actor, action, resource, tenant, request ID, IP/session context, and outcome in immutable
  audit events.
- Test cross-tenant IDs, guessed IDs, stale memberships, service accounts, and role changes.

## Use predictable HTTP semantics

- Prefix stable routes with `/api/v1`.
- Use plural nouns, cursor pagination, UTC RFC 3339 timestamps, and explicit enums.
- Return RFC 9457-style problem details with stable `code`, safe `detail`, `request_id`, and field
  errors.
- Accept an idempotency key for create-run, cancel, retry, backfill, connection test, and schedule
  mutations.
- Use optimistic concurrency or version preconditions for mutable drafts.
- Return `202 Accepted` for queued work and expose its durable status resource.
- Treat crawl seeds, scope rules, extraction rules, browser mode, budgets, and auth-profile
  references as versioned source configuration. Never accept an unbounded crawl.

## Stream operations

- Persist append-only run events with monotonic per-run sequence numbers.
- Provide SSE first unless bidirectional low-latency control is required.
- Support `Last-Event-ID`, heartbeats, bounded retention, reconnect, and gap detection.
- Keep logs redacted before persistence. Paginate historical logs and cap live client buffers.
- Derive dashboards from documented metrics endpoints or read models, not expensive ad hoc scans.
- Paginate crawl pages/frontier reads and redact sensitive URL query values. Keep raw page bodies,
  cookies, browser state, downloads, and traces behind authorized artifact endpoints.

## Validate

Add contract tests, authorization matrix tests, migration tests, state-machine tests, idempotency
tests, pagination tests, and SSE reconnect/gap tests. Fuzz malformed identifiers and filters.
Verify OpenAPI generation has no undocumented routes and no secret-bearing response fields.
