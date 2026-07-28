# Control-plane resource model

## Ownership

```text
organization
  ├── memberships and service accounts
  └── workspace
       ├── environments
       ├── connection metadata and secret references
       ├── pipelines
       │    └── immutable versions
       ├── schedules
       ├── crawl sources and encrypted auth-profile references
       ├── runs
       │    ├── stages
       │    ├── partitions and batches
       │    ├── crawl pages/frontier entries
       │    └── append-only events
       ├── artifacts
       └── audit events
```

## Minimum routes

```text
GET/POST  /api/v1/pipelines
POST      /api/v1/pipelines/validate
GET/POST  /api/v1/pipelines/{id}/versions
GET/POST  /api/v1/connections
POST      /api/v1/connections/{id}/test
GET/POST  /api/v1/schedules
GET/POST  /api/v1/runs
GET       /api/v1/runs/{id}
POST      /api/v1/runs/{id}/cancel
POST      /api/v1/runs/{id}/retry
GET       /api/v1/runs/{id}/events
GET       /api/v1/runs/{id}/logs
GET       /api/v1/metrics/overview
POST      /api/v1/crawl-previews
GET       /api/v1/runs/{id}/crawl-pages
```

Prefer immutable pipeline versions. A run references one exact version, environment, transform
artifact, and execution policy.

## State transitions

Define allowed transitions for runs, stages, and batches. Reject stale worker updates with lease or
fencing tokens. Terminal states are immutable except for administrative reconciliation recorded in
the audit log.

Crawl previews must be bounded by pages, bytes, time, domains, and browser concurrency. Crawl-page
resources expose safe URL/provenance/status fields and artifact references, never session cookies,
authentication state, or unrestricted response bodies.
