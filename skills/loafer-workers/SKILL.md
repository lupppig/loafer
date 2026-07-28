---
name: loafer-workers
description: Design, implement, harden, debug, or review Loafer's distributed worker runtime and data-plane operations. Use for NATS JetStream, durable queues, pipeline and crawl jobs, job claims, leases, heartbeats, fencing, retries, cancellation, checkpoints, browser/process isolation, resource limits, worker pools, event emission, graceful shutdown, autoscaling, and failure recovery.
---

# Loafer Workers

Run one immutable job contract through the engine under a durable lease. Keep workers independently
deployable from the API, scheduler, web, and CLI.

## Start

1. Read [references/worker-protocol.md](references/worker-protocol.md).
2. Inspect the queue, run store, engine boundary, secret provider, and event sink.
3. Define crash points and the durable state before and after each point.
4. Identify tenant, environment, pool, network, and resource isolation requirements.
5. Use `$loafer-web-scraping` for browser workers, crawl frontiers, and per-page semantics.
6. Use `$loafer-document-extraction` for parser/OCR worker limits, artifacts, and page semantics.

## Claim work safely

- Claim by run/job ID with an expiring lease and fencing token.
- Heartbeat separately from progress events.
- Reject updates from expired or superseded workers.
- Make claim, retry scheduling, and terminal transition transactional.
- Apply concurrency limits by installation, organization, workspace, connection, and worker pool.
- Use queue visibility/lease timeout greater than the heartbeat interval with measured margins.

## Use JetStream as transport

- Prefer NATS JetStream durable pull consumers with explicit acknowledgements for the distributed
  profile; keep queue access behind a port and preserve a no-NATS local CLI mode.
- Keep PostgreSQL authoritative for jobs, leases/fencing, checkpoints, crawl pages, and outbox
  records. Publish through a transactional outbox instead of a database/NATS dual write.
- Put opaque IDs and routing metadata in messages, never credentials, configs, datasets, page
  bodies, PDFs, screenshots, or browser state.
- Use stable message IDs for publish deduplication, but still make every consumer idempotent.
- Acknowledge only after durable state/result/checkpoint persistence. Send in-progress
  acknowledgements for long tasks and reject stale fencing tokens after redelivery.
- Separate pipeline, HTTP-crawl, and browser-crawl subjects/consumers so browser load cannot starve
  ordinary pipelines.
- Configure bounded message age/bytes, `MaxAckPending`, `MaxDeliver`, backoff, replicas, and an
  explicit quarantine/dead-letter workflow.

## Execute predictably

- Resolve the immutable pipeline version, transform artifact, policy, and secret references after
  claim.
- Give the engine bounded cancellation, event, checkpoint, and secret-resolution interfaces.
- Emit sequenced structured events for state transitions and metrics.
- Commit a batch checkpoint only after durable target publication.
- Separate infrastructure retry, failed-batch retry, manual rerun, and backfill.
- Preserve config and artifact versions across automatic retries.

## Isolate data work

- Run custom and generated code in a network-disabled, read-only, unprivileged sandbox with CPU,
  memory, time, process, file, and output limits.
- Grant short-lived minimum credentials for one job.
- Separate worker pools by trust level, environment, and network reachability.
- Redact before logs/events leave the worker.
- Never mount the container runtime socket into a general worker.

## Handle lifecycle

- Stop claiming work on shutdown, keep heartbeating active jobs, checkpoint at safe boundaries, and
  release or expire leases.
- Treat duplicate delivery as normal and make target/checkpoint protocols idempotent.
- Surface stuck leases, heartbeat lag, queue age, resource pressure, and crash loops.
- Quarantine poison jobs after a bounded retry policy and preserve diagnostic evidence.

## Validate

Test duplicate delivery, lost publish/acknowledgement, stale fencing tokens, worker kill at every
commit boundary, queue outage, metadata outage, source/target disconnect, cancellation, timeout,
disk/memory exhaustion, secret redaction, browser crash, JetStream restart/retention pressure,
graceful shutdown, and multi-tenant concurrency limits. Measure recovery time and verify no silent
loss or unreported partial publication.
