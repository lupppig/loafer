# NATS JetStream contract

## Decision

Use NATS JetStream as the preferred distributed transport candidate for Loafer run jobs and
eventual crawl tasks. Keep it behind queue/event ports and keep embedded local execution free of a
NATS dependency.

JetStream provides durable streams, acknowledgements, redelivery, flow control, pull consumers,
retention policies, and publish deduplication. These are transport guarantees, not proof that a
target write or crawl page was processed exactly once.

## Ownership

| System | Owns |
|---|---|
| PostgreSQL metadata | runs, crawl pages, state machines, leases/fencing, checkpoints, outbox |
| NATS JetStream | durable delivery, backpressure, redelivery, bounded replay |
| Object storage | large artifacts, page bodies, downloads, logs, browser traces |
| Worker | execution, heartbeat, durable result/checkpoint, acknowledgement |

Never store credentials, full configs, HTML, PDFs, screenshots, or transform artifacts in NATS.
Messages contain opaque IDs, attempt, required pool, fencing/trace context, and schema version.

## Publication

- Insert the run/task and outbox record in one metadata transaction.
- Relay the outbox record to JetStream with a stable `Nats-Msg-Id`.
- Mark the outbox record published only after the server acknowledges persistence.
- Make consumers idempotent even when publish deduplication is enabled; its deduplication window is
  bounded.

## Consumption

- Use durable pull consumers with explicit acknowledgements for horizontally scaled workers.
- Set `MaxAckPending`, fetch batch, and byte limits to create backpressure.
- Set `AckWait` from measured task duration and send in-progress acknowledgements for long work.
- Configure bounded `MaxDeliver` and backoff. Move terminal poison tasks to an explicit
  quarantine/dead-letter workflow with preserved diagnostics.
- Acknowledge only after the authoritative state/result/checkpoint is durable.
- Use message ID, job/task ID, attempt, and fencing token to reject stale or duplicate effects.
- Treat late acknowledgement and redelivery races as normal.

## Streams and subjects

Separate workload classes so browser crawls cannot starve database pipelines:

```text
loafer.jobs.pipeline.<pool>
loafer.jobs.crawl.http.<pool>
loafer.jobs.crawl.browser.<pool>
loafer.events.run.<shard>
```

Do not put organization names, domains, URLs, or secrets in subjects. Authorize service accounts
to the narrow publish/consume subjects they require. Define maximum message size, age, bytes,
replicas, discard behavior, and consumer count explicitly.

Use work-queue retention for exclusive job delivery. Use a separate limits-retention event stream
only when replay is required; the metadata event store remains authoritative for the UI.

## Operations

Monitor stream bytes/messages, consumer lag, oldest message age, pending and redelivered messages,
ack latency, max-deliver advisories, outbox lag, publish errors, and unavailable replicas.

Test NATS restart, lost publish acknowledgement, duplicate publish, worker death before/after
checkpoint, acknowledgement loss, poison messages, retention limits, disk full, network partition,
consumer version skew, and restore procedures.
