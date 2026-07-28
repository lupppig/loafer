# Worker protocol

## Job payload

Keep queue payloads small:

```text
job_id, run_id, workspace_id, environment_id
pipeline_version_id, execution_policy_id
required_worker_pool, enqueued_at, trace_context
```

Resolve configuration and secrets server-side after an authorized claim. Do not put credentials,
full configs, samples, or generated code in the queue.

For JetStream, publish this envelope from a transactional outbox with a stable message ID. Use
durable pull consumers and explicit acknowledgements. Acknowledge only after the corresponding
metadata/checkpoint transaction commits.

## Lease lifecycle

```text
queued → claimed(lease, fencing token) → running → terminal
                         ↘ lease expires → queued/retry
```

Heartbeat renews the lease only for its fencing token. A worker includes that token in every state,
event, and checkpoint write.

## Event lifecycle

Emit append-only per-run sequence numbers for claimed, stage started, progress, checkpoint,
warning, retry scheduled, cancellation observed, stage completed, run completed, and run failed.
Persist structured fields separately from a sanitized message.

## Operational metrics

Expose claims, queue age, active jobs, heartbeats, lease expirations, retry counts, stage duration,
rows/bytes throughput, CPU, RSS, spill/disk, source/target latency, sandbox kills, and shutdown
drain time. For crawls add discovered/fetched/parsed/emitted pages, per-domain rate, browser
contexts, downloads, blocked-policy count, and frontier checkpoint lag.
