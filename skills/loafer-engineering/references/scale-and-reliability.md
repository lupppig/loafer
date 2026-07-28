# Scale and reliability

## Contents

- Meaning of the target workload
- Required execution model
- Delivery and recovery semantics
- Validation and schema
- Security
- Observability
- Performance validation
- Release gates

## Meaning of the target workload

“30–100M+ rows” is incomplete without row width, transform class, source/target engines, network,
latency target, concurrency, and acceptable recovery point. Record these dimensions in every scale
proposal. A 100M-row projection and a 100M-row global sort are different systems.

Do not extrapolate full-pipeline capacity from connector-only benchmarks.

## Required execution model

Use an explicit batch envelope:

```text
run_id
partition_id
batch_id
source_position_start/end
schema_version
rows_in/out/rejected
bytes_in/out
transform_artifact_version
attempt
checksum
```

For row-local transforms:

```text
source partition → bounded batch → validate → transform → stage/write → commit checkpoint
```

Keep a small number of bounded batches in flight for backpressure. Avoid `list.extend()` across the
run. Prefer native bulk paths (`COPY`, Arrow/Parquet, object storage multipart uploads) over
row-oriented Python inserts for high volume.

For joins, aggregation, sorting, windows, and large deduplication, prefer pushdown into a database or
warehouse. If local execution is required, use an engine with explicit disk spill and enforce disk,
memory, and temp-space budgets.

## Delivery and recovery semantics

Document the actual guarantee:

- **At-most-once** can lose data.
- **At-least-once** requires idempotent target writes or deduplication.
- **Exactly-once effect** requires a transactional or idempotent protocol across checkpoint and
  target commit; a successful function return is not enough.

Use a durable metadata store for run, partition, batch, event, artifact, and checkpoint records.
Use leases/heartbeats for workers. Make cancellation cooperative at batch boundaries. Make retries
resume from the last committed position and preserve the transform artifact and config revision.

For file outputs, write to a run-scoped temporary object and publish atomically. For database
outputs, prefer staging tables plus transactional merge/swap where supported.

Advance an incremental watermark only after the corresponding target effect is durable. A single
maximum cursor can skip late-arriving rows; support lookback windows or compound cursors where
source semantics require them.

## Validation and schema

Sample validation is suitable for prompt construction and previews, not run-wide quality assurance.
Validate every batch and aggregate metrics across the run. Support:

- required columns and types;
- null, range, regex, enum, uniqueness, and referential checks;
- rejected-row quarantine with reason and source position;
- schema drift policies and schema versions;
- configurable fail-fast versus threshold-based acceptance;
- input/output counts and optional checksums.

Inferencing a target schema from the first row is unsafe. Infer from a declared contract or a
representative bounded sample and detect changes later.

## Security

- Treat custom and AI-generated code as untrusted.
- Prefer container/microVM isolation with no network, read-only filesystem, seccomp, UID isolation,
  strict CPU/memory/time/output limits, and an allowlisted runtime.
- Do not rely on AST validation as a security boundary.
- Store secrets in a server-side secret manager and refer to them by identifier.
- Redact URLs, headers, query parameters, samples, generated code, and exceptions.
- Use parameterized values and safe identifier composition for SQL.
- Add authentication, authorization, audit logs, workspace isolation, and CSRF protection before
  exposing control-plane mutations over the web.

## Observability

Persist and emit:

- run/stage/batch status and attempt;
- rows and bytes read, written, rejected, and retried;
- throughput, queue time, execution time, and backpressure;
- CPU, RSS, spill, disk, network, and target latency;
- checkpoint position and lag;
- schema/config/artifact versions;
- structured redacted errors and trace IDs.

Expose metrics and traces through a standard telemetry path. Logs alone are insufficient for a
multi-worker operator UI.

## Performance validation

Build a reproducible matrix:

| Dimension | Minimum cases |
|---|---|
| Rows | 1M, 10M, 30M, 100M |
| Width | narrow, medium, wide/nested |
| Transform | identity, map/filter, SQL aggregate/join |
| Flow | file→file, DB→file, DB→DB, ELT |
| Failure | source drop, target drop, worker kill, disk full, schema drift |
| Concurrency | 1, 2, 4+ independent partitions |

Measure peak RSS, throughput, CPU, bytes, target commit time, recovery time, duplicate/lost rows, and
output correctness. Run in a pinned container with recorded hardware and dependency versions.

## Release gates

Do not advertise production readiness for 30–100M rows until all gates pass:

- Peak memory remains within a documented bound independent of row count.
- No full-run materialization occurs in the chosen transform path.
- A killed worker resumes without silent loss; duplicates match the documented guarantee.
- Partial output is not published as success.
- Schema drift and malformed rows follow an explicit tested policy.
- Credentials and sampled sensitive data do not appear in logs or UI events.
- Cancellation, timeouts, and backpressure work.
- Full-pipeline 30M and 100M benchmarks are reproducible.
- The operational envelope and unsupported transform classes are documented.
