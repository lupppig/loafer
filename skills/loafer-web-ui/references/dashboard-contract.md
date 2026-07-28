# Dashboard contract

## Overview

Show:

- active, queued, failed, and SLA-breached runs;
- success rate and p50/p95 duration for a selected time range;
- rows and bytes processed with throughput;
- rejected rows and quality failures;
- source freshness and checkpoint lag;
- worker availability, utilization, and queue age;
- recent incidents and audit-sensitive actions.

Every card includes scope, time range, unit, freshness, and a link to supporting records. Do not
aggregate across workspaces unless the user has organization-level permission.

## Run detail

Use this hierarchy:

```text
run summary and authorized actions
stage timeline
live metrics and checkpoint
error or warning summary
partition/batch table
logs and events
config, transform artifact, lineage, and output
```

The stage timeline consumes authoritative event/state fields:

```text
run_id, stage_id, stage, status, attempt, sequence
started_at, finished_at, worker_id
rows_in, rows_out, rows_rejected, bytes_in, bytes_out
checkpoint, error_code, trace_id
```

Animate only transitions confirmed by events. On reconnect, request events after the last sequence,
detect gaps, and refresh the run snapshot.

For web-crawl sources, add an extraction subview with discovered, queued, active, fetched, parsed,
emitted, skipped, blocked, retried, and failed page counts. Show domain, depth, response status,
browser escalation, bytes, duplicates, downloads/PDFs, and crawl checkpoint without exposing
cookies, page credentials, or sensitive URL query values.

## Logs

Default to structured events, with raw log text as a secondary view. Support timestamp, severity,
stage, partition, attempt, worker, trace ID, search, pause-follow, copy, and redacted export. Make
redaction visible. Virtualize large streams and cap client memory.

## Shared semantics

The web dashboard and terminal dashboard must use the same status vocabulary, metrics definitions,
event sequence, time ranges, and permission rules. Presentation may differ; meaning must not.
