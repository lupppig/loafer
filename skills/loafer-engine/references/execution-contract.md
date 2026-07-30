# Execution contract

## Batch envelope

```text
run_id, stage_id, partition_id, batch_id, attempt
source_position_start, source_position_end
schema_version, transform_artifact_version
rows_in, rows_out, rows_rejected, rows_filtered
bytes_in, bytes_out, input_checksum, output_checksum
```

## Row-local flow

```text
read bounded batch
  → validate and quarantine
  → transform with versioned artifact
  → stage/write target effect
  → commit target
  → commit checkpoint and metrics
```

Bound the number of batches in flight. Cancellation occurs between safe boundaries. Retry the same
artifact and config version.

Loafer's implemented local row-local profile currently uses one batch in flight and atomically
publishes CSV/JSON files after every batch succeeds. PostgreSQL writes to a hidden run-scoped table
and publishes in one final transaction: `replace` is replayable replacement, `error` is atomic
create-once, `append` is at-least-once across a target-commit/checkpoint gap, and deterministic
`upsert` is idempotent by its declared key. MongoDB is rejected until it provides an equivalent
tested staging/merge publication protocol. The validated execution plan exposes the selected
delivery guarantee.

## Global relational flow

Prefer source/target pushdown. Otherwise write partitioned intermediate Arrow/Parquet data and use
a spill-capable relational engine with explicit memory, disk, and temp limits. Never disguise a
full-run Python list as streaming.

## Required result

Return final status, stage results, authoritative counts, quality results, checkpoint, output
publication state, transform/config versions, resource metrics, warnings, and a sanitized error.
