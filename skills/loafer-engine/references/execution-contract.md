# Execution contract

## Batch envelope

```text
run_id, stage_id, partition_id, batch_id, attempt
source_position_start, source_position_end
schema_version, transform_artifact_version
rows_in, rows_out, rows_rejected
bytes_in, bytes_out, checksum
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

## Global relational flow

Prefer source/target pushdown. Otherwise write partitioned intermediate Arrow/Parquet data and use
a spill-capable relational engine with explicit memory, disk, and temp limits. Never disguise a
full-run Python list as streaming.

## Required result

Return final status, stage results, authoritative counts, quality results, checkpoint, output
publication state, transform/config versions, resource metrics, warnings, and a sanitized error.
