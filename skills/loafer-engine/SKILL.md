---
name: loafer-engine
description: Build, refactor, optimize, debug, benchmark, or review Loafer's reusable ETL/ELT data engine. Use for pipeline configuration, planning, connectors, database/API/file/PDF/web extraction, validation, transforms, loading, incremental state, schema drift, checkpoints, data correctness, bounded memory, 30–100M+ row workloads, and engine tests.
---

# Loafer Engine

Build a reusable library that knows how to plan and execute data work but knows nothing about
Typer, React, HTTP sessions, tenant memberships, or queue implementations.

## Start

1. Read [references/execution-contract.md](references/execution-contract.md).
2. Trace config → plan → graph/stage → port → adapter → result/checkpoint.
3. Classify the transform as row-local, partition-local, or global relational.
4. Define commit, retry, cancellation, and partial-output semantics before implementation.
5. Inspect `git status --short` and preserve unrelated changes.

## Protect boundaries

- Keep pure policies and value objects independent of frameworks.
- Define external systems through ports and implement them in adapters.
- Resolve connectors through the registry, never branches in agents.
- Keep ETL and ELT plans distinct while sharing safe primitives.
- Return structured events/results; never render Rich output or HTTP responses in the engine.
- Accept cancellation, event, checkpoint, and secret-resolution interfaces from the caller.
- Keep live iterators, connections, providers, and credentials out of durable state.

## Make scale an execution contract

- Pass bounded batches end to end; never collect a full run into `raw_data` or
  `transformed_data`.
- Generate AI transform artifacts once from bounded schema/sample input, version them, and execute
  them under an explicit transform class.
- Push joins, sorts, windows, aggregates, and large deduplication to a database/warehouse or a
  spill-capable engine.
- Use native bulk read/write paths, backpressure, configurable concurrency, and memory/disk limits.
- Record effective batch size, rows, bytes, duration, throughput, retries, and peak resource use.

## Preserve correctness

- Validate every batch and aggregate quality results.
- Make schema drift policy explicit: fail, evolve, quarantine, or coerce.
- Advance checkpoints only after the target effect is durable.
- Publish files atomically and use staging/merge/swap where supported.
- Document actual delivery guarantees per target and write mode.
- Quote identifiers with adapter-native APIs and bind values separately.
- Make cleanup and finalization safe after partial initialization and cancellation.

## Add connectors

Define capability flags for discovery, streaming, partitioning, incremental cursors, pushdown,
bulk load, staging, merge, upsert, and transactional publication. Fail unsupported combinations at
validation time. Require live integration tests against pinned service versions.

For PDF text/tables, scanned documents, OCR, layout extraction, page provenance, and document
quality, use `$loafer-document-extraction`. Keep document parsing behind the normal source port.

For web scraping, recursive crawling, JavaScript rendering, authenticated browser sessions, or
download discovery, use `$loafer-web-scraping`. Keep the crawler behind a source port and return
normal bounded record batches; do not embed crawling policy or browser lifecycle in an agent.

## Validate

Run unit, integration, end-to-end, and full-pipeline benchmarks proportional to the change. For a
30–100M-row claim, test memory bounds, throughput, worker kill/resume, network failure, schema
drift, disk full, cancellation, duplicates/loss, and output correctness in a pinned environment.

Never convert a connector-only benchmark into a full-engine production claim.
