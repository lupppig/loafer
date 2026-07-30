# Changelog

Notable changes to Loafer are documented here. This project follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A framework-independent application service with strict, JSON-roundtrippable contracts for run
  requests, execution plans, batch envelopes, events, snapshots, and results.
- Runtime ports and local adapters for cancellation, checkpoints, secret resolution, event
  publication, and interactive transform review.

### Changed

- The CLI, scheduler, and legacy Python runner now share the same application boundary while core
  execution orchestration remains independent of client frameworks.
- Durable application contracts now exclude credentials, connector instances, iterators, provider
  clients, row payloads, and other ephemeral runtime objects.

## [0.4.0] - 2026-07-29

### Added

- A 45-case deterministic AI transform regression suite with optional PostgreSQL ELT
  integration coverage.
- A deterministic full-pipeline benchmark that measures process-tree peak RSS, enforces memory
  and timeout limits, and verifies output row counts and SHA-256 checksums.
- Adversarial tests for PostgreSQL identifiers, schema-qualified tables, incremental cursor
  identifiers, atomic file publication, concurrent file creation, and failed transforms.
- A measured workload envelope and phased production-readiness roadmap.

### Changed

- AI transform providers now receive optional custom transform code as part of the shared provider
  contract, and `custom_first` prompts use schema metadata from the custom transform's output.
- Non-query sources (CSV, Excel, MongoDB, and PDF) now support cursor-based incremental extraction
  through chunked client-side filtering with an explicit full-scan warning.
- Updated provider defaults to `gemini-3.6-flash`, `claude-sonnet-5`, `gpt-5.6-terra`, and
  `qwen3.7-plus`; omitted model values now resolve from the selected provider.
- Updated OpenAI Chat Completions requests to use `max_completion_tokens` for current model
  compatibility and disabled Claude Sonnet 5 adaptive thinking so the configured output budget
  remains available for generated code and SQL.
- CSV and JSON targets now write to same-directory temporary files, flush them to disk, and publish
  them atomically only after a successful run.
- PostgreSQL target and ELT table operations now use driver-native SQL identifier composition.
- PostgreSQL ELT replacement operations now commit or roll back as one transaction.
- Target connector contexts no longer finalize output when pipeline execution raises an exception.

### Fixed

- Replaced the POSIX sandbox's multiprocessing spawn worker with a dedicated internal module
  process, preventing unguarded programmatic `run_pipeline()` calls from recursively re-importing
  the caller and hanging.
- Isolated ELT raw loads in unique schema-local staging tables, cleaned them after terminal success
  or failure, and made exhausted ELT retries fail the pipeline instead of returning success.
- Initialized an LLM provider for multi-step transforms containing enabled AI steps.
- Retried sandbox execution failures with traceback feedback while failing fast on permanent HTTP
  4xx provider errors.
- Computed model-facing schema statistics across the full materialized input while keeping sample
  values bounded, and preserved incremental watermarks after streamed consumption.
- Prevented failed CSV and JSON runs from replacing existing output or leaving a final partial file.
- Prevented `write_mode: error` races from overwriting a file created by another process.
- Rejected malformed PostgreSQL target names while preserving supported `table` and
  `schema.table` forms.
- Escaped engine-specific quote characters in incremental cursor identifiers.

### Known limitations

- Python and AI transform paths still materialize a complete run in memory. A measured four-column
  identity pipeline completed 1M rows at approximately 1.15 GiB peak process-tree RSS; a 10M-row
  run exceeded a 2 GiB limit and was terminated without publishing output.
- Direct PostgreSQL ETL target loads can still retain partial database writes after a failed run.
  Durable checkpoint/retry semantics and staged publication for that path are planned work.

[Unreleased]: https://github.com/lupppig/loafer/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/lupppig/loafer/compare/v0.3.1...v0.4.0
