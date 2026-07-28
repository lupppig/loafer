---
name: loafer-document-extraction
description: Design, implement, harden, debug, benchmark, or review Loafer PDF and document ingestion. Use for native PDF text, tables, scanned documents and OCR, page/layout extraction, password-protected files, document metadata and provenance, downloaded web documents, document worker isolation, quality reporting, API/UI document sources, and extraction tests.
---

# Loafer Document Extraction

Build documents as a versioned source capability that emits bounded record batches with page-level
provenance. Keep parsing, OCR, credentials, and document bytes out of web/API processes.

## Start

1. Read [references/document-contract.md](references/document-contract.md).
2. Inspect the current PDF config, adapter, registry, source port, batch contract, and tests.
3. Classify the input as native text, table-heavy, scanned/image-only, mixed, encrypted, or
   malformed.
4. Define output granularity, required provenance, quality thresholds, limits, and failure policy.
5. State clearly whether the requested mode is implemented, experimental, or roadmap-only.

## Preserve the source boundary

- Treat the current `pdfplumber` adapter as native text/table extraction, not general document AI.
- Return ordinary bounded Loafer batches; do not create a PDF-only execution engine.
- Emit stable document, page, block/table, and source artifact identifiers.
- Preserve page number, page dimensions, extraction method/version, source hash, source URI, and
  crawl provenance when the document came from a web run.
- Put document binaries, page images, and OCR artifacts in object storage; put only references and
  safe metadata in control-plane records, queues, events, and logs.
- Route documents discovered by crawlers through this capability rather than parsing them inside
  browser workers.

## Select extraction deliberately

- Use native parsing first for PDFs with a usable text layer.
- Enable table extraction only when requested; expose table boundaries and confidence/quality
  metadata instead of flattening silently.
- Escalate image-only or low-text pages to a separately packaged OCR worker when OCR is enabled.
- Keep OCR providers behind a port. Support a local open-source baseline before optional managed
  services, and never make an external AI service mandatory.
- Do not claim handwriting, semantic form understanding, or arbitrary layout understanding without
  an implemented adapter, evaluation corpus, limits, and documented confidence behavior.

## Bound and isolate work

- Enforce file byte, page, pixel, decompression, object-count, elapsed-time, memory, and output
  limits before and during parsing.
- Process page ranges as retryable partitions; never materialize an unbounded document collection.
- Run parsers/OCR in isolated document-worker pools with no control-plane access and tightly scoped
  temporary storage.
- Resolve passwords from tenant-scoped secret references at the worker. Never store passwords in
  pipeline config, filenames, events, logs, or previews.
- Detect malformed, suspicious, and unsupported files; quarantine them with a safe reason.

## Make quality observable

- Report pages discovered, parsed, OCRed, skipped, failed, and retried.
- Report text characters, tables, rows, blocks, images, extraction latency, OCR confidence, and
  low-quality page counts where the selected adapter can measure them.
- Support `fail | continue | quarantine` policy for page-level failures and quality thresholds.
- Keep extracted samples bounded and redacted. Never render raw document HTML or unsafe embedded
  content in the UI.

## Integrate across Loafer

- Use `$loafer-engine` for source and bounded-batch behavior.
- Use `$loafer-workers` for leases, retries, isolation, and resource budgets.
- Use `$loafer-web-scraping` for discovered/downloaded document provenance.
- Use `$loafer-api-design` for versioned source config, preview jobs, artifacts, and permissions.
- Use `$loafer-web-ui` for document setup, preview, progress, quality, and failure states.

## Validate

Test native text, tables on/off, image-only pages, mixed documents, rotated pages, empty pages,
Unicode, encrypted files, wrong passwords, malformed/truncated files, oversized files, page/output
limits, cancellation, timeout, parser crash, OCR crash, retries, duplicate delivery, redaction,
cross-tenant artifact access, and crawler-discovered PDFs.

Maintain a versioned legal fixture corpus with expected page counts, normalized text/table
assertions, provenance checks, and quality tolerances. Benchmark throughput and peak RSS by page
count, page complexity, and OCR mode.
