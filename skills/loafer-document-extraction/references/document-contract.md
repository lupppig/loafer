# Document extraction contract

## Contents

- Current capability
- Source configuration
- Record and artifact model
- Extraction profiles
- Safety and limits
- Quality and failure policy
- API and UI behavior
- Validation matrix

## Current capability

The repository currently implements a local PDF source with `pdfplumber`. It auto-detects `.pdf`,
streams one record per page, and can include extracted tables. Unit tests cover connection, page
counting, page records, table enable/disable behavior, chunking, missing files, and cleanup.

This is native PDF text/table extraction. It is not OCR, handwriting recognition, semantic form
understanding, image extraction, or a hosted document-processing service.

## Source configuration

Keep the existing simple form compatible:

```yaml
source:
  type: pdf
  path: ./documents/invoice.pdf
  extract_tables: true
```

The platform form should reference an uploaded or discovered artifact:

```yaml
source:
  type: document
  artifact_ref: artifact_01...
  profile: auto
  output_granularity: page
  table_mode: detect
  ocr:
    enabled: true
    languages: [eng]
  limits:
    max_bytes: 104857600
    max_pages: 2000
    timeout_seconds: 900
  failure_policy: quarantine
```

Treat exact field names as an API/config design decision. Preserve the semantic requirements:
immutable input reference, explicit extraction profile, bounded work, quality policy, and output
contract.

## Record and artifact model

Every emitted record must include enough provenance to reproduce and debug extraction:

| Field | Purpose |
|---|---|
| `document_id` | Stable ID for one immutable source artifact |
| `document_sha256` | Content identity and deduplication input |
| `page_number` | One-based source page |
| `record_kind` | `page_text`, `block`, `table`, or another declared kind |
| `content` | Extracted bounded content or structured table |
| `extraction_method` | Native parser, OCR adapter, or fallback path |
| `extractor_version` | Reproducibility and reprocessing |
| `quality` | Adapter-supported confidence and warnings |
| `source_uri` | Safe source reference without embedded credentials |
| `crawl_run_id` / `source_url` | Optional discovery provenance |

Store the original document, optional page images, parser diagnostics, and OCR outputs as
tenant-scoped artifacts. Queue messages carry only IDs and routing metadata.

## Extraction profiles

### Native

Use for PDFs with a reliable text layer. Extract text page by page and optionally tables. This is
the current baseline.

### OCR

Use for image-only pages. Package OCR dependencies separately because models and native libraries
make workers larger and more resource intensive. Put the OCR engine behind a port. Prefer an
open-source local profile; make managed services explicit optional adapters with data-residency and
cost controls.

### Auto

Try native extraction, measure usable text/table output, then escalate only low-text or image-only
pages to OCR. Record the decision per page. Never run both paths blindly across every document.

### Layout-aware

Use only when the product needs blocks, reading order, coordinates, or forms. Version the output
schema and evaluation corpus. Do not market this profile until quality and failure behavior are
measured.

## Safety and limits

Validate magic bytes and detected media type; do not trust extensions. Bound:

- input bytes and pages;
- rendered pixel count and DPI;
- nested/compressed object expansion;
- parser/OCR CPU, RSS, processes, disk, and elapsed time;
- extracted characters, tables, cells, images, and artifacts;
- concurrent pages and documents per tenant/pool.

Use isolated, non-root document workers with read-only roots, disposable temp directories, no
control-plane network access, and an egress policy appropriate to the selected adapter. Quarantine
suspicious inputs. Treat parsers as untrusted native-code dependencies and track their advisories.

## Quality and failure policy

Quality is not one universal confidence score. Record adapter-specific evidence such as:

- empty or low-text pages;
- replacement/control character rate;
- OCR confidence distribution;
- table cell/row counts and extraction warnings;
- native-to-OCR fallback count;
- pages skipped or failed.

Apply declared thresholds and one of:

- `fail`: stop the document/run;
- `continue`: emit successful pages and visible warnings;
- `quarantine`: isolate failed pages/documents for review.

Never silently return an empty successful document when pages failed.

## API and UI behavior

The control plane should support upload/discovery registration, immutable artifact lookup,
versioned document-source configuration, bounded asynchronous preview, run execution, quality
summary, page-level events, and authorized artifact download.

The UI should show:

- extraction profile and whether OCR is available;
- file/page limits before execution;
- native parse → OCR fallback → structure → emit progression;
- pages parsed/OCRed/failed, tables/rows, throughput, and warnings;
- page preview with provenance and explicit sample/redaction labels;
- quarantine and reprocess actions gated by permission.

Never upload or parse documents in the Next.js server process beyond bounded transfer/proxy work.

## Validation matrix

Cover at least:

| Dimension | Cases |
|---|---|
| Content | native text, tables, scanned, mixed, empty, Unicode |
| Geometry | rotated, landscape, unusual sizes, many pages |
| Protection | encrypted, wrong password, unsupported encryption |
| Corruption | truncated, malformed objects, decompression bomb |
| Limits | bytes, pages, pixels, time, memory, output |
| Lifecycle | cancel, retry, worker kill, duplicate delivery, resume |
| Security | malicious file, secret redaction, tenant artifact isolation |
| Provenance | upload, local path, crawler download, hash deduplication |

Compare normalized outputs rather than brittle byte-for-byte layout unless exact layout is the
declared contract.
