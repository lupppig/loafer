# Connector development

## Contents

- Capability contract
- Extension priorities
- Relational adapters
- Document adapters
- Verification matrix

## Capability contract

Model connector behavior as explicit capabilities rather than assumptions:

```text
source: discover, count, sample, stream, partition, incremental cursor, predicate pushdown
target: create, append, replace, upsert, stage, merge, bulk load, transactional publish
shared: test connection, schema metadata, quoting, cancellation, retry classification
```

A connector may support only a subset. Expose capability flags to validation and the UI so an
unsupported write mode or incremental strategy fails before a run starts.

Use the existing source/target ports and registry. If a database needs behavior the port cannot
express, extend the narrow contract and update every adapter; do not branch on connector names in
agents or the UI.

## Extension priorities

Treat these requested connectors as separate engineering efforts:

| Connector | Clarify/driver | Initial scope |
|---|---|---|
| MariaDB | MariaDB Connector/Python or compatible MySQL protocol after integration testing | source + target, server cursor, bulk insert, upsert |
| ClickHouse | official ClickHouse Connect client | source + target, native block streaming, insert, server-side transform |
| CouchDB | HTTP `_find`, `_all_docs`, and `_bulk_docs` APIs | source + target, bookmark pagination, bulk writes, revision/conflict policy |
| Tiger database | Resolve the exact product/protocol before coding; “TigerDB” is ambiguous | capability spike, then a dedicated adapter |
| Web crawl | Crawlee for Python with Parsel/Playwright profiles | bounded HTTP/browser crawl source, auth profiles, downloads/PDF artifacts |

Do not advertise a connector until a live integration suite passes against a pinned server
version. Protocol similarity is not proof of semantic compatibility.

## Relational adapters

- Compose identifiers with driver-native quoting APIs.
- Bind values separately from SQL text.
- Use server-side cursors or native block/result streaming.
- Support query cancellation and classify transient versus permanent errors.
- Document isolation level, transaction boundaries, and partial-write behavior.
- Prefer native bulk import/export paths for high-volume jobs.
- Parse schema-qualified objects explicitly.

For MariaDB, test compatibility separately from MySQL for types, identifier quoting, server-side
cursors, `ON DUPLICATE KEY UPDATE`, timezone behavior, and packet limits.

For ClickHouse, design around columnar blocks and asynchronous merges. Do not pretend its insert,
mutation, transaction, or deduplication semantics match PostgreSQL. Prefer source/target pushdown
and partition-aware reads for global relational transforms.

## Document adapters

For CouchDB:

- paginate without loading the full result set;
- preserve `_id` and handle `_rev` explicitly;
- define whether design documents and deleted documents are included;
- batch `_bulk_docs` writes and inspect per-document failures;
- make conflict behavior configurable: fail, retry latest revision, or quarantine;
- checkpoint a stable bookmark or changes-feed sequence where the query permits it;
- limit attachment size and stream attachments separately.

Never log document bodies by default. Sample only bounded, redacted fields.

## Verification matrix

For every new connector test:

- connection and authentication failures;
- empty, single-batch, multi-batch, and large streaming datasets;
- Unicode, reserved identifiers, nulls, decimals, timestamps/timezones, and nested values;
- schema drift and malformed rows;
- append/replace/upsert or conflict semantics;
- mid-read and mid-write disconnects;
- cancellation, retry, cleanup, and secret redaction;
- supported version matrix and optional dependency installation;
- a real container/service integration test, not only mocks.

Record rows, bytes, chunk/block sizes, duration, retries, and server version in run metadata.

For web crawling, use `$loafer-web-scraping`. Do not treat the current REST adapter as a crawler:
recursive discovery, URL canonicalization, browser lifecycle, authentication state, politeness,
frontier checkpoints, and SSRF defenses are distinct contracts.
