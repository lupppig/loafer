# Crawler contract

## Contents

- Pipeline configuration
- Execution profiles
- Frontier and checkpoints
- Records and artifacts
- Security and policy
- UI and observability

## Pipeline configuration

Represent web ingestion as a source, not a transform:

```yaml
source:
  type: web
  seeds:
    - https://docs.example.com/
  allowed_domains:
    - docs.example.com
  render: auto
  crawl:
    strategy: breadth_first
    max_depth: 3
    max_pages: 10000
  links:
    include:
      - /guides/**
    exclude:
      - /account/**
  extract:
    fields:
      title:
        css: h1::text
      body:
        css: article
        format: text
```

Keep authentication, proxy, and browser-state values as secret/artifact references. Validation
must reject an unbounded crawl and unsupported combinations before execution.

## Execution profiles

Use one adapter contract with explicit profiles:

| Profile | Baseline | Use |
|---|---|---|
| HTTP | Crawlee `ParselCrawler` | server-rendered HTML, sitemaps, feeds, high throughput |
| Browser | Crawlee `PlaywrightCrawler` | JavaScript, login flows, interaction, infinite scroll |
| Auto | HTTP first, policy-driven browser escalation | mixed sites with a bounded escalation budget |

Do not silently escalate every failed HTTP request to a browser. Record the trigger and enforce
browser page, time, and concurrency budgets.

Support CSS/XPath extraction, attributes, text/HTML, JSON-LD, tables, links, response metadata,
pagination, sitemap seeds, and bounded user-defined parsing artifacts. Run user code under the
same sandbox policy as transforms.

## Frontier and checkpoints

For the first distributed version, assign one crawl run to one crawl worker. Let Crawlee manage its
within-run request queue and persist a resumable frontier snapshot.

Only introduce per-URL distributed fan-out after measuring that crawl-level parallelism is
insufficient. At that point:

- keep URL state in the metadata database with a workspace/crawl-scoped canonical-key constraint;
- transition `discovered → queued → leased → fetched → parsed → emitted | skipped | failed`;
- use fencing tokens for stale URL workers;
- publish URL task IDs through a transactional outbox;
- acknowledge delivery only after the page result and newly discovered links are durable.

Never use a JetStream stream alone as the queryable frontier. Operators need durable URL status,
deduplication decisions, retries, provenance, and checkpoints independent of message retention.

## Records and artifacts

Emit structured records in bounded batches. Every record includes:

```text
crawl_id, page_id, source_url, final_url, parent_url, depth
fetched_at, status_code, content_type, content_hash
extractor_version, browser_used, auth_profile_id
```

Store bodies only when configured and authorized. Put HTML snapshots, HARs, screenshots, browser
traces, and downloads in object storage with retention, encryption, access control, and size
limits. Persist references and checksums in metadata.

Send PDF artifacts to the existing PDF adapter. Preserve source URL and crawl provenance. Treat
OCR, image extraction, and document understanding as separately advertised capabilities.

## Security and policy

- Default to domain allowlists and HTTP/HTTPS only.
- Resolve hosts before connect and after every redirect.
- Deny localhost, private, link-local, multicast, and cloud metadata address ranges.
- Bind DNS results for the request or otherwise prevent resolution changes between validation and
  connect.
- Cap headers, bodies, redirects, decompression, DOM nodes, downloads, and archive expansion.
- Never expose a general-purpose interactive browser or arbitrary network proxy to users.
- Preserve `robots.txt` and terms-policy decisions as auditable crawl events.
- Separate browser worker pools by tenant trust, environment, and permitted network.

## UI and observability

Provide a source builder with seeds, scope preview, extraction rules, browser/auth mode, crawl
budgets, schedule, destination, and a bounded dry run.

Show a crawl frontier view with discovered, queued, active, fetched, parsed, skipped, blocked,
failed, and emitted counts. Add per-domain rate, depth distribution, HTTP status distribution,
browser escalation, bytes, duplicate rate, PDF/download counts, and recent blocked-policy events.

Never display raw credentials, cookies, storage state, unrestricted page bodies, or sensitive URL
query values.
