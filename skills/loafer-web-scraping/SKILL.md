---
name: loafer-web-scraping
description: Design, implement, harden, debug, or review Loafer web scraping and crawling ingestion. Use for HTTP and JavaScript-rendered sites, recursive crawls, selectors, pagination, authenticated sessions, browser automation, discovered files and PDFs, crawl frontiers, URL deduplication, proxies, politeness, anti-SSRF controls, crawl checkpoints, NATS JetStream delivery, and scraping tests.
---

# Loafer Web Scraping

Build web ingestion as a versioned source capability executed by isolated workers. Do not put a
crawler, browser, credentials, or crawl frontier in the Next.js process.

## Start

1. Read [references/crawler-contract.md](references/crawler-contract.md).
2. Read [references/nats-jetstream.md](references/nats-jetstream.md) for distributed delivery.
3. Inspect Loafer's source port, registry, batch envelope, worker lease, secret references, event
   schema, and existing REST/PDF adapters.
4. Classify the request as one URL, paginated extraction, bounded recursive crawl, authenticated
   crawl, browser crawl, or file/document discovery.
5. Define domain scope, budgets, data contract, checkpoint, and publication semantics first.

## Use an integration, not a home-grown crawler

- Use Crawlee for Python as the initial library baseline behind a Loafer adapter.
- Prefer `ParselCrawler` for HTML that does not require JavaScript.
- Use `PlaywrightCrawler` only for client-rendered content, browser login, interaction, infinite
  scroll, or downloads that require a real browser.
- Keep Crawlee behind a Loafer crawl port so the engine can be replaced without changing pipeline
  configuration or run semantics.
- Let Crawlee own within-job request scheduling, sessions, retries, browser pooling, and link
  discovery. Do not create a competing scheduler in agents.
- Pin library and browser versions and package browser workers separately from lightweight workers.

## Model a crawl explicitly

- Version seed URLs, allowed domains, include/exclude patterns, render mode, extraction rules,
  pagination/link rules, authentication flow, headers, limits, and output schema.
- Normalize and deduplicate canonical URLs while retaining the fetched URL, redirect chain, and
  canonicalization reason.
- Bound depth, pages, bytes, duration, redirects, retries, per-host concurrency, browser contexts,
  downloads, and discovered links.
- Emit records with provenance: source URL, fetched time, status, content type, content hash,
  extractor version, parent URL, depth, and crawl/run IDs.
- Checkpoint handled and pending requests plus session-safe metadata. Resume without refetching
  committed pages unless the refresh policy requests it.

## Handle authentication and documents safely

- Resolve Basic, bearer, header, cookie, form-login, and browser-login secrets at the worker.
- Store reusable browser state only as an encrypted, tenant-scoped artifact with expiry and
  revocation. Never place cookies, passwords, tokens, or storage state in config, queues, events,
  logs, or samples.
- Isolate browser contexts per crawl identity and close them deterministically.
- Treat downloaded files as artifacts. Route discovered PDFs through the existing PDF source
  capability and use `$loafer-document-extraction` for text, tables, OCR, quality, and provenance.
- Do not claim OCR for scanned PDFs until an OCR adapter, limits, quality reporting, and tests
  exist.

## Protect the platform

- Block loopback, link-local, private, metadata-service, non-HTTP, and disallowed redirect targets
  by default. Re-resolve and revalidate every redirect to resist DNS rebinding.
- Honor explicit tenant egress policy, robots policy, legal authorization, rate limits, and
  per-domain politeness. Do not implement CAPTCHA defeating or access-control bypass.
- Sandbox browsers with unprivileged users, read-only roots, temporary profiles, network policy,
  CPU/RSS/time/process limits, and no control-plane network access.
- Limit response sizes, decompression ratios, DOM size, screenshots, traces, downloads, and log
  bodies. Quarantine suspicious files.
- Redact URLs, query values, headers, forms, cookies, page samples, and browser traces before they
  leave the worker.

## Integrate with Loafer

- Return bounded record batches through the normal source contract.
- Keep crawl page/frontier state authoritative in the metadata store; keep large bodies, browser
  traces, screenshots, and downloads in object storage.
- Use NATS JetStream for distributed job/task delivery only through a queue port.
- Keep local CLI mode usable without NATS; use an embedded local frontier for one-process runs.
- Surface discovered, queued, fetched, skipped, retried, blocked, failed, parsed, and emitted
  counts plus HTTP/browser latency, bytes, depth, domain, and checkpoint lag.
- Expose crawl configuration and progress through the same tenant-aware API, event, log, audit,
  quota, and permission contracts as other Loafer runs.

## Validate

Test static HTML, JavaScript rendering, pagination, infinite scroll, login/session expiry, redirects,
downloaded PDFs, duplicate/canonical URLs, resume, cancellation, browser crash, proxy failure,
timeouts, malformed HTML, huge responses, decompression bombs, private-network targets, DNS
rebinding, cross-tenant session access, redaction, queue redelivery, and worker kill.

Use controlled test sites and fixtures. Never run broad or authenticated crawls against a third
party without explicit authorization.
