---
name: loafer-web-ui
description: Design, build, refactor, or review Loafer's Next.js web application, artsy 3D landing page, and multi-tenant operator dashboard. Use for App Router architecture, infrastructure-focused brand design, pipeline and web-crawl authoring, connection management, run history, live execution visualization, metrics, logs, quality, onboarding, responsive behavior, accessibility, visual systems, and frontend API integration.
---

# Loafer Web UI

Build the browser as an authenticated Next.js control-plane client. Never execute pipelines, hold
source or target database credentials, or infer authoritative run state in browser code.

## Start

1. Inspect `web/app`, `web/src`, route groups, server/client boundaries, tokens, shared components,
   and the current API boundary.
2. Read [references/dashboard-contract.md](references/dashboard-contract.md) for dashboard and run
   detail work.
3. Read [references/visual-direction.md](references/visual-direction.md) for landing-page, 3D,
   illustration, mascot, or brand-system work.
4. Use `$loafer-web-scraping` for crawler configuration, frontier, and crawl-run semantics.
5. Confirm whether data is live, mocked, or unavailable. Label non-live surfaces.
6. Inspect `git status --short` and preserve unrelated changes.

## Apply the product model

- Scope navigation and requests to organization, workspace, and environment.
- Use one information architecture: Overview, Pipelines, Runs, Connections, Explorer, Quality,
  Artifacts, Members, Settings.
- Offer guided, YAML/code, and lineage views without making a node canvas the only editor.
- Make preview, dry run, deployment, and production execution distinct states.
- Keep URLs deep-linkable and browser history meaningful.
- Hide actions the user cannot perform and explain disabled actions when context is missing.

## Build the operations dashboard

- Display persisted metrics with visible time range, units, freshness, and empty/error states.
- Prioritize active/failed runs, queue health, freshness SLA, throughput, rejected rows, and worker
  capacity over decorative totals.
- Make every metric link to the filtered runs or pipelines that explain it.
- Render extract → validate → transform → load → verify from sequenced run events.
- Show queued, running, succeeded, warning, failed, cancelled, and retrying with text and icons,
  never color alone.
- Provide a detailed searchable log/event viewer with pause-follow, filters, trace correlation,
  wrapping, copy, redacted download, and reconnect from the last sequence.

## Keep the visual identity useful

- Make Loafer feel like an infrastructure instrument: tactile, engineered, calm, and memorable.
- Prefer a neutral operational palette, strong typography, crisp borders, dense tables, material
  textures, and restrained signal colors.
- Avoid generic AI gradients, glassmorphism everywhere, fake neon metrics, and oversized cards.
- Use a coherent 3D pipeline/foundry world and one occasional mascot for the landing page,
  onboarding, empty states, and run-stage context. Never scatter unrelated 3D assets.
- Make 3D communicate flow, scale, state, or architecture. Never let animation replace the
  accessible status model or delay primary content.
- Respect reduced motion, high contrast, keyboard navigation, focus visibility, and 200% zoom.
- Design intentionally for laptop widths, large operations displays, tablets, and failure states.

## Build public product surfaces

- Lead with the real outcome, supported workloads, self-hosting model, and honest project status.
- Show source → extract → validate → transform → load → verify as a physical data system, with
  crawl/download/document branches where relevant.
- Demonstrate the actual product through pipeline authoring, run monitoring, detailed logs, and
  recovery states instead of abstract AI claims.
- Use Supabase for approachable developer navigation, Databricks for data-product density, and
  Firebase for guided onboarding only as interaction references. Do not copy their visual systems.
- Keep primary headings, calls to action, and proof available without WebGL. Lazy-load 3D as a
  progressive enhancement with static poster and reduced-motion fallbacks.

## Integrate safely

- Use the Next.js App Router. Keep pages and layouts as Server Components by default; introduce
  narrow Client Component boundaries only for state, events, custom hooks, or browser APIs.
- Use Next.js metadata exports, `next/link`, route loading/error boundaries, and server-side data
  access instead of recreating framework primitives.
- Treat Route Handlers as a browser-facing gateway/BFF. They may host Better Auth and proxy
  authenticated control-plane requests, but must not become a second scheduler or execution engine.
- Keep secrets and privileged API credentials in server-only modules. Only expose intentionally
  public values through `NEXT_PUBLIC_*`.
- Generate or consume typed API clients from the versioned OpenAPI contract.
- Keep server state in a query/cache layer and local interaction state in components.
- Reconcile SSE/WebSocket events by sequence number; refetch after gaps.
- Never render raw HTML, secret values, unrestricted samples, or unredacted stack traces.
- Require explicit confirmation for replace, drop, backfill, production promotion, and secret
  rotation.

## Structure code

- Keep App Router pages, layouts, and Route Handlers thin.
- Group public marketing/docs routes separately from authenticated application routes.
- Group domain components, hooks, types, and fixtures by feature.
- Extract repeated status, metric, log, table, and empty-state primitives.
- Keep fixtures visibly separate from production clients.
- Avoid components that combine fetching, orchestration, large static datasets, and rendering.

## Validate

Run:

```bash
cd web
npm run lint
npm run build
```

Add component tests for state transitions and accessibility, contract tests for API/event parsing,
and end-to-end tests for author, run, crawl, observe, cancel, retry, and tenant-switch workflows.
Verify loading, partial, empty, disconnected, permission-denied, reduced-motion, WebGL-unavailable,
and failure states.
