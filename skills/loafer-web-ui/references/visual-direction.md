# Loafer visual direction

## Contents

- Brand idea
- Landing-page composition
- 3D and illustration
- Application surfaces
- Anti-patterns
- Performance and accessibility

## Brand idea

Make Loafer feel like a data foundry and routing yard: a precise place where raw inputs arrive,
move through visible machinery, and leave as trusted outputs.

Use a tactile editorial system:

- graphite, ink, warm off-white, steel, and muted earth surfaces;
- small signal accents such as safety orange, electric blue, or instrument green;
- technical sans-serif typography paired with a distinctive display face or mono labels;
- blueprint lines, registration marks, stamped labels, grids, paper/metal grain, and physical
  depth;
- confident whitespace and asymmetric editorial composition.

Do not make the brand look like a generic AI product. Avoid purple-blue mesh gradients, glowing
orbs, random glass cards, star fields, and “magic” sparkles.

## Landing-page composition

Use this narrative:

1. Hero: a concise data-infrastructure promise, primary self-host/get-started action, and a
   meaningful 3D foundry scene.
2. Execution proof: a real pipeline moves through extract, validate, transform, load, and verify
   with rows, bytes, timing, and logs.
3. Authoring: guided, YAML/SQL/Python, and crawl-source examples for different data personas.
4. Sources and targets: databases, APIs, files, PDFs, and web crawls without unsupported claims.
5. Operations: schedules, retries, checkpoints, quality, lineage, logs, and worker health.
6. Self-hosting: one-host startup topology expanding into isolated enterprise worker pools.
7. Security and tenancy: secret references, workspace isolation, auditability, and deployment
   control.
8. Open source: repository, documentation, verified capabilities, and honest roadmap status.

Show actual UI fragments and operational evidence. Do not use fake customer logos, fabricated
metrics, or generic feature-card grids as the main proof.

## 3D and illustration

Use one coherent world:

- sources are physical docks, tanks, files, browser windows, or document crates;
- bounded record batches are visible parcels moving through pipes or conveyors;
- validation gates inspect parcels;
- transformations reshape or sort them;
- targets receive sealed, counted outputs;
- warning, retry, and quarantine branches are physically legible.

An occasional cartoon operator or courier may guide onboarding and empty states. Keep the character
clever and restrained, not childish, and never use it as the only carrier of status.

For live runs, drive 3D state from the same sequenced events as the accessible timeline. Use
discrete state changes, not a decorative infinite loop that suggests work is happening.

## Application surfaces

Let the authenticated product become denser and more instrument-like than the landing page:

- strong left navigation and workspace/environment context;
- compact tables, inspectors, timelines, code, metrics, and logs;
- restrained elevation with sharp boundaries;
- color reserved for state, severity, selection, and actions;
- crawl runs show frontier depth, domains, browser escalation, downloads/PDFs, and blocked-policy
  events.

The public brand and operations UI should share typography, materials, iconography, and motion, but
the dashboard must prioritize scanning and diagnosis over spectacle.

## Anti-patterns

- generic AI gradients or neon “intelligence” effects;
- a Supabase, Databricks, or Firebase visual clone;
- an oversized node canvas as the whole product;
- floating cards without hierarchy;
- 3D blobs unrelated to data movement;
- mascots repeated in every section;
- animation that hides errors, slows navigation, or consumes sustained CPU;
- charts or run states backed by fabricated data.

## Performance and accessibility

- Render meaningful HTML and primary calls to action before 3D initializes.
- Load WebGL only on routes and viewports that use it; prefer one canvas and dispose resources.
- Provide a static responsive poster for slow devices, data-saving mode, crawler rendering,
  WebGL failure, and print.
- Pause work when off-screen or backgrounded.
- Honor `prefers-reduced-motion` with a static or step-based representation.
- Preserve keyboard navigation, semantic headings, contrast, zoom, and screen-reader stage text.
- Measure LCP, INP, CLS, transferred JavaScript, GPU/CPU time, and memory on representative mobile
  and laptop hardware before release.
