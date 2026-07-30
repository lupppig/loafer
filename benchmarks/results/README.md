# Full-pipeline benchmark artifacts

The `1m.json` and `10m.json` reports record the Phase 0 release baseline for Loafer `v0.4.0`.

## Provenance

- Date: 2026-07-30
- Git revision: `9007746634da06b77fea5d6246bb35c3dff2a978` (`v0.4.0`)
- Working tree before the run: clean
- Container: production `docker/Dockerfile` built from that revision
- Container image ID:
  `sha256:0efbede1d2ef2a043a29ef1bdd12f9cbd46ff3d91793e63ae314633f82d4aafe`
- Container runtime: Python 3.11.15, Loafer 0.4.0, Linux x86-64
- Host: Linux 6.12.96, Intel Core i7-7600U (4 logical CPUs), 15 GiB RAM
- Docker limit: 3 GiB memory / 6 GiB memory-plus-swap
- Harness limits: 2,048 MiB process-tree RSS, 1,536 MiB sandbox memory,
  1,800 seconds
- Workload: deterministic four-column CSV, custom identity transform,
  chunk size 1,000

The production image does not include Git. The harness's revision lookup was
provided by a read-only helper after the host had verified that `HEAD` was the
clean, exact `v0.4.0` tag. Runtime dependencies are captured by the immutable
container image ID and originate from the Dockerfile at the recorded revision.

## Results

| Report | Expected interpretation |
|---|---|
| [`1m.json`](1m.json) | Success: exact 1,000,000-row count and SHA-256 equality, with no temporary output left behind. |
| [`10m.json`](10m.json) | Expected safety cutoff: process-tree RSS exceeded 2 GiB; no final or temporary output was published. |

Input generation is excluded from pipeline wall time. The 10M report has
`correct: false` because the requested pipeline did not complete; for the Phase
0 workload-envelope gate, its safe termination and lack of published output are
the expected evidence.

## Bounded row-local development gate

[`30m-row-local.json`](30m-row-local.json) records the Phase 2 implementation gate run on
2026-07-30. A deterministic CSV → custom identity → CSV pipeline processed 30,000,000 rows with
chunk size 10,000 under a 512 MiB process-tree RSS limit and a 256 MiB sandbox limit.

The run completed in 1,950.86 seconds at 15,377.85 rows/second with 101.09 MiB peak process-tree
RSS. All 30,000,000 output rows were counted, the 1,140,588,914-byte input and output SHA-256
digests matched, and no temporary output remained.

This artifact honestly records `git_worktree_dirty: true`: it validates the implementation in this
change set, not a clean tagged release or production container. Before a release workload claim,
repeat the 1M/10M/30M curve from the committed revision in a pinned production image and retain the
image/dependency provenance beside these results.
