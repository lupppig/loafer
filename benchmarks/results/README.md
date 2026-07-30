# Phase 0 release benchmark baseline

These reports close the Phase 0 benchmark gate for Loafer `v0.4.0`.

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
