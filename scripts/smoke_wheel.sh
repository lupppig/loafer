#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# smoke_wheel.sh — build the PyPI wheel and test it in a CLEAN container.
#
# This mirrors exactly what a `pip install loafer-etl` user gets: the built
# wheel is installed into a bare python:3.11-slim image with NO dev extras and
# NO access to this repo's .venv. If a runtime dependency is undeclared (the
# BUG-1 class), `pip install` resolves only what pyproject declares and the
# first `loafer` command crashes — here, loudly, instead of in production.
#
# Usage:
#   scripts/smoke_wheel.sh
#
# Env:
#   VERSION   version to stamp into the build and assert via --version.
#             Defaults to `git describe --tags` (falls back to 0.0.0+smoke).
#   PY_IMAGE  base image for the clean room (default python:3.11-slim).
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PY_IMAGE="${PY_IMAGE:-python:3.11-slim}"

# Resolve the version the same way docker.yml / publish.yml do, so what we test
# matches what a release would publish.
if [ -z "${VERSION:-}" ]; then
    if tag="$(git describe --tags --abbrev=0 2>/dev/null)"; then
        VERSION="${tag#v}"
    else
        VERSION="0.0.0+smoke"
    fi
fi

echo "▶ Building wheel (version ${VERSION})…"
rm -rf dist
SETUPTOOLS_SCM_PRETEND_VERSION_FOR_LOAFER_ETL="${VERSION}" \
SETUPTOOLS_SCM_PRETEND_VERSION="${VERSION}" \
    uv build --wheel

wheel="$(ls -1 dist/*.whl 2>/dev/null | head -n1)"
[ -n "$wheel" ] || { echo "✗ no wheel built in dist/" >&2; exit 1; }
echo "  built: ${wheel}"

echo "▶ Installing into a clean ${PY_IMAGE} and running the smoke pipeline…"
# Mount the wheel and the smoke harness read-only; the container copies the
# smoke fixtures to a writable /tmp dir (the run writes an output file) and runs
# the shared assertions. EXPECTED_VERSION enforces the artifact reports the
# version we built.
docker run --rm \
    -v "${REPO_ROOT}/dist:/artifacts:ro" \
    -v "${REPO_ROOT}/scripts:/harness:ro" \
    -e EXPECTED_VERSION="${VERSION}" \
    "${PY_IMAGE}" \
    bash -c '
        set -euo pipefail
        pip install --no-cache-dir --quiet /artifacts/*.whl
        cp -r /harness/smoke /tmp/smoke
        cp /harness/smoke_test.sh /tmp/smoke_test.sh
        chmod +x /tmp/smoke_test.sh
        EXPECTED_VERSION="'"${VERSION}"'" /tmp/smoke_test.sh /tmp/smoke
    '

echo "✓ wheel smoke passed"
