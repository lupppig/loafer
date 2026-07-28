#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# smoke_docker.sh — build the Docker image and test it end-to-end.
#
# Builds docker/Dockerfile with the same pinned-version build-args the real
# docker.yml release workflow passes, then runs the shared smoke assertions
# against the baked-in `loafer` binary — the exact artifact users `docker pull`.
#
# We override the image ENTRYPOINT (normally `loafer`) with bash so we can reuse
# scripts/smoke_test.sh, pointing LOAFER at the loafer on the image's PATH
# (/app/.venv/bin). This still exercises the real installed binary inside the
# real image, not a host install.
#
# Usage:
#   scripts/smoke_docker.sh
#
# Env:
#   VERSION   version to build/assert (default: git describe --tags, else
#             0.0.0+smoke).
#   IMAGE     image tag to build (default loafer:smoke).
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

IMAGE="${IMAGE:-loafer:smoke}"

if [ -z "${VERSION:-}" ]; then
    if tag="$(git describe --tags --abbrev=0 2>/dev/null)"; then
        VERSION="${tag#v}"
    else
        VERSION="0.0.0+smoke"
    fi
fi

echo "▶ Building image ${IMAGE} (version ${VERSION})…"
docker build \
    -f docker/Dockerfile \
    --build-arg "SETUPTOOLS_SCM_PRETEND_VERSION_FOR_LOAFER_ETL=${VERSION}" \
    -t "${IMAGE}" \
    .

echo "▶ Running the smoke pipeline against ${IMAGE}…"
# Override the `loafer` entrypoint with bash so the shared assertions can drive
# multiple commands. The smoke fixtures are copied to a writable /tmp dir (the
# pipeline writes an output file). LOAFER resolves to the image's baked-in binary.
docker run --rm \
    --entrypoint bash \
    -v "${REPO_ROOT}/scripts:/harness:ro" \
    -e EXPECTED_VERSION="${VERSION}" \
    "${IMAGE}" \
    -c '
        set -euo pipefail
        cp -r /harness/smoke /tmp/smoke
        cp /harness/smoke_test.sh /tmp/smoke_test.sh
        chmod +x /tmp/smoke_test.sh
        LOAFER=loafer EXPECTED_VERSION="'"${VERSION}"'" /tmp/smoke_test.sh /tmp/smoke
    '

echo "✓ docker smoke passed"
