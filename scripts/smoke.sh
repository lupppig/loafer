#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# smoke.sh — run pre-deploy smoke tests against the built artifacts.
#
# This is the command to run BEFORE cutting a release. It builds the real
# artifacts (PyPI wheel and/or Docker image) and runs a full pipeline against
# each inside a clean container — the check that the dev .venv hides.
#
# Usage:
#   scripts/smoke.sh [wheel|docker|all]     (default: all)
#
# Env:
#   VERSION   version to stamp/assert. Defaults to `git describe --tags`
#             (falls back to 0.0.0+smoke). Exported to the sub-scripts.
#
# Examples:
#   scripts/smoke.sh                 # build + test wheel and docker image
#   scripts/smoke.sh wheel           # just the PyPI wheel path
#   VERSION=0.3.2 scripts/smoke.sh   # test as if releasing 0.3.2
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

TARGET="${1:-all}"

if [ -z "${VERSION:-}" ]; then
    if tag="$(git describe --tags --abbrev=0 2>/dev/null)"; then
        VERSION="${tag#v}"
    else
        VERSION="0.0.0+smoke"
    fi
fi
export VERSION

command -v docker >/dev/null 2>&1 || { echo "✗ docker is required but not found on PATH" >&2; exit 1; }

run_wheel=false
run_docker=false
case "$TARGET" in
    wheel)  run_wheel=true ;;
    docker) run_docker=true ;;
    all)    run_wheel=true; run_docker=true ;;
    *) echo "usage: scripts/smoke.sh [wheel|docker|all]" >&2; exit 2 ;;
esac

echo "═══════════════════════════════════════════════════════════════"
echo " Loafer pre-deploy smoke test"
echo " version: ${VERSION}   target: ${TARGET}"
echo "═══════════════════════════════════════════════════════════════"

wheel_result="skipped"
docker_result="skipped"

if $run_wheel; then
    if bash "${REPO_ROOT}/scripts/smoke_wheel.sh"; then
        wheel_result="PASS"
    else
        wheel_result="FAIL"
    fi
fi

if $run_docker; then
    if bash "${REPO_ROOT}/scripts/smoke_docker.sh"; then
        docker_result="PASS"
    else
        docker_result="FAIL"
    fi
fi

echo
echo "═══════════════════════════════════════════════════════════════"
echo " Summary"
printf '   wheel (PyPI):   %s\n' "$wheel_result"
printf '   docker image:   %s\n' "$docker_result"
echo "═══════════════════════════════════════════════════════════════"

if [ "$wheel_result" = "FAIL" ] || [ "$docker_result" = "FAIL" ]; then
    echo "✗ smoke test FAILED — do not deploy" >&2
    exit 1
fi
echo "✓ all selected smoke tests passed — safe to deploy"
