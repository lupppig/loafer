#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# smoke_test.sh — assert that an INSTALLED `loafer` works end-to-end.
#
# This script builds nothing. It assumes `loafer` is already on PATH (installed
# from a wheel into a clean container, or baked into the Docker image) and runs
# a real pipeline against it. It is the shared core reused by smoke_wheel.sh,
# smoke_docker.sh, and CI.
#
# Why it exists: the dev .venv pulls in transitive deps (e.g. `click`) that the
# published artifact may not declare, so everything passes locally and the
# shipped wheel/image crashes on every command. Running the *installed* artifact
# in a clean room is the only thing that catches that class of bug (BUG-1).
#
# Usage:
#   scripts/smoke_test.sh <smoke_dir>
#
# Env:
#   EXPECTED_VERSION   if set, `loafer --version` must print exactly this.
#                      if unset, the version is printed but not asserted.
#   LOAFER             override the loafer command (default: "loafer").
#
# Exits non-zero on the first failed assertion.
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SMOKE_DIR="${1:?usage: smoke_test.sh <smoke_dir>}"
LOAFER="${LOAFER:-loafer}"
EXPECTED_ROWS=10  # data rows in scripts/smoke/sample.csv (passthrough -> same count)

CONFIG="${SMOKE_DIR}/pipeline.smoke.yaml"
OUTPUT="${SMOKE_DIR}/out/smoke_output.json"

pass() { printf '  \033[32m✓\033[0m %s\n' "$1"; }
fail() { printf '  \033[31m✗ %s\033[0m\n' "$1" >&2; exit 1; }

echo "── loafer smoke test ──────────────────────────────────────────"
echo "loafer:   $(command -v "$LOAFER" 2>/dev/null || echo "$LOAFER")"
echo "config:   $CONFIG"
echo "───────────────────────────────────────────────────────────────"

# Fresh output dir so a stale file can't make a broken run look successful.
rm -rf "${SMOKE_DIR}/out"
mkdir -p "${SMOKE_DIR}/out"

# 1) --version : proves the entry point loads and all imports resolve (the BUG-1
#    crash, `ModuleNotFoundError: click`, happens right here on a broken build).
version_out="$("$LOAFER" --version 2>&1)" \
    || fail "--version failed (exit $?): ${version_out}"
echo "    version line: ${version_out}"
if [ -n "${EXPECTED_VERSION:-}" ]; then
    if [ "${version_out}" = "loafer ${EXPECTED_VERSION}" ]; then
        pass "--version == 'loafer ${EXPECTED_VERSION}'"
    else
        fail "--version mismatch: got '${version_out}', want 'loafer ${EXPECTED_VERSION}'"
    fi
else
    pass "--version runs"
fi

# 2) --help
"$LOAFER" --help >/dev/null 2>&1 || fail "--help failed"
pass "--help runs"

# 3) connectors
"$LOAFER" connectors >/dev/null 2>&1 || fail "connectors failed"
pass "connectors runs"

# 4) validate
"$LOAFER" validate "$CONFIG" >/dev/null 2>&1 || fail "validate failed"
pass "validate runs"

# 5) run end-to-end + assert real output (guards the 'exit 0 but empty' class).
"$LOAFER" run "$CONFIG" --local --quiet >/dev/null 2>&1 || fail "run failed (non-zero exit)"
[ -f "$OUTPUT" ] || fail "run produced no output file at ${OUTPUT}"

# Count rows in the JSON output without assuming jq is present.
rows="$(python3 -c "import json,sys; print(len(json.load(open(sys.argv[1]))))" "$OUTPUT")" \
    || fail "output is not valid JSON: ${OUTPUT}"
if [ "$rows" -eq "$EXPECTED_ROWS" ]; then
    pass "run produced ${rows} rows (expected ${EXPECTED_ROWS})"
else
    fail "row count mismatch: got ${rows}, expected ${EXPECTED_ROWS} (silent data loss?)"
fi

# 6) control-plane entry point + health. This catches missing server/auth
# dependencies and verifies the built artifact can migrate and serve its schema.
CONTROL_DIR="${SMOKE_DIR}/control-plane"
METADATA_URL="sqlite:///${CONTROL_DIR}/metadata.db"
mkdir -p "${CONTROL_DIR}"
LOAFER_METADATA_URL="${METADATA_URL}" "$LOAFER" metadata migrate >/dev/null 2>&1 \
    || fail "metadata migration failed"

command -v loaferd >/dev/null 2>&1 || fail "loaferd entry point is missing"
loaferd --help >/dev/null 2>&1 || fail "loaferd --help failed"

LOAFER_METADATA_URL="${METADATA_URL}" \
LOAFER_AUTH_ISSUER="https://auth.example.invalid" \
LOAFER_AUTH_AUDIENCE="https://api.example.invalid" \
LOAFER_AUTH_JWKS_URL="https://auth.example.invalid/api/auth/jwks" \
LOAFER_ALLOWED_ORIGINS="https://app.example.invalid" \
    loaferd --behind-tls-proxy --host 127.0.0.1 --port 19443 \
    >"${CONTROL_DIR}/loaferd.log" 2>&1 &
loaferd_pid=$!
cleanup_loaferd() {
    kill "${loaferd_pid}" >/dev/null 2>&1 || true
    wait "${loaferd_pid}" >/dev/null 2>&1 || true
}
trap cleanup_loaferd EXIT

health_ok=false
for _ in $(seq 1 50); do
    if ! kill -0 "${loaferd_pid}" >/dev/null 2>&1; then
        cat "${CONTROL_DIR}/loaferd.log" >&2
        fail "loaferd exited before becoming healthy"
    fi
    if python3 - <<'PY' >/dev/null 2>&1
import json
import urllib.request

request = urllib.request.Request(
    "http://127.0.0.1:19443/healthz",
    headers={
        "X-Forwarded-For": "127.0.0.1",
        "X-Forwarded-Proto": "https",
    },
)
with urllib.request.urlopen(request, timeout=0.5) as response:
    assert response.status == 200
    assert json.load(response) == {"status": "ok"}
PY
    then
        health_ok=true
        break
    fi
    sleep 0.1
done

if ! $health_ok; then
    cat "${CONTROL_DIR}/loaferd.log" >&2
    fail "loaferd health check did not become ready"
fi
cleanup_loaferd
trap - EXIT
pass "loaferd starts with a migrated schema and serves /healthz"

echo "───────────────────────────────────────────────────────────────"
printf '\033[32mSMOKE PASSED\033[0m\n'
