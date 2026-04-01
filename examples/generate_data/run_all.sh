#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="$PROJECT_DIR/examples/output"

mkdir -p "$OUTPUT_DIR"

echo "============================================"
echo "  Loafer ETL Demo - Running All Pipelines"
echo "============================================"
echo ""

PIPELINES=(
    "postgres_users_to_json.yaml:PostgreSQL Users → JSON"
    "postgres_orders_to_csv.yaml:PostgreSQL Orders → CSV"
    "csv_to_postgres.yaml:CSV → PostgreSQL"
    "excel_to_csv.yaml:Excel → CSV"
    "mysql_to_json.yaml:MySQL → JSON"
    "mongo_to_csv.yaml:MongoDB → CSV"
)

FAILED=0
SUCCEEDED=0

for entry in "${PIPELINES[@]}"; do
    FILE="${entry%%:*}"
    DESC="${entry##*:}"
    PIPELINE_PATH="$PROJECT_DIR/examples/pipelines/$FILE"

    echo "────────────────────────────────────────────"
    echo "▶ Pipeline: $DESC"
    echo "  Config: $PIPELINE_PATH"
    echo "────────────────────────────────────────────"

    if [ ! -f "$PIPELINE_PATH" ]; then
        echo "  SKIP: Config file not found"
        FAILED=$((FAILED + 1))
        echo ""
        continue
    fi

    if uv run loafer run "$PIPELINE_PATH" 2>&1; then
        SUCCEEDED=$((SUCCEEDED + 1))
        echo "  ✓ Success"
    else
        FAILED=$((FAILED + 1))
        echo "  ✗ Failed"
    fi
    echo ""
done

echo "============================================"
echo "  Results: $SUCCEEDED succeeded, $FAILED failed"
echo "============================================"
echo ""
echo "Output files:"
ls -lh "$OUTPUT_DIR"/ 2>/dev/null || echo "  (no output directory)"

exit $FAILED
