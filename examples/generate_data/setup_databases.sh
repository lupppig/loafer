#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo "  Loafer ETL Demo - Database Setup"
echo "============================================"
echo ""

# 1. Generate CSV and Excel files
echo "[1/4] Generating sample CSV and Excel files..."
uv run python "$SCRIPT_DIR/generate_files.py"
echo ""

# 2. Seed PostgreSQL
echo "[2/4] Seeding PostgreSQL (10M records across 4 tables)..."
if command -v psql &> /dev/null; then
    if psql -U loafer -d loafer_dev -c "SELECT 1" &> /dev/null 2>&1; then
        psql -U loafer -d loafer_dev -f "$SCRIPT_DIR/setup_postgres.sql"
        echo "  PostgreSQL seed complete."
    else
        echo "  WARNING: Cannot connect to PostgreSQL. Start it with:"
        echo "    docker compose -f docker/docker-compose.yml up -d postgres"
        echo "  Then re-run this script."
    fi
else
    echo "  WARNING: psql not found. Install postgresql-client or use Docker."
fi
echo ""

# 3. Seed MySQL
echo "[3/4] Seeding MySQL (100K records across 2 tables)..."
if command -v mysql &> /dev/null; then
    if mysql -u loafer -ploafer -e "SELECT 1" &> /dev/null 2>&1; then
        mysql -u loafer -ploafer < "$SCRIPT_DIR/setup_mysql.sql"
        echo "  MySQL seed complete."
    else
        echo "  WARNING: Cannot connect to MySQL. Start it with:"
        echo "    docker compose -f docker/docker-compose.yml up -d mysql"
        echo "  Then re-run this script."
        echo "  NOTE: Add a mysql service to docker-compose.yml first."
    fi
else
    echo "  WARNING: mysql client not found. Install mysql-client or use Docker."
fi
echo ""

# 4. Seed MongoDB
echo "[4/4] Seeding MongoDB (50K documents across 2 collections)..."
if command -v mongosh &> /dev/null; then
    if mongosh --quiet --eval "db.adminCommand('ping')" &> /dev/null 2>&1; then
        mongosh "mongodb://loafer:loafer@localhost:27017/admin" "$SCRIPT_DIR/setup_mongo.js"
        echo "  MongoDB seed complete."
    else
        echo "  WARNING: Cannot connect to MongoDB. Start it with:"
        echo "    docker compose -f docker/docker-compose.yml up -d mongo"
        echo "  Then re-run this script."
    fi
elif command -v mongo &> /dev/null; then
    mongo "mongodb://loafer:loafer@localhost:27017/admin" "$SCRIPT_DIR/setup_mongo.js"
    echo "  MongoDB seed complete."
else
    echo "  WARNING: mongosh/mongo not found. Install MongoDB shell or use Docker."
fi
echo ""

echo "============================================"
echo "  Setup complete!"
echo "============================================"
echo ""
echo "Run all ETL pipelines:"
echo "  bash bin/run_all.sh"
echo ""
echo "Or run individual pipelines:"
echo "  uv run loafer run examples/pipelines/postgres_users_to_json.yaml"
echo "  uv run loafer run examples/pipelines/postgres_orders_to_csv.yaml"
echo "  uv run loafer run examples/pipelines/csv_to_postgres.yaml"
echo "  uv run loafer run examples/pipelines/excel_to_csv.yaml"
echo "  uv run loafer run examples/pipelines/mysql_to_json.yaml"
echo "  uv run loafer run examples/pipelines/mongo_to_csv.yaml"
