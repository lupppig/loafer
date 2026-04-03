"""End-to-end pipeline integration tests.

Tests the full ETL flow: extract → validate → transform → load
using real connectors against real databases and files.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

# Import integration fixtures so they're visible here
sys.path.insert(0, str(Path(__file__).parent.parent / "integration"))
from conftest import _POSTGRES_URL, _pg_available

from loafer.runner import run_pipeline


@pytest.fixture()
def tmp_output(tmp_path: Path) -> Path:
    out = tmp_path / "output"
    out.mkdir(parents=True, exist_ok=True)
    return out


def _get_pg_conn():
    """Get a psycopg2 connection, or skip if Postgres unavailable."""
    if not _pg_available():
        pytest.skip(f"PostgreSQL not available at {_POSTGRES_URL}")
    import psycopg2

    return psycopg2.connect(_POSTGRES_URL)


def _create_test_table(cur, table: str, ddl: str) -> None:
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.execute(ddl)


class TestFullEtlPostgresToJson:
    """Full ETL: PostgreSQL source → custom transform → JSON target."""

    def test_postgres_to_json_with_transform(self, tmp_output: Path) -> None:
        conn = _get_pg_conn()
        cur = conn.cursor()
        table = "e2e_test_pg_json"
        _create_test_table(
            cur,
            table,
            """
            CREATE TABLE e2e_test_pg_json (
                id SERIAL PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                status TEXT DEFAULT 'active',
                country TEXT,
                age INT
            )
        """,
        )
        cur.execute(
            f"""
            INSERT INTO {table} (first_name, last_name, email, status, country, age)
            VALUES
                ('Alice', 'Smith', 'ALICE@TEST.COM', 'active', 'US', 30),
                ('Bob', 'Jones', 'BOB@TEST.COM', 'inactive', 'UK', 25),
                ('Charlie', 'Brown', 'CHARLIE@TEST.COM', 'active', 'CA', 40),
                ('Diana', 'Prince', 'DIANA@TEST.COM', 'suspended', 'US', 35),
                ('Eve', 'Wilson', 'EVE@TEST.COM', 'pending', 'DE', 28)
        """
        )
        conn.commit()

        transform_path = tmp_output / "test_transform.py"
        transform_path.write_text("""
def transform(data):
    result = []
    for row in data:
        if row.get("status") in ("suspended", "pending"):
            continue
        result.append({
            "id": row.get("id"),
            "full_name": f"{row.get('first_name', '')} {row.get('last_name', '')}".strip(),
            "email": str(row.get("email", "")).lower(),
            "status": row.get("status"),
            "country": row.get("country"),
        })
    return result
""")

        config_path = tmp_output / "pipeline.yaml"
        config_path.write_text(f"""
name: Test PG → JSON
source:
  type: postgres
  url: {_POSTGRES_URL}
  query: "SELECT id, first_name, last_name, email, status, country, age FROM {table} ORDER BY id"
  timeout: 30

target:
  type: json
  path: {tmp_output}/users.json

transform:
  type: custom
  path: {transform_path}

mode: etl
chunk_size: 10
streaming_threshold: 1000
""")

        try:
            state = run_pipeline(str(config_path), yes=True)
        finally:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
            conn.close()

        assert state["rows_extracted"] == 5
        assert state["rows_loaded"] == 3

        with open(tmp_output / "users.json") as f:
            data = json.load(f)

        assert len(data) == 3
        assert data[0]["email"] == "alice@test.com"
        assert data[0]["full_name"] == "Alice Smith"
        assert all(r["status"] not in ("suspended", "pending") for r in data)


class TestFullEtlCsvToPostgres:
    """Full ETL: CSV source → custom transform → PostgreSQL target."""

    def test_csv_to_postgres_with_transform(self, tmp_output: Path) -> None:
        conn = _get_pg_conn()
        cur = conn.cursor()
        table = "e2e_test_csv_pg"
        _create_test_table(
            cur,
            table,
            """
            CREATE TABLE e2e_test_csv_pg (
                id SERIAL PRIMARY KEY,
                name TEXT,
                email TEXT,
                score NUMERIC(10, 2)
            )
        """,
        )
        conn.commit()

        csv_path = tmp_output / "input.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "email", "score", "status"])
            writer.writerow([1, "Alice", "ALICE@TEST.COM", 95.5, "active"])
            writer.writerow([2, "Bob", "BOB@TEST.COM", 0, "active"])
            writer.writerow([3, "Charlie", "CHARLIE@TEST.COM", 87.0, "active"])
            writer.writerow([4, "Diana", "DIANA@TEST.COM", -5, "inactive"])

        transform_path = tmp_output / "test_transform.py"
        transform_path.write_text("""
def transform(data):
    result = []
    for row in data:
        score = row.get("score", 0)
        try:
            score = float(score)
        except (ValueError, TypeError):
            score = 0
        if score <= 0:
            continue
        result.append({
            "name": row.get("name"),
            "email": str(row.get("email", "")).lower(),
            "score": score,
        })
    return result
""")

        config_path = tmp_output / "pipeline.yaml"
        config_path.write_text(f"""
name: Test CSV → PG
source:
  type: csv
  path: {csv_path}

target:
  type: postgres
  url: {_POSTGRES_URL}
  table: {table}
  write_mode: append

transform:
  type: custom
  path: {transform_path}

mode: etl
chunk_size: 10
streaming_threshold: 1000
""")

        try:
            state = run_pipeline(str(config_path), yes=True)

            assert state["rows_extracted"] == 4
            assert state["rows_loaded"] == 2

            cur.execute(f"SELECT name, email, score FROM {table} ORDER BY name")
            rows = cur.fetchall()
            assert len(rows) == 2
            assert rows[0] == ("Alice", "alice@test.com", 95.5)
            assert rows[1] == ("Charlie", "charlie@test.com", 87.0)
        finally:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
            conn.close()


class TestFullEtlExcelToCsv:
    """Full ETL: Excel source → custom transform → CSV target."""

    def test_excel_to_csv_with_transform(self, tmp_output: Path) -> None:
        import openpyxl

        xlsx_path = tmp_output / "input.xlsx"
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Data"
        ws.append(["product", "cost", "price", "active"])
        for i in range(1, 21):
            ws.append(
                [
                    f"Product-{i}",
                    round(i * 10.5, 2),
                    round(i * 20.0, 2),
                    i % 4 != 0,
                ]
            )
        wb.save(str(xlsx_path))

        transform_path = tmp_output / "test_transform.py"
        transform_path.write_text("""
def transform(data):
    result = []
    for row in data:
        active = row.get("active")
        if active is False or (isinstance(active, str) and active.lower() == "false"):
            continue
        cost = float(row.get("cost", 0))
        price = float(row.get("price", 0))
        margin = round(((price - cost) / price * 100), 2) if price > 0 else 0
        result.append({
            "product": row.get("product"),
            "cost": cost,
            "price": price,
            "margin_pct": margin,
        })
    return result
""")

        csv_output = tmp_output / "output.csv"
        config_path = tmp_output / "pipeline.yaml"
        config_path.write_text(f"""
name: Test Excel → CSV
source:
  type: excel
  path: {xlsx_path}
  sheet: Data

target:
  type: csv
  path: {csv_output}

transform:
  type: custom
  path: {transform_path}

mode: etl
chunk_size: 10
streaming_threshold: 1000
""")

        state = run_pipeline(str(config_path), yes=True)

        assert state["rows_extracted"] == 20
        assert state["rows_loaded"] == 15

        with open(csv_output) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 15
        assert "margin_pct" in rows[0]
        assert float(rows[0]["margin_pct"]) > 0


class TestFullEtlJsonTarget:
    """Full ETL: CSV source → JSON target (no transform filtering)."""

    def test_csv_to_json_no_filtering(self, tmp_output: Path) -> None:
        csv_path = tmp_output / "input.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "value"])
            for i in range(100):
                writer.writerow([i, f"item_{i}", i * 1.5])

        json_output = tmp_output / "output.json"
        config_path = tmp_output / "pipeline.yaml"
        config_path.write_text(f"""
name: Test CSV → JSON
source:
  type: csv
  path: {csv_path}

target:
  type: json
  path: {json_output}

transform:
  type: custom
  path: {Path(__file__).parent.parent.parent / "examples" / "transforms" / "sample_transform.py"}

mode: etl
chunk_size: 25
streaming_threshold: 1000
""")

        state = run_pipeline(str(config_path), yes=True)

        assert state["rows_extracted"] == 100
        assert state["rows_loaded"] == 100

        with open(json_output) as f:
            data = json.load(f)
        assert len(data) == 100


class TestFullEtlPostgresToCsv:
    """Full ETL: PostgreSQL source → CSV target with filtering transform."""

    def test_postgres_to_csv_with_filtering(self, tmp_output: Path) -> None:
        conn = _get_pg_conn()
        cur = conn.cursor()
        users_table = "e2e_test_pg_csv_users"
        orders_table = "e2e_test_pg_csv_orders"
        _create_test_table(
            cur,
            users_table,
            """
            CREATE TABLE e2e_test_pg_csv_users (
                id SERIAL PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT NOT NULL,
                status TEXT DEFAULT 'active',
                country TEXT,
                age INT
            )
        """,
        )
        _create_test_table(
            cur,
            orders_table,
            """
            CREATE TABLE e2e_test_pg_csv_orders (
                id SERIAL PRIMARY KEY,
                user_id INT,
                product TEXT,
                quantity INT,
                unit_price NUMERIC(10, 2),
                total_price NUMERIC(10, 2),
                status TEXT DEFAULT 'pending',
                region TEXT
            )
        """,
        )
        cur.execute(
            f"""
            INSERT INTO {users_table} (first_name, last_name, email, status, country, age)
            VALUES ('Test', 'User', 'test@test.com', 'active', 'US', 30)
        """
        )
        cur.execute(
            f"""
            INSERT INTO {orders_table} (user_id, product, quantity, unit_price, total_price, status, region)
            VALUES
                (1, 'Widget', 2, 10.0, 20.0, 'completed', 'North'),
                (1, 'Gadget', 1, 50.0, 50.0, 'completed', 'South'),
                (1, 'Thing', 3, 5.0, 15.0, 'cancelled', 'East'),
                (1, 'Item', 1, 100.0, 100.0, 'refunded', 'West'),
                (1, 'Part', 5, 8.0, 40.0, 'completed', 'North')
        """
        )
        conn.commit()

        transform_path = tmp_output / "test_transform.py"
        transform_path.write_text("""
def transform(data):
    result = []
    for row in data:
        if row.get("status") in ("cancelled", "refunded"):
            continue
        result.append({
            "product": row.get("product"),
            "quantity": row.get("quantity"),
            "total_price": row.get("total_price"),
            "status": row.get("status"),
            "region": row.get("region"),
        })
    return result
""")

        csv_output = tmp_output / "orders.csv"
        config_path = tmp_output / "pipeline.yaml"
        config_path.write_text(f"""
name: Test PG → CSV
source:
  type: postgres
  url: {_POSTGRES_URL}
  query: "SELECT id, user_id, product, quantity, unit_price, total_price, status, region FROM {orders_table} ORDER BY id"
  timeout: 30

target:
  type: csv
  path: {csv_output}

transform:
  type: custom
  path: {transform_path}

mode: etl
chunk_size: 10
streaming_threshold: 1000
""")

        try:
            state = run_pipeline(str(config_path), yes=True)

            assert state["rows_extracted"] == 5
            assert state["rows_loaded"] == 3

            with open(csv_output) as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 3
            assert all(r["status"] == "completed" for r in rows)
            assert sum(int(r["quantity"]) for r in rows) == 8
        finally:
            cur.execute(f"DROP TABLE IF EXISTS {orders_table}")
            cur.execute(f"DROP TABLE IF EXISTS {users_table}")
            conn.commit()
            conn.close()


class TestStreamingLargeDataset:
    """Test streaming behavior with datasets above the threshold."""

    def test_streaming_csv_source(self, tmp_output: Path) -> None:
        csv_path = tmp_output / "large.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "value"])
            for i in range(5000):
                writer.writerow([i, i * 2])

        json_output = tmp_output / "output.json"
        config_path = tmp_output / "pipeline.yaml"
        config_path.write_text(f"""
name: Test Streaming
source:
  type: csv
  path: {csv_path}

target:
  type: json
  path: {json_output}

transform:
  type: custom
  path: {Path(__file__).parent.parent.parent / "examples" / "transforms" / "sample_transform.py"}

mode: etl
chunk_size: 500
streaming_threshold: 1000
""")

        state = run_pipeline(str(config_path), yes=True)

        assert state["is_streaming"] is True
        assert state["rows_extracted"] == 5000
        assert state["rows_loaded"] == 5000

        with open(json_output) as f:
            data = json.load(f)
        assert len(data) == 5000
