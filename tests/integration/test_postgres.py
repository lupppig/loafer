"""PostgreSQL source and target connector integration tests.

Requires a running PostgreSQL instance at the configured URL.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from loafer.connectors.registry import PostgresSourceConnector, PostgresTargetConnector


class TestPostgresSourceConnector:
    """Test PostgreSQL source connector against a real database."""

    def test_connect_and_disconnect(self, postgres_url: str) -> None:
        conn = PostgresSourceConnector(
            url=postgres_url,
            query="SELECT 1 AS val",
            timeout=30,
        )
        conn.connect()
        try:
            assert conn._cursor is not None
        finally:
            conn.disconnect()
        assert conn._cursor is None

    def test_stream_small_result(self, postgres_url: str) -> None:
        conn = PostgresSourceConnector(
            url=postgres_url,
            query="SELECT generate_series(1, 10) AS id",
            timeout=30,
        )
        conn.connect()
        try:
            chunks = list(conn.stream(chunk_size=3))
        finally:
            conn.disconnect()

        total = sum(len(c) for c in chunks)
        assert total == 10
        assert chunks[0][0] == {"id": 1}

    def test_stream_chunking(self, postgres_url: str) -> None:
        conn = PostgresSourceConnector(
            url=postgres_url,
            query="SELECT generate_series(1, 100) AS id",
            timeout=30,
        )
        conn.connect()
        try:
            chunks = list(conn.stream(chunk_size=25))
        finally:
            conn.disconnect()

        assert len(chunks) == 4
        assert all(len(c) == 25 for c in chunks)

    def test_type_conversion(self, postgres_url: str) -> None:
        conn = PostgresSourceConnector(
            url=postgres_url,
            query="""
                SELECT
                    42 AS int_val,
                    3.14::NUMERIC(10,2) AS dec_val,
                    'hello' AS str_val,
                    true AS bool_val,
                    NULL AS null_val,
                    NOW() AS ts_val,
                    gen_random_uuid() AS uuid_val
            """,
            timeout=30,
        )
        conn.connect()
        try:
            chunks = list(conn.stream(chunk_size=1))
        finally:
            conn.disconnect()

        row = chunks[0][0]
        assert row["int_val"] == 42
        assert isinstance(row["dec_val"], float)
        assert row["str_val"] == "hello"
        assert row["bool_val"] is True
        assert row["null_val"] is None
        assert isinstance(row["ts_val"], str)
        assert isinstance(row["uuid_val"], str)

    def test_stream_large_result(self, postgres_url: str) -> None:
        conn = PostgresSourceConnector(
            url=postgres_url,
            query="SELECT generate_series(1, 10000) AS id",
            timeout=60,
        )
        conn.connect()
        try:
            total = 0
            for chunk in conn.stream(chunk_size=500):
                total += len(chunk)
        finally:
            conn.disconnect()

        assert total == 10000

    def test_count_returns_rowcount(self, postgres_url: str) -> None:
        conn = PostgresSourceConnector(
            url=postgres_url,
            query="SELECT generate_series(1, 42) AS id",
            timeout=30,
        )
        conn.connect()
        try:
            list(conn.stream(chunk_size=10))
        finally:
            conn.disconnect()

        assert conn.count() == 42

    def test_invalid_query_raises(self, postgres_url: str) -> None:
        from loafer.exceptions import ExtractionError

        conn = PostgresSourceConnector(
            url=postgres_url,
            query="SELECT * FROM nonexistent_table_xyz",
            timeout=30,
        )
        with pytest.raises(ExtractionError, match="query failed"):
            conn.connect()

    def test_bad_connection_url_raises(self) -> None:
        from loafer.exceptions import ExtractionError

        conn = PostgresSourceConnector(
            url="postgresql://baduser:badpass@localhost:5432/nonexistent_db_xyz",
            query="SELECT 1",
            timeout=5,
        )
        with pytest.raises(ExtractionError, match="failed to connect"):
            conn.connect()

    def test_context_manager(self, postgres_url: str) -> None:
        conn = PostgresSourceConnector(
            url=postgres_url,
            query="SELECT 1 AS val",
            timeout=30,
        )
        with conn:
            chunks = list(conn.stream(chunk_size=1))
        assert len(chunks) == 1
        assert chunks[0][0]["val"] == 1
        assert conn._conn is None

    def test_read_all(self, postgres_url: str) -> None:
        conn = PostgresSourceConnector(
            url=postgres_url,
            query="SELECT generate_series(1, 50) AS id",
            timeout=30,
        )
        with conn:
            rows = conn.read_all()
        assert len(rows) == 50


class TestPostgresTargetConnector:
    """Test PostgreSQL target connector against a real database."""

    def test_write_and_read(self, postgres_url: str, pg_test_table: str) -> None:
        import psycopg2

        target = PostgresTargetConnector(
            url=postgres_url,
            table=pg_test_table,
            write_mode="append",
        )
        target.connect()
        try:
            rows = [
                {"name": "Alice", "email": "alice@test.com", "score": 95.5},
                {"name": "Bob", "email": "bob@test.com", "score": 87.0},
                {"name": "Charlie", "email": "charlie@test.com", "score": None},
            ]
            written = target.write_chunk(rows)
            assert written == 3
            target.finalize()
        finally:
            target.disconnect()

        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {pg_test_table}")
        assert cur.fetchone()[0] == 3
        cur.execute(f"SELECT name, email, score FROM {pg_test_table} ORDER BY name")
        result = cur.fetchall()
        assert result[0] == ("Alice", "alice@test.com", Decimal("95.50"))
        assert result[1] == ("Bob", "bob@test.com", Decimal("87.00"))
        assert result[2] == ("Charlie", "charlie@test.com", None)
        conn.close()

    def test_write_multiple_chunks(self, postgres_url: str, pg_test_table: str) -> None:
        import psycopg2

        target = PostgresTargetConnector(
            url=postgres_url,
            table=pg_test_table,
            write_mode="append",
        )
        target.connect()
        try:
            for i in range(10):
                chunk = [
                    {"name": f"User_{i}_{j}", "email": f"u{i}_{j}@test.com", "score": float(j)}
                    for j in range(5)
                ]
                target.write_chunk(chunk)
            target.finalize()
        finally:
            target.disconnect()

        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {pg_test_table}")
        assert cur.fetchone()[0] == 50
        conn.close()

    def test_write_with_nulls(self, postgres_url: str, pg_test_table: str) -> None:
        import psycopg2

        target = PostgresTargetConnector(
            url=postgres_url,
            table=pg_test_table,
            write_mode="append",
        )
        target.connect()
        try:
            rows = [
                {"name": None, "email": "noname@test.com", "score": 50.0},
                {"name": "HasName", "email": None, "score": None},
            ]
            target.write_chunk(rows)
            target.finalize()
        finally:
            target.disconnect()

        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {pg_test_table}")
        assert cur.fetchone()[0] == 2
        conn.close()

    def test_write_empty_chunk(self, postgres_url: str, pg_test_table: str) -> None:
        target = PostgresTargetConnector(
            url=postgres_url,
            table=pg_test_table,
            write_mode="append",
        )
        target.connect()
        try:
            written = target.write_chunk([])
            assert written == 0
        finally:
            target.disconnect()

    def test_context_manager(self, postgres_url: str, pg_test_table: str) -> None:
        import psycopg2

        with PostgresTargetConnector(
            url=postgres_url,
            table=pg_test_table,
            write_mode="append",
        ) as target:
            target.write_chunk([{"name": "Ctx", "email": "ctx@test.com", "score": 1.0}])

        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {pg_test_table}")
        assert cur.fetchone()[0] == 1
        conn.close()

    def test_write_before_connect_raises(self, postgres_url: str) -> None:
        from loafer.exceptions import LoadError

        target = PostgresTargetConnector(
            url=postgres_url,
            table="test_table",
            write_mode="append",
        )
        with pytest.raises(LoadError, match="connect\\(\\) must be called"):
            target.write_chunk([{"name": "test"}])

    def test_finalize_commits(self, postgres_url: str, pg_test_table: str) -> None:
        import psycopg2

        target = PostgresTargetConnector(
            url=postgres_url,
            table=pg_test_table,
            write_mode="append",
        )
        target.connect()
        try:
            target.write_chunk([{"name": "Commit", "email": "commit@test.com", "score": 1.0}])
            target.finalize()
        finally:
            target.disconnect()

        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {pg_test_table}")
        assert cur.fetchone()[0] == 1
        conn.close()

    def test_large_batch_write(self, postgres_url: str, pg_test_table: str) -> None:
        import psycopg2

        target = PostgresTargetConnector(
            url=postgres_url,
            table=pg_test_table,
            write_mode="append",
        )
        target.connect()
        try:
            rows = [
                {"name": f"User_{i}", "email": f"u{i}@test.com", "score": float(i % 100)}
                for i in range(1000)
            ]
            written = target.write_chunk(rows)
            assert written == 1000
            target.finalize()
        finally:
            target.disconnect()

        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {pg_test_table}")
        assert cur.fetchone()[0] == 1000
        conn.close()
