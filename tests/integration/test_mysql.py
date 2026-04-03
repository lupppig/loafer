"""MySQL source connector integration tests.

Requires a running MySQL instance. Tests are skipped if MySQL is not available.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from loafer.connectors.registry import MySQLSourceConnector

pytestmark = pytest.mark.integration


class TestMySQLSourceConnector:
    """Test MySQL source connector against a real database."""

    def test_connect_and_disconnect(self, mysql_url: str) -> None:
        conn = MySQLSourceConnector(
            url=mysql_url,
            query="SELECT 1 AS val",
            timeout=30,
        )
        conn.connect()
        try:
            assert conn._cursor is not None
        finally:
            conn.disconnect()
        assert conn._cursor is None

    def test_stream_small_result(self, mysql_url: str) -> None:
        conn = MySQLSourceConnector(
            url=mysql_url,
            query="SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3",
            timeout=30,
        )
        conn.connect()
        try:
            chunks = list(conn.stream(chunk_size=2))
        finally:
            conn.disconnect()

        total = sum(len(c) for c in chunks)
        assert total == 3
        assert chunks[0][0]["id"] == 1

    def test_stream_chunking(self, mysql_url: str) -> None:
        conn = MySQLSourceConnector(
            url=mysql_url,
            query="SELECT seq FROM seq_1_to_100",
            timeout=30,
        )
        conn.connect()
        try:
            chunks = list(conn.stream(chunk_size=25))
        finally:
            conn.disconnect()

        assert len(chunks) == 4

    def test_type_conversion(self, mysql_url: str) -> None:
        conn = MySQLSourceConnector(
            url=mysql_url,
            query="""
                SELECT
                    42 AS int_val,
                    3.14 AS dec_val,
                    'hello' AS str_val,
                    true AS bool_val,
                    NULL AS null_val,
                    NOW() AS ts_val
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
        assert isinstance(row["dec_val"], (float, Decimal))
        assert row["str_val"] == "hello"
        assert row["bool_val"] in (True, 1)
        assert row["null_val"] is None
        assert isinstance(row["ts_val"], (str,))

    def test_count_returns_rowcount(self, mysql_url: str) -> None:
        conn = MySQLSourceConnector(
            url=mysql_url,
            query="SELECT 1 AS id FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t",
            timeout=30,
        )
        conn.connect()
        try:
            list(conn.stream(chunk_size=10))
        finally:
            conn.disconnect()

        count = conn.count()
        assert count is not None
        assert count == 3

    def test_invalid_query_raises(self, mysql_url: str) -> None:
        from loafer.exceptions import ExtractionError

        conn = MySQLSourceConnector(
            url=mysql_url,
            query="SELECT * FROM nonexistent_table_xyz",
            timeout=30,
        )
        with pytest.raises(ExtractionError, match="query failed"):
            conn.connect()

    def test_context_manager(self, mysql_url: str) -> None:
        conn = MySQLSourceConnector(
            url=mysql_url,
            query="SELECT 1 AS val",
            timeout=30,
        )
        with conn:
            chunks = list(conn.stream(chunk_size=1))
        assert len(chunks) == 1
        assert chunks[0][0]["val"] == 1
        assert conn._conn is None

    def test_read_all(self, mysql_url: str) -> None:
        conn = MySQLSourceConnector(
            url=mysql_url,
            query="SELECT seq AS id FROM seq_1_to_100",
            timeout=30,
        )
        with conn:
            rows = conn.read_all()
        assert len(rows) == 100
