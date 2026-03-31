"""Tests for PostgresSourceConnector (via registry)."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest


class TestPostgresSourceConnector:
    def test_stream_basic(self) -> None:
        from loafer.connectors.registry import PostgresSourceConnector

        mock_cursor = MagicMock()
        mock_cursor.itersize = 500
        mock_cursor.rowcount = 3
        mock_cursor.__iter__ = lambda self: iter([(1, "Alice"), (2, "Bob"), (3, "Charlie")])
        id_col = MagicMock()
        id_col.name = "id"
        id_col.type_code = 23
        name_col = MagicMock()
        name_col.name = "name"
        name_col.type_code = 25
        mock_cursor.description = [id_col, name_col]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(PostgresSourceConnector, "connect"):
            conn = PostgresSourceConnector(
                "postgresql://user:pass@localhost/db", "SELECT * FROM users"
            )
            conn._conn = mock_conn
            conn._cursor = mock_cursor
            rows = conn.read_all()

        assert len(rows) == 3
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Alice"

    def test_connection_failure_raises_extraction_error(self) -> None:
        import psycopg2

        from loafer.connectors.registry import PostgresSourceConnector
        from loafer.exceptions import ExtractionError

        with patch("psycopg2.connect", side_effect=psycopg2.Error("refused")):
            conn = PostgresSourceConnector("postgresql://user:pass@localhost/db", "SELECT 1")
            with pytest.raises(ExtractionError, match="failed to connect"):
                conn.connect()

    def test_stream_before_connect_raises(self) -> None:
        from loafer.connectors.registry import PostgresSourceConnector
        from loafer.exceptions import ExtractionError

        conn = PostgresSourceConnector("postgresql://user:pass@localhost/db", "SELECT 1")
        with pytest.raises(ExtractionError, match="connect"):
            list(conn.stream(100))

    def test_decimal_converted_to_float(self) -> None:
        from loafer.connectors.registry import PostgresSourceConnector

        mock_cursor = MagicMock()
        mock_cursor.itersize = 500
        mock_cursor.rowcount = 1
        mock_cursor.__iter__ = lambda self: iter([(Decimal("19.99"),)])
        price_col = MagicMock()
        price_col.name = "price"
        price_col.type_code = 1700
        mock_cursor.description = [price_col]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(PostgresSourceConnector, "connect"):
            conn = PostgresSourceConnector(
                "postgresql://user:pass@localhost/db",
                "SELECT price FROM products",
            )
            conn._conn = mock_conn
            conn._cursor = mock_cursor
            rows = conn.read_all()

        assert rows[0]["price"] == 19.99
        assert isinstance(rows[0]["price"], float)

    def test_count_returns_rowcount(self) -> None:
        from loafer.connectors.registry import PostgresSourceConnector

        conn = PostgresSourceConnector("postgresql://user:pass@localhost/db", "SELECT 1")
        conn._row_count = 50
        assert conn.count() == 50

    def test_count_returns_none_when_unknown(self) -> None:
        from loafer.connectors.registry import PostgresSourceConnector

        conn = PostgresSourceConnector("postgresql://user:pass@localhost/db", "SELECT 1")
        assert conn.count() is None

    def test_chunking(self) -> None:
        from loafer.connectors.registry import PostgresSourceConnector

        mock_cursor = MagicMock()
        mock_cursor.itersize = 500
        mock_cursor.rowcount = 10
        id_col = MagicMock()
        id_col.name = "id"
        id_col.type_code = 23
        mock_cursor.description = [id_col]
        mock_cursor.__iter__ = lambda self: iter([(i,) for i in range(10)])
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(PostgresSourceConnector, "connect"):
            conn = PostgresSourceConnector(
                "postgresql://user:pass@localhost/db", "SELECT id FROM items"
            )
            conn._conn = mock_conn
            conn._cursor = mock_cursor
            chunks = list(conn.stream(chunk_size=3))

        assert len(chunks) == 4
        assert sum(len(c) for c in chunks) == 10
