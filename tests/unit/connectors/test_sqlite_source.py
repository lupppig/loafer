"""Tests for SqliteSourceConnector."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


class TestSqliteSourceConnector:
    """Tests for the SQLite source connector."""

    @pytest.fixture
    def db_path(self, tmp_path: Path) -> Path:
        """Create a temporary SQLite database with test data."""
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT, score REAL)")
        conn.executemany(
            "INSERT INTO users VALUES (?, ?, ?)",
            [(i, f"user_{i}", float(i * 10)) for i in range(1, 21)],
        )
        conn.commit()
        conn.close()
        return db

    def test_connect_and_disconnect(self, db_path: Path) -> None:
        from loafer.connectors.registry import SqliteSourceConnector

        conn = SqliteSourceConnector(str(db_path), "SELECT * FROM users")
        conn.connect()
        assert conn._conn is not None
        conn.disconnect()
        assert conn._conn is None

    def test_stream_returns_all_rows(self, db_path: Path) -> None:
        from loafer.connectors.registry import SqliteSourceConnector

        conn = SqliteSourceConnector(str(db_path), "SELECT * FROM users")
        conn.connect()
        chunks = list(conn.stream(chunk_size=5))
        conn.disconnect()

        assert len(chunks) == 4  # 20 rows / 5 per chunk
        all_rows = [row for chunk in chunks for row in chunk]
        assert len(all_rows) == 20
        assert all_rows[0]["name"] == "user_1"
        assert all_rows[-1]["name"] == "user_20"

    def test_stream_with_filter(self, db_path: Path) -> None:
        from loafer.connectors.registry import SqliteSourceConnector

        conn = SqliteSourceConnector(str(db_path), "SELECT * FROM users WHERE score > 100")
        conn.connect()
        chunks = list(conn.stream(chunk_size=10))
        conn.disconnect()

        all_rows = [row for chunk in chunks for row in chunk]
        assert len(all_rows) == 10  # scores 110..200

    def test_read_all_convenience(self, db_path: Path) -> None:
        from loafer.connectors.registry import SqliteSourceConnector

        conn = SqliteSourceConnector(str(db_path), "SELECT * FROM users")
        conn.connect()
        rows = conn.read_all()
        conn.disconnect()

        assert len(rows) == 20

    def test_count(self, db_path: Path) -> None:
        from loafer.connectors.registry import SqliteSourceConnector

        conn = SqliteSourceConnector(str(db_path), "SELECT * FROM users")
        conn.connect()
        count = conn.count()
        conn.disconnect()

        assert count == 20

    def test_count_with_filter(self, db_path: Path) -> None:
        from loafer.connectors.registry import SqliteSourceConnector

        conn = SqliteSourceConnector(str(db_path), "SELECT * FROM users WHERE score > 50")
        conn.connect()
        count = conn.count()
        conn.disconnect()

        assert count == 15

    def test_count_before_connect_returns_none(self) -> None:
        from loafer.connectors.registry import SqliteSourceConnector

        conn = SqliteSourceConnector("/tmp/test.db", "SELECT 1")
        assert conn.count() is None

    def test_context_manager(self, db_path: Path) -> None:
        from loafer.connectors.registry import SqliteSourceConnector

        with SqliteSourceConnector(str(db_path), "SELECT * FROM users") as conn:
            rows = conn.read_all()

        assert len(rows) == 20
        assert conn._conn is None

    def test_invalid_sql_raises(self, db_path: Path) -> None:
        from loafer.connectors.registry import SqliteSourceConnector

        conn = SqliteSourceConnector(str(db_path), "SELECT * FROM nonexistent")
        conn.connect()
        with pytest.raises(Exception, match="no such table"):
            list(conn.stream(chunk_size=10))
        conn.disconnect()
