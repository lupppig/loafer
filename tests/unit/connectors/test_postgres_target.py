"""Tests for PostgresTargetConnector (via registry)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tests.postgres_sql import render_sql


class TestPostgresTargetConnector:
    def test_write_before_connect_raises(self) -> None:
        from loafer.connectors.registry import PostgresTargetConnector
        from loafer.exceptions import LoadError

        conn = PostgresTargetConnector("postgresql://user:pass@localhost/db", "users")
        with pytest.raises(LoadError, match="connect"):
            conn.write_chunk([{"id": 1}])

    def test_finalize_commits(self) -> None:
        from loafer.connectors.registry import PostgresTargetConnector

        mock_conn = MagicMock()

        conn = PostgresTargetConnector("postgresql://user:pass@localhost/db", "users")
        conn._conn = mock_conn
        conn.finalize()

        assert mock_conn.commit.called

    def test_table_exists_uses_explicit_schema_and_table(self) -> None:
        from loafer.connectors.registry import PostgresTargetConnector

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True,)

        conn = PostgresTargetConnector(
            "postgresql://user:pass@localhost/db",
            "analytics.events",
        )
        conn._cursor = mock_cursor

        assert conn._table_exists() is True
        assert mock_cursor.execute.call_args.args[1] == ("analytics", "events")

    def test_replace_quotes_adversarial_table_identifier(self) -> None:
        from loafer.connectors.registry import PostgresTargetConnector

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        conn = PostgresTargetConnector(
            "postgresql://user:pass@localhost/db",
            'analytics.events"; DROP TABLE users; --',
            write_mode="replace",
        )
        conn._conn = mock_conn
        conn._cursor = mock_cursor

        conn._apply_write_mode()

        query = render_sql(mock_cursor.execute.call_args.args[0])
        assert query == 'DROP TABLE IF EXISTS "analytics"."events""; DROP TABLE users; --"'
        mock_conn.commit.assert_called_once()

    def test_create_table_quotes_adversarial_columns(self) -> None:
        from loafer.connectors.registry import PostgresTargetConnector

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        conn = PostgresTargetConnector(
            "postgresql://user:pass@localhost/db",
            "analytics.events",
        )
        conn._conn = mock_conn
        conn._cursor = mock_cursor

        conn._create_table({'name"; DROP TABLE users; --': "safe"})

        query = render_sql(mock_cursor.execute.call_args.args[0])
        assert query == ('CREATE TABLE "analytics"."events" ("name""; DROP TABLE users; --" TEXT)')

    def test_unique_index_quotes_key_and_uses_safe_generated_name(self) -> None:
        from loafer.connectors.registry import PostgresTargetConnector

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        conn = PostgresTargetConnector(
            "postgresql://user:pass@localhost/db",
            "analytics.events",
            write_mode="upsert",
            key=['id"; DROP TABLE users; --'],
        )
        conn._conn = mock_conn
        conn._cursor = mock_cursor

        conn._ensure_unique_index()

        query = render_sql(mock_cursor.execute.call_args.args[0])
        assert query.startswith('CREATE UNIQUE INDEX IF NOT EXISTS "loafer_uq_')
        assert 'ON "analytics"."events"' in query
        assert '("id""; DROP TABLE users; --")' in query
