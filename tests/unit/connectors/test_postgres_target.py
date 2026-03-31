"""Tests for PostgresTargetConnector (via registry)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


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
