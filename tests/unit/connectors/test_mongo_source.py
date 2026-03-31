"""Tests for MongoSourceConnector (via registry)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestMongoSourceConnector:
    def test_stream_basic(self) -> None:
        from loafer.connectors.registry import MongoSourceConnector

        mock_coll = MagicMock()
        mock_coll.count_documents.return_value = 2
        mock_coll.find.return_value = iter(
            [{"_id": "abc", "name": "Alice"}, {"_id": "def", "name": "Bob"}]
        )
        mock_client = MagicMock()
        mock_client.__getitem__.return_value = mock_coll
        mock_client.__getitem__.return_value.list_collection_names.return_value = ["users"]

        with patch.object(MongoSourceConnector, "connect"):
            conn = MongoSourceConnector("mongodb://localhost:27017", "mydb", "users")
            conn._client = mock_client
            conn._coll = mock_coll
            conn._row_count = 2
            rows = list(conn.stream(chunk_size=100))

        assert len(rows) == 1
        assert rows[0][0]["name"] == "Alice"
        assert rows[0][0]["_id"] == "abc"
        assert rows[0][1]["name"] == "Bob"
        assert rows[0][1]["_id"] == "def"

    def test_collection_not_found_raises(self) -> None:
        from loafer.connectors.registry import MongoSourceConnector
        from loafer.exceptions import ExtractionError

        with patch.object(
            MongoSourceConnector,
            "connect",
            side_effect=ExtractionError("collection 'missing' not found"),
        ):
            conn = MongoSourceConnector("mongodb://localhost:27017", "mydb", "missing")
            with pytest.raises(ExtractionError, match="not found"):
                conn.connect()

    def test_stream_before_connect_raises(self) -> None:
        from loafer.connectors.registry import MongoSourceConnector
        from loafer.exceptions import ExtractionError

        conn = MongoSourceConnector("mongodb://localhost:27017", "mydb", "users")
        with pytest.raises(ExtractionError, match="connect"):
            list(conn.stream(100))

    def test_count(self) -> None:
        from loafer.connectors.registry import MongoSourceConnector

        with patch.object(MongoSourceConnector, "connect"):
            conn = MongoSourceConnector("mongodb://localhost:27017", "mydb", "users")
            conn._row_count = 42
            assert conn.count() == 42

    def test_filter_option(self) -> None:
        from loafer.connectors.registry import MongoSourceConnector

        with patch.object(MongoSourceConnector, "connect"):
            conn = MongoSourceConnector(
                "mongodb://localhost:27017",
                "mydb",
                "users",
                filter_doc={"active": True},
            )
            assert conn._filter == {"active": True}
            assert conn._filter is not None
