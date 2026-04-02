"""Tests for MongoTargetConnector."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestMongoTargetConnector:
    """Tests for the MongoDB target connector."""

    def test_connect_and_disconnect(self) -> None:
        from loafer.connectors.registry import MongoTargetConnector

        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_db.list_collection_names.return_value = []

        with patch("pymongo.MongoClient", return_value=mock_client):
            conn = MongoTargetConnector(
                url="mongodb://localhost:27017",
                database="testdb",
                collection="testcoll",
            )
            conn.connect()
            assert conn._client is not None
            conn.disconnect()
            assert conn._client is None
            mock_client.close.assert_called_once()

    def test_replace_mode_drops_collection(self) -> None:
        from loafer.connectors.registry import MongoTargetConnector

        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_db.list_collection_names.return_value = []

        with patch("pymongo.MongoClient", return_value=mock_client):
            conn = MongoTargetConnector(
                url="mongodb://localhost:27017",
                database="testdb",
                collection="testcoll",
                write_mode="replace",
            )
            conn.connect()
            mock_collection.drop.assert_called_once()
            conn.disconnect()

    def test_error_mode_raises_if_exists(self) -> None:
        from loafer.connectors.registry import MongoTargetConnector
        from loafer.exceptions import LoadError

        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_db.list_collection_names.return_value = ["testcoll"]

        with patch("pymongo.MongoClient", return_value=mock_client):
            conn = MongoTargetConnector(
                url="mongodb://localhost:27017",
                database="testdb",
                collection="testcoll",
                write_mode="error",
            )
            with pytest.raises(LoadError, match="already exists"):
                conn.connect()

    def test_write_chunk_returns_count(self) -> None:
        from loafer.connectors.registry import MongoTargetConnector

        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_result = MagicMock()
        mock_result.inserted_ids = [1, 2, 3]
        mock_collection.insert_many.return_value = mock_result
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_db.list_collection_names.return_value = []

        with patch("pymongo.MongoClient", return_value=mock_client):
            conn = MongoTargetConnector(
                url="mongodb://localhost:27017",
                database="testdb",
                collection="testcoll",
            )
            conn.connect()
            count = conn.write_chunk([{"a": 1}, {"b": 2}, {"c": 3}])
            conn.disconnect()

        assert count == 3
        mock_collection.insert_many.assert_called_once()

    def test_write_empty_chunk_returns_zero(self) -> None:
        from loafer.connectors.registry import MongoTargetConnector

        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_db.list_collection_names.return_value = []

        with patch("pymongo.MongoClient", return_value=mock_client):
            conn = MongoTargetConnector(
                url="mongodb://localhost:27017",
                database="testdb",
                collection="testcoll",
            )
            conn.connect()
            count = conn.write_chunk([])
            conn.disconnect()

        assert count == 0
        mock_collection.insert_many.assert_not_called()

    def test_finalize_is_noop(self) -> None:
        from loafer.connectors.registry import MongoTargetConnector

        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_db.list_collection_names.return_value = []

        with patch("pymongo.MongoClient", return_value=mock_client):
            conn = MongoTargetConnector(
                url="mongodb://localhost:27017",
                database="testdb",
                collection="testcoll",
            )
            conn.connect()
            conn.finalize()
            conn.disconnect()

    def test_context_manager(self) -> None:
        from loafer.connectors.registry import MongoTargetConnector

        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_result = MagicMock()
        mock_result.inserted_ids = [1]
        mock_collection.insert_many.return_value = mock_result
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_db.list_collection_names.return_value = []

        with patch("pymongo.MongoClient", return_value=mock_client), MongoTargetConnector(
            url="mongodb://localhost:27017",
            database="testdb",
            collection="testcoll",
        ) as conn:
            count = conn.write_chunk([{"x": 1}])

        assert count == 1
        mock_client.close.assert_called_once()
