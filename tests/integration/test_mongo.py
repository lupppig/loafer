"""MongoDB source connector integration tests.

Requires a running MongoDB instance. Tests are skipped if MongoDB is not available.
"""

from __future__ import annotations

from typing import Any

import pytest

from loafer.connectors.registry import MongoSourceConnector


class TestMongoSourceConnector:
    """Test MongoDB source connector against a real database."""

    def test_connect_and_disconnect(self, mongo_url: str, mongo_client: Any) -> None:
        conn = MongoSourceConnector(
            url=mongo_url,
            database="test_loafer",
            collection="test_docs",
        )
        mongo_client["test_docs"].insert_one({"x": 1})
        try:
            conn.connect()
            assert conn._coll is not None
        finally:
            conn.disconnect()
        assert conn._coll is None

    def test_stream_small_result(self, mongo_url: str, mongo_client: Any) -> None:
        conn = MongoSourceConnector(
            url=mongo_url,
            database="test_loafer",
            collection="test_docs",
        )
        mongo_client["test_docs"].insert_many([{"val": i} for i in range(10)])
        with conn:
            chunks = list(conn.stream(chunk_size=3))

        total = sum(len(c) for c in chunks)
        assert total == 10
        assert chunks[0][0]["val"] == 0

    def test_stream_chunking(self, mongo_url: str, mongo_client: Any) -> None:
        conn = MongoSourceConnector(
            url=mongo_url,
            database="test_loafer",
            collection="test_docs",
        )
        mongo_client["test_docs"].insert_many([{"val": i} for i in range(100)])
        with conn:
            chunks = list(conn.stream(chunk_size=25))

        assert len(chunks) == 4
        assert all(len(c) == 25 for c in chunks)

    def test_objectid_converted_to_string(self, mongo_url: str, mongo_client: Any) -> None:
        conn = MongoSourceConnector(
            url=mongo_url,
            database="test_loafer",
            collection="test_docs",
        )
        mongo_client["test_docs"].insert_one({"name": "test"})
        with conn:
            chunks = list(conn.stream(chunk_size=1))

        row = chunks[0][0]
        assert "_id" in row
        assert isinstance(row["_id"], str)

    def test_nested_docs_passthrough(self, mongo_url: str, mongo_client: Any) -> None:
        conn = MongoSourceConnector(
            url=mongo_url,
            database="test_loafer",
            collection="test_docs",
        )
        mongo_client["test_docs"].insert_one(
            {
                "name": "nested",
                "address": {"city": "NYC", "zip": "10001"},
                "tags": ["a", "b", "c"],
            }
        )
        with conn:
            chunks = list(conn.stream(chunk_size=1))

        row = chunks[0][0]
        assert row["address"]["city"] == "NYC"
        assert row["tags"] == ["a", "b", "c"]

    def test_filter_option(self, mongo_url: str, mongo_client: Any) -> None:
        conn = MongoSourceConnector(
            url=mongo_url,
            database="test_loafer",
            collection="test_docs",
            filter_doc={"status": "active"},
        )
        mongo_client["test_docs"].insert_many(
            [
                {"name": "a", "status": "active"},
                {"name": "b", "status": "inactive"},
                {"name": "c", "status": "active"},
            ]
        )
        with conn:
            chunks = list(conn.stream(chunk_size=10))

        total = sum(len(c) for c in chunks)
        assert total == 2

    def test_count_returns_document_count(self, mongo_url: str, mongo_client: Any) -> None:
        conn = MongoSourceConnector(
            url=mongo_url,
            database="test_loafer",
            collection="test_docs",
        )
        mongo_client["test_docs"].insert_many([{"val": i} for i in range(42)])
        with conn:
            list(conn.stream(chunk_size=10))

        assert conn.count() == 42

    def test_collection_not_found_raises(self, mongo_url: str) -> None:
        from loafer.exceptions import ExtractionError

        conn = MongoSourceConnector(
            url=mongo_url,
            database="test_loafer",
            collection="nonexistent_collection_xyz",
        )
        with pytest.raises(ExtractionError, match=r"collection.*not found"):
            conn.connect()

    def test_stream_before_connect_raises(self, mongo_url: str, mongo_client: Any) -> None:
        from loafer.exceptions import ExtractionError

        conn = MongoSourceConnector(
            url=mongo_url,
            database="test_loafer",
            collection="test_docs",
        )
        mongo_client["test_docs"].insert_one({"x": 1})
        with pytest.raises(ExtractionError, match="connect\\(\\) must be called"):
            list(conn.stream(chunk_size=1))

    def test_context_manager(self, mongo_url: str, mongo_client: Any) -> None:
        conn = MongoSourceConnector(
            url=mongo_url,
            database="test_loafer",
            collection="test_docs",
        )
        mongo_client["test_docs"].insert_many([{"val": i} for i in range(5)])
        with conn:
            chunks = list(conn.stream(chunk_size=10))
        assert len(chunks) == 1
        assert conn._client is None

    def test_read_all(self, mongo_url: str, mongo_client: Any) -> None:
        conn = MongoSourceConnector(
            url=mongo_url,
            database="test_loafer",
            collection="test_docs",
        )
        mongo_client["test_docs"].insert_many([{"val": i} for i in range(50)])
        with conn:
            rows = conn.read_all()
        assert len(rows) == 50
