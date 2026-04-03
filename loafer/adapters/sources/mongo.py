"""MongoDB source connector adapter."""

from __future__ import annotations

from typing import Any

from loafer.ports.connector import SourceConnector


class MongoSourceConnector(SourceConnector):
    def __init__(
        self,
        url: str,
        database: str,
        collection: str,
        filter_doc: dict[str, Any] | None = None,
    ) -> None:
        self._url = url
        self._database = database
        self._collection = collection
        self._filter = filter_doc or {}
        self._client: Any = None
        self._coll: Any = None
        self._row_count: int | None = None

    def connect(self) -> None:
        try:
            import pymongo
        except ImportError as exc:
            from loafer.exceptions import ExtractionError

            raise ExtractionError("MongoDB connector requires 'pymongo'") from exc

        try:
            self._client = pymongo.MongoClient(self._url, serverSelectionTimeoutMS=30000)
            self._client.admin.command("ping")
        except pymongo.errors.ConnectionFailure as exc:
            from loafer.exceptions import ExtractionError

            raise ExtractionError(f"failed to connect to MongoDB: {exc}") from exc

        db = self._client[self._database]
        if self._collection not in db.list_collection_names():
            from loafer.exceptions import ExtractionError

            raise ExtractionError(
                f"collection '{self._collection}' not found in database '{self._database}'"
            )
        self._coll = db[self._collection]

        try:
            self._row_count = self._coll.count_documents(self._filter)
        except pymongo.errors.PyMongoError:
            self._row_count = None

    def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
        self._coll = None

    def stream(self, chunk_size: int) -> Any:
        if self._coll is None:
            from loafer.exceptions import ExtractionError

            raise ExtractionError("connect() must be called before stream()")

        cursor = self._coll.find(self._filter, batch_size=chunk_size)
        chunk: list[dict[str, Any]] = []

        for doc in cursor:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
            chunk.append(doc)

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        if chunk:
            yield chunk

    def count(self) -> int | None:
        return self._row_count
