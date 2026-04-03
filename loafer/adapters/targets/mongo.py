"""MongoDB target connector adapter."""

from __future__ import annotations

from typing import Any

from loafer.ports.connector import TargetConnector


class MongoTargetConnector(TargetConnector):
    """Write data to a MongoDB collection."""

    def __init__(
        self,
        url: str,
        database: str,
        collection: str,
        write_mode: str = "append",
    ) -> None:
        self._url = url
        self._database = database
        self._collection_name = collection
        self._write_mode = write_mode
        self._client: Any = None
        self._collection: Any = None

    def connect(self) -> None:
        try:
            import pymongo
        except ImportError:
            from loafer.exceptions import LoadError

            raise LoadError("MongoDB connector requires 'pymongo'")

        try:
            self._client = pymongo.MongoClient(self._url)
            db = self._client[self._database]
            self._collection = db[self._collection_name]

            if self._write_mode == "replace":
                self._collection.drop()
            elif (
                self._write_mode == "error" and self._collection_name in db.list_collection_names()
            ):
                from loafer.exceptions import LoadError

                raise LoadError(
                    f"collection '{self._collection_name}' already exists and write_mode is 'error'"
                )
        except pymongo.errors.PyMongoError as exc:
            from loafer.exceptions import LoadError

            raise LoadError(f"failed to connect to MongoDB: {exc}") from exc

    def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
            self._collection = None

    def write_chunk(self, chunk: list[dict[str, Any]]) -> int:
        if self._collection is None:
            from loafer.exceptions import LoadError

            raise LoadError("not connected")

        if not chunk:
            return 0

        clean_chunk: list[dict[str, Any]] = []
        for doc in chunk:
            clean_doc: dict[str, Any] = {}
            for key, val in doc.items():
                clean_doc[key] = val
            clean_chunk.append(clean_doc)

        result = self._collection.insert_many(clean_chunk)
        return len(result.inserted_ids)

    def finalize(self) -> None:
        pass
