"""PostgreSQL source connector adapter."""

from __future__ import annotations

from typing import Any

from loafer.ports.connector import SourceConnector


class PostgresSourceConnector(SourceConnector):
    def __init__(self, url: str, query: str, timeout: int = 30) -> None:
        self._url = url
        self._query = query
        self._timeout = timeout
        self._conn: Any = None
        self._cursor: Any = None
        self._row_count: int | None = None

    def connect(self) -> None:
        try:
            import psycopg2
        except ImportError as exc:
            from loafer.exceptions import ExtractionError

            raise ExtractionError("PostgreSQL connector requires 'psycopg2-binary'") from exc

        try:
            self._conn = psycopg2.connect(self._url, connect_timeout=self._timeout)
        except psycopg2.Error as exc:
            from loafer.exceptions import ExtractionError

            raise ExtractionError(f"failed to connect to PostgreSQL: {exc}") from exc

        try:
            self._conn.set_client_encoding("UTF8")
            tmp_cursor = self._conn.cursor()
            tmp_cursor.execute(f"SET statement_timeout = '{self._timeout * 1000}'")
            tmp_cursor.close()
        except psycopg2.Error as exc:
            self._conn.close()
            from loafer.exceptions import ExtractionError

            raise ExtractionError(
                f"failed to set timeout (timeout={self._timeout}s): {exc}"
            ) from exc

        try:
            cursor = self._conn.cursor(name="loafer_stream")
            cursor.itersize = 500
            cursor.execute(self._query)
            self._cursor = cursor
            self._row_count = cursor.rowcount if cursor.rowcount >= 0 else None
        except psycopg2.Error as exc:
            self._conn.close()
            from loafer.exceptions import ExtractionError

            raise ExtractionError(f"query failed (timeout={self._timeout}s): {exc}") from exc

    def disconnect(self) -> None:
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._conn:
            self._conn.close()
            self._conn = None

    def stream(self, chunk_size: int) -> Any:
        if self._cursor is None:
            from loafer.exceptions import ExtractionError

            raise ExtractionError("connect() must be called before stream()")

        chunk: list[dict[str, Any]] = []
        total_rows = 0

        for row in self._cursor:
            total_rows += 1
            chunk.append(self._row_to_dict(row))

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        if chunk:
            yield chunk

        self._row_count = total_rows

    def _row_to_dict(self, row: tuple[Any, ...]) -> dict[str, Any]:
        if self._cursor is None:
            return {}
        return {
            col.name: self._convert_value(val, col.type_code)
            for col, val in zip(self._cursor.description, row, strict=False)
        }

    def _convert_value(self, value: Any, type_code: int) -> Any:
        if value is None:
            return None
        if isinstance(value, (int, float, str, bool)):
            return value
        try:
            from decimal import Decimal

            if isinstance(value, Decimal):
                return float(value)
        except ImportError:
            pass
        try:
            import uuid

            if isinstance(value, uuid.UUID):
                return str(value)
        except ImportError:
            pass
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    def count(self) -> int | None:
        return self._row_count
