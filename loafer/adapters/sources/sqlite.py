"""SQLite source connector adapter."""

from __future__ import annotations

from typing import Any

from loafer.ports.connector import SourceConnector


class SqliteSourceConnector(SourceConnector):
    """Stream rows from a SQLite database via SQL query."""

    def __init__(
        self,
        path: str,
        query: str,
        incremental_column: str | None = None,
        incremental_value: Any = None,
    ) -> None:
        self._path = path
        self._query = query
        self._incremental_column = incremental_column
        self._incremental_value = incremental_value
        self._conn: Any = None
        self._cursor: Any = None

    def connect(self) -> None:
        import sqlite3

        try:
            self._conn = sqlite3.connect(self._path)
            self._conn.row_factory = sqlite3.Row
            self._cursor = self._conn.cursor()
        except sqlite3.Error as exc:
            from loafer.exceptions import ConnectorError

            raise ConnectorError(f"failed to connect to SQLite: {exc}") from exc

    def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
            self._cursor = None

    def stream(self, chunk_size: int) -> Any:
        if self._cursor is None:
            from loafer.exceptions import ConnectorError

            raise ConnectorError("not connected")

        query, params = self._resolve_query()
        self._cursor.execute(query, params)
        while True:
            rows = self._cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield [dict(row) for row in rows]

    def _resolve_query(self) -> tuple[str, tuple[Any, ...]]:
        """Return the query and bound params, wrapped for incremental extraction."""
        if self._incremental_column is None:
            return self._query, ()
        from loafer.core.incremental import wrap_incremental_query

        wrapped = wrap_incremental_query(self._query, self._incremental_column, "?")
        return wrapped, (self._incremental_value,)

    def count(self) -> int | None:
        if self._conn is None:
            return None
        try:
            query, params = self._resolve_query()
            cursor = self._conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM ({query})", params)
            row = cursor.fetchone()
            return int(row[0]) if row else None
        except Exception:
            return None
