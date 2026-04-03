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
    ) -> None:
        self._path = path
        self._query = query
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

        self._cursor.execute(self._query)
        while True:
            rows = self._cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield [dict(row) for row in rows]

    def count(self) -> int | None:
        if self._conn is None:
            return None
        try:
            cursor = self._conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM ({self._query})")
            row = cursor.fetchone()
            return int(row[0]) if row else None
        except Exception:
            return None
