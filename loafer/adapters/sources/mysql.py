"""MySQL source connector adapter."""

from __future__ import annotations

from typing import Any

from loafer.ports.connector import SourceConnector


class MySQLSourceConnector(SourceConnector):
    def __init__(
        self,
        url: str,
        query: str,
        timeout: int = 30,
        incremental_column: str | None = None,
        incremental_value: Any = None,
    ) -> None:
        self._url = url
        self._query = query
        self._timeout = timeout
        self._incremental_column = incremental_column
        self._incremental_value = incremental_value
        self._conn: Any = None
        self._cursor: Any = None
        self._row_count: int | None = None
        self._description: tuple[Any, ...] | None = None

    def connect(self) -> None:
        try:
            import pymysql
        except ImportError as exc:
            from loafer.exceptions import ConnectorError

            raise ConnectorError("MySQL connector requires 'pymysql'") from exc

        from loafer.connectors.registry import _resolve_url

        parsed = _resolve_url(self._url)
        try:
            self._conn = pymysql.connect(
                host=parsed["host"],
                port=parsed["port"] or 3306,
                user=parsed["username"],
                password=parsed["password"],
                database=parsed["database"],
                connect_timeout=self._timeout,
            )
        except pymysql.Error as exc:
            from loafer.exceptions import ConnectorError

            raise ConnectorError(f"failed to connect to MySQL: {exc}") from exc

        try:
            self._cursor = self._conn.cursor()
            query, params = self._resolve_query()
            self._cursor.execute(query, params)
            self._description = self._cursor.description
            self._row_count = self._cursor.rowcount if self._cursor.rowcount >= 0 else None
        except pymysql.Error as exc:
            self._conn.close()
            from loafer.exceptions import ConnectorError

            raise ConnectorError(f"query failed (timeout={self._timeout}s): {exc}") from exc

    def disconnect(self) -> None:
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._conn:
            self._conn.close()
            self._conn = None

    def stream(self, chunk_size: int) -> Any:
        if self._cursor is None:
            from loafer.exceptions import ConnectorError

            raise ConnectorError("connect() must be called before stream()")

        chunk: list[dict[str, Any]] = []
        total_rows = 0

        while True:
            rows = self._cursor.fetchmany(chunk_size)
            if not rows:
                break
            for row in rows:
                total_rows += 1
                chunk.append(self._row_to_dict(row))

            if chunk:
                yield chunk
                chunk = []

        self._row_count = total_rows

    def _resolve_query(self) -> tuple[str, tuple[Any, ...]]:
        """Return the query and bound params, wrapped for incremental extraction."""
        if self._incremental_column is None:
            return self._query, ()
        from loafer.core.incremental import wrap_incremental_query

        wrapped = wrap_incremental_query(self._query, self._incremental_column, "%s", quote="`")
        return wrapped, (self._incremental_value,)

    def _row_to_dict(self, row: tuple[Any, ...]) -> dict[str, Any]:
        if self._description is None:
            return {}
        return {
            col[0]: self._convert_value(val)
            for col, val in zip(self._description, row, strict=False)
        }

    def _convert_value(self, value: Any) -> Any:
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
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    def count(self) -> int | None:
        return self._row_count
