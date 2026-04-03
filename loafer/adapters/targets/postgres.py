"""PostgreSQL target connector adapter."""

from __future__ import annotations

from typing import Any

from loafer.ports.connector import TargetConnector


class PostgresTargetConnector(TargetConnector):
    def __init__(self, url: str, table: str, write_mode: str = "append") -> None:
        self._url = url
        self._table = table
        self._write_mode = write_mode
        self._conn: Any = None
        self._cursor: Any = None
        self._rows_written = 0
        self._columns: list[str] = []

    def connect(self) -> None:
        try:
            import psycopg2
        except ImportError as exc:
            from loafer.exceptions import LoadError

            raise LoadError("PostgreSQL connector requires 'psycopg2-binary'") from exc

        try:
            self._conn = psycopg2.connect(self._url)
            self._conn.autocommit = False
        except psycopg2.Error as exc:
            from loafer.exceptions import LoadError

            raise LoadError(f"failed to connect to PostgreSQL: {exc}") from exc

        self._cursor = self._conn.cursor()

    def disconnect(self) -> None:
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._conn:
            self._conn.close()
            self._conn = None

    def write_chunk(self, chunk: list[dict[str, Any]]) -> int:
        if self._conn is None or self._cursor is None:
            from loafer.exceptions import LoadError

            raise LoadError("connect() must be called before write_chunk()")

        if not chunk:
            return 0

        import psycopg2
        import psycopg2.extras

        table_exists = self._table_exists()
        if not table_exists:
            self._columns = list(chunk[0].keys())
            self._create_table(chunk[0])
        elif not self._columns:
            self._columns = list(chunk[0].keys())

        batch_size = min(len(chunk), 100)
        rows_in_batch = 0

        for i in range(0, len(chunk), batch_size):
            batch = chunk[i : i + batch_size]
            cols = list(batch[0].keys()) if not self._columns else self._columns
            col_names = ", ".join(cols)

            query = f"INSERT INTO {self._table} ({col_names}) VALUES %s"
            values = [self._serialize_value(row, cols) for row in batch]

            try:
                psycopg2.extras.execute_values(
                    self._cursor, query, values, template=None, page_size=batch_size
                )
                self._conn.commit()
                rows_in_batch += len(batch)
            except psycopg2.Error as exc:
                self._conn.rollback()
                from loafer.exceptions import LoadError

                raise LoadError(f"batch insert failed ({len(batch)} rows): {exc}") from exc

        self._rows_written += rows_in_batch
        return rows_in_batch

    def _table_exists(self) -> bool:
        import psycopg2

        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            )
        """
        try:
            self._cursor.execute(query, (self._table,))
            return bool(self._cursor.fetchone()[0])
        except psycopg2.Error:
            return False

    def _create_table(self, sample_row: dict[str, Any]) -> None:
        import psycopg2

        col_defs: list[str] = []
        for col, val in sample_row.items():
            pg_type = self._infer_pg_type(val)
            col_defs.append(f'"{col}" {pg_type}')

        query = f"CREATE TABLE {self._table} ({', '.join(col_defs)})"
        try:
            self._cursor.execute(query)
            self._conn.commit()
        except psycopg2.Error as exc:
            self._conn.rollback()
            from loafer.exceptions import LoadError

            raise LoadError(f"failed to create table {self._table}: {exc}") from exc

    def _infer_pg_type(self, value: Any) -> str:
        if value is None:
            return "TEXT"
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int):
            return "BIGINT"
        if isinstance(value, float):
            return "DOUBLE PRECISION"
        if isinstance(value, (dict, list)):
            return "JSONB"
        return "TEXT"

    def _serialize_value(self, row: dict[str, Any], cols: list[str]) -> tuple[Any, ...]:
        result: list[Any] = []
        for c in cols:
            v: Any | None = row.get(c)
            if isinstance(v, (dict, list)):
                import json

                result.append(json.dumps(v))
            else:
                result.append(v)
        return tuple(result)

    def finalize(self) -> None:
        if self._conn:
            try:
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
