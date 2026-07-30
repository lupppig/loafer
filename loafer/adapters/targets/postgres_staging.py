"""Run-scoped PostgreSQL staging target with atomic final publication."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from psycopg2 import sql

from loafer.adapters.postgres_sql import column_list, qualified_identifier
from loafer.core.identifiers import split_qualified_name
from loafer.exceptions import LoadError
from loafer.ports.connector import TargetConnector


class PostgresStagingTargetConnector(TargetConnector):
    """Write batches to a hidden table, then merge/swap in one transaction."""

    def __init__(
        self,
        url: str,
        table: str,
        write_mode: str,
        key: list[str] | None,
        run_id: str,
    ) -> None:
        self._url = url
        self._table = table
        self._write_mode = write_mode
        self._key = key or []
        self._run_id = run_id
        schema, _table_name = split_qualified_name(table)
        identity = f"{table}:{run_id}".encode()
        stage_name = f"_loafer_stage_{hashlib.sha256(identity).hexdigest()[:24]}"
        self._staging_table = f"{schema}.{stage_name}"
        self._conn: Any = None
        self._cursor: Any = None
        self._columns: list[str] = []
        self._staging_created = False
        self._published = False
        self._rows_written = 0

    def connect(self) -> None:
        try:
            import psycopg2

            self._conn = psycopg2.connect(self._url)
            self._conn.autocommit = False
            self._cursor = self._conn.cursor()
            self._drop_staging()
            self._conn.commit()
        except Exception as exc:
            self.disconnect()
            raise LoadError(f"failed to connect PostgreSQL staging target: {exc}") from exc

    def disconnect(self) -> None:
        if self._conn is not None and not self._published:
            try:
                self._conn.rollback()
                if self._cursor is not None:
                    self._drop_staging()
                    self._conn.commit()
            except Exception:
                try:
                    self._conn.rollback()
                except Exception:
                    pass
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def write_chunk(self, chunk: list[dict[str, Any]]) -> int:
        if self._conn is None or self._cursor is None:
            raise LoadError("connect() must be called before write_chunk()")
        if not chunk:
            return 0

        try:
            import psycopg2.extras

            if not self._staging_created:
                self._columns = list(chunk[0])
                self._create_staging(chunk[0])
                self._staging_created = True
            self._assert_columns(chunk)

            query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                qualified_identifier(self._staging_table),
                column_list(self._columns),
            )
            values = [self._serialize(row) for row in chunk]
            psycopg2.extras.execute_values(
                self._cursor,
                query,
                values,
                template=None,
                page_size=min(len(chunk), 1000),
            )
            self._conn.commit()
        except Exception as exc:
            self._conn.rollback()
            raise LoadError(f"staging batch insert failed ({len(chunk)} rows): {exc}") from exc

        self._rows_written += len(chunk)
        return len(chunk)

    def finalize(self) -> None:
        if self._conn is None or self._cursor is None:
            return
        if not self._staging_created:
            self._publish_empty()
            self._published = True
            return

        try:
            if self._write_mode == "replace":
                self._replace()
            elif self._write_mode == "error":
                self._publish_new(error_if_exists=True)
            elif self._write_mode == "append":
                self._append()
            elif self._write_mode == "upsert":
                self._upsert()
            else:
                raise LoadError(f"unsupported PostgreSQL write mode: {self._write_mode}")
            self._conn.commit()
            self._published = True
        except Exception as exc:
            self._conn.rollback()
            if isinstance(exc, LoadError):
                raise
            raise LoadError(f"failed to publish PostgreSQL staging table: {exc}") from exc

    def _create_staging(self, sample: dict[str, Any]) -> None:
        definitions = [
            sql.SQL("{} {}").format(
                sql.Identifier(column),
                sql.SQL(_infer_pg_type(value)),
            )
            for column, value in sample.items()
        ]
        query = sql.SQL("CREATE TABLE {} ({})").format(
            qualified_identifier(self._staging_table),
            sql.SQL(", ").join(definitions),
        )
        self._cursor.execute(query)

    def _replace(self) -> None:
        self._cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(qualified_identifier(self._table))
        )
        self._rename_staging()

    def _publish_new(self, *, error_if_exists: bool) -> None:
        if self._table_exists(self._table):
            if error_if_exists:
                raise LoadError(f"table '{self._table}' already exists and write_mode is 'error'")
            return
        self._rename_staging()

    def _append(self) -> None:
        if not self._table_exists(self._table):
            self._rename_staging()
            return
        self._cursor.execute(
            sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                qualified_identifier(self._table),
                column_list(self._columns),
                column_list(self._columns),
                qualified_identifier(self._staging_table),
            )
        )
        self._drop_staging()

    def _upsert(self) -> None:
        if not self._key:
            raise LoadError("PostgreSQL staged upsert requires key columns")
        if not self._table_exists(self._table):
            self._rename_staging()
            self._ensure_unique_index()
            return

        self._ensure_unique_index()
        updates = [column for column in self._columns if column not in self._key]
        if updates:
            action = sql.SQL("DO UPDATE SET {}").format(
                sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(
                        sql.Identifier(column),
                        sql.Identifier(column),
                    )
                    for column in updates
                )
            )
        else:
            action = sql.SQL("DO NOTHING")
        query = sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) {}").format(
            qualified_identifier(self._table),
            column_list(self._columns),
            column_list(self._columns),
            qualified_identifier(self._staging_table),
            column_list(self._key),
            action,
        )
        self._cursor.execute(query)
        self._drop_staging()

    def _publish_empty(self) -> None:
        target_exists = self._table_exists(self._table)
        if self._write_mode == "replace":
            if target_exists:
                self._cursor.execute(
                    sql.SQL("TRUNCATE TABLE {}").format(qualified_identifier(self._table))
                )
                self._conn.commit()
                return
            raise LoadError(
                "cannot publish an empty PostgreSQL replacement because no output schema "
                "or existing target table is available"
            )
        if self._write_mode == "error" and target_exists:
            raise LoadError(f"table '{self._table}' already exists and write_mode is 'error'")
        if not target_exists:
            raise LoadError(
                "cannot publish an empty PostgreSQL run because no output schema "
                "or existing target table is available"
            )
        self._conn.commit()

    def _rename_staging(self) -> None:
        _schema, final_name = split_qualified_name(self._table)
        self._cursor.execute(
            sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                qualified_identifier(self._staging_table),
                sql.Identifier(final_name),
            )
        )
        self._staging_created = False

    def _drop_staging(self) -> None:
        self._cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(qualified_identifier(self._staging_table))
        )
        self._staging_created = False

    def _table_exists(self, table: str) -> bool:
        schema, table_name = split_qualified_name(table)
        self._cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
            """,
            (schema, table_name),
        )
        return bool(self._cursor.fetchone()[0])

    def _ensure_unique_index(self) -> None:
        identity = f"{self._table}:{','.join(self._key)}".encode()
        index_name = f"loafer_uq_{hashlib.sha256(identity).hexdigest()[:16]}"
        self._cursor.execute(
            sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(index_name),
                qualified_identifier(self._table),
                column_list(self._key),
            )
        )

    def _assert_columns(self, chunk: list[dict[str, Any]]) -> None:
        expected = set(self._columns)
        for row in chunk:
            if set(row) != expected:
                raise LoadError(
                    "PostgreSQL staging batch schema changed after the first output batch"
                )

    def _serialize(self, row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(
            json.dumps(row[column]) if isinstance(row[column], (dict, list)) else row[column]
            for column in self._columns
        )


def _infer_pg_type(value: Any) -> str:
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
