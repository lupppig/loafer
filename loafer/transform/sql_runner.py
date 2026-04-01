"""SQL transform runner.

Validates SQL via sqlglot AST analysis, transpiles to the target dialect,
substitutes {{source}} safely via parameterized identifiers, and executes
the query.  Works in both ETL (read results back) and ELT (CREATE TABLE AS)
modes.
"""

from __future__ import annotations

import re
import time
from typing import Any

import sqlglot

from loafer.exceptions import TransformError
from loafer.graph.state import PipelineState
from loafer.transform import TransformRunner
from loafer.transform.sql_validator import validate_transform_sql

_PLACEHOLDER_RE = re.compile(r"\{\{source\}\}")


class SqlTransformRunner(TransformRunner):
    """Execute a SQL-based transform."""

    def run(self, state: PipelineState) -> PipelineState:
        transform_config = state.get("transform_config")
        sql: str | None = transform_config.query if transform_config else None
        if not sql:
            raise TransformError("sql transform requires a 'query' in transform_config")

        source_table: str = state.get("raw_table_name", "loafer_source")

        is_valid, reason = validate_transform_sql(sql)
        if not is_valid:
            raise TransformError(f"SQL validation failed: {reason}")

        sql = _substitute_source(sql, source_table)

        start = time.monotonic()

        mode: str = state.get("mode", "etl")

        if mode == "elt":
            self._run_elt(state, sql, source_table)
        else:
            self._run_etl(state, sql)

        state["duration_ms"]["transform"] = (time.monotonic() - start) * 1000
        return state

    def _run_etl(self, state: PipelineState, sql: str) -> None:
        """Execute SQL in ETL mode — read results into transformed_data."""
        try:
            import duckdb
        except ImportError as exc:
            raise TransformError(
                "SQL transform in ETL mode requires 'duckdb'. Install it with: uv add duckdb"
            ) from exc

        raw_data: list[dict[str, Any]] = state.get("raw_data", [])

        if not raw_data:
            is_streaming: bool = state.get("is_streaming", False)
            if not is_streaming:
                raise TransformError("No raw data available for SQL transform")

        conn = duckdb.connect()

        if raw_data:
            conn.execute(
                "CREATE TABLE loafer_source AS SELECT * FROM raw_data",
                {"raw_data": raw_data},
            )

        try:
            result = conn.execute(sql).fetchall()
            columns = [desc[0] for desc in conn.description] if conn.description else []
            state["transformed_data"] = [dict(zip(columns, row, strict=False)) for row in result]
        except Exception as exc:
            conn.close()
            raise TransformError(f"SQL execution failed: {exc}") from exc

        conn.close()

    def _run_elt(self, state: PipelineState, sql: str, source_table: str) -> None:
        """Execute SQL in ELT mode — CREATE TABLE AS SELECT on target."""
        target_config = state.get("target_config")
        output_table: str | None = target_config.table if target_config else None
        if not output_table:
            raise TransformError("ELT mode requires a target table name")

        sql = _transpile_sql(sql, "postgres")

        create_sql = f"CREATE TABLE {output_table} AS ({sql})"

        try:
            import psycopg2
        except ImportError as exc:
            raise TransformError("ELT SQL transform requires 'psycopg2-binary'") from exc

        target_url: str = target_config.url if target_config else ""
        conn: Any | None = None
        cursor: Any | None = None
        try:
            conn = psycopg2.connect(target_url)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(create_sql)
            cursor.execute(f"SELECT COUNT(*) FROM {output_table}")
            count = cursor.fetchone()[0]
            state["rows_loaded"] = count
            state["generated_sql"] = sql
        except psycopg2.Error as exc:
            raise TransformError(f"ELT SQL execution failed: {exc}") from exc
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()


def _substitute_source(sql: str, table_name: str) -> str:
    """Replace {{source}} with a safely quoted table name.

    Uses ``psycopg2.sql.Identifier`` when available, otherwise falls
    back to double-quoting (standard SQL identifier quoting).
    """
    if not _PLACEHOLDER_RE.search(sql):
        return sql

    try:
        from psycopg2.sql import Identifier

        return _PLACEHOLDER_RE.sub(str(Identifier(table_name)), sql)
    except ImportError:
        safe_name = f'"{table_name.replace(chr(34), chr(34) + chr(34))}"'
        return _PLACEHOLDER_RE.sub(safe_name, sql)


def _transpile_sql(sql: str, target_dialect: str) -> str:
    """Transpile SQL to the target dialect via sqlglot."""
    try:
        transpiled = sqlglot.transpile(sql, read="postgres", write=target_dialect)
        if transpiled:
            return transpiled[0]
    except Exception:
        pass

    return sql
