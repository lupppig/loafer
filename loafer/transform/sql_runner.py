"""SQL transform runner.

Validates SQL via sqlglot AST analysis, transpiles to the target dialect,
substitutes {{source}} safely via parameterized identifiers, and executes
the query.  Works in both ETL (read results back) and ELT (CREATE TABLE AS)
modes.
"""

from __future__ import annotations

import json
import re
import time
from typing import Any

import sqlglot

from loafer.config import PostgresTargetConfig, SQLTransformConfig
from loafer.core.destructive import detect_destructive_operations, raise_if_destructive
from loafer.exceptions import TransformError
from loafer.graph.state import PipelineState
from loafer.transform import TransformRunner, materialize_input_rows
from loafer.transform.sql_validator import validate_transform_sql

_PLACEHOLDER_RE = re.compile(r"\{\{source\}\}")


class SqlTransformRunner(TransformRunner):
    """Execute a SQL-based transform."""

    def run(self, state: PipelineState) -> PipelineState:
        transform_config = state.get("transform_config")
        if not isinstance(transform_config, SQLTransformConfig):
            raise TransformError("sql transform requires a SQLTransformConfig")
        sql: str = transform_config.query
        if not sql:
            raise TransformError("sql transform requires a 'query' in transform_config")

        source_table: str = state.get("raw_table_name") or "loafer_source"

        # Substitute {{source}} first — it inserts only a quoted identifier — so
        # the validator parses real SQL rather than choking on the placeholder.
        sql = _substitute_source(sql, source_table)

        is_valid, reason = validate_transform_sql(sql)
        if not is_valid:
            raise TransformError(f"SQL validation failed: {reason}")

        start = time.monotonic()

        mode: str = state.get("mode", "etl")

        if mode == "elt":
            self._run_elt(state, sql, source_table)
        else:
            self._run_etl(state, sql, source_table)

        state["duration_ms"]["transform"] = (time.monotonic() - start) * 1000

        # Destructive operation detection (ETL mode only — in-memory comparison)
        if mode == "etl":
            raw_data: list[dict[str, Any]] = state.get("raw_data", [])
            transformed_data: list[dict[str, Any]] = state.get("transformed_data", [])
            before_state = {"raw_data": raw_data}
            after_state = {"transformed_data": transformed_data}
            threshold = state.get("destructive_filter_threshold", 0.3)
            warnings = detect_destructive_operations(before_state, after_state, threshold)
            raise_if_destructive(warnings, state.get("auto_confirmed", False))
            if warnings:
                state.setdefault("destructive_warnings", []).extend(warnings)

        return state

    def _run_etl(self, state: PipelineState, sql: str, source_table: str) -> None:
        """Execute SQL in ETL mode — read results into transformed_data.

        Rows (``list[dict]``) are loaded into an in-memory DuckDB table named
        ``source_table`` so the substituted ``{{source}}`` identifier resolves.
        """
        try:
            import duckdb
        except ImportError as exc:
            raise TransformError(
                "SQL transform in ETL mode requires 'duckdb'. Install it with: uv add duckdb"
            ) from exc

        raw_data = materialize_input_rows(state)
        # Persist the materialized rows so the post-run destructive comparison
        # (which reads state["raw_data"]) is accurate in streaming mode too.
        state["raw_data"] = raw_data

        # An empty input is a valid state (e.g. an upstream pipeline step filtered
        # everything out): there is no schema to build a table from, so the query
        # trivially yields no rows.
        if not raw_data:
            state["transformed_data"] = []
            return

        conn = duckdb.connect()
        try:
            _load_rows_into_duckdb(conn, source_table, raw_data)
            result = conn.execute(sql).fetchall()
            columns = [desc[0] for desc in conn.description] if conn.description else []
            state["transformed_data"] = [dict(zip(columns, row, strict=False)) for row in result]
        except TransformError:
            raise
        except Exception as exc:
            raise TransformError(f"SQL execution failed: {exc}") from exc
        finally:
            conn.close()

    def _run_elt(self, state: PipelineState, sql: str, source_table: str) -> None:
        """Execute SQL in ELT mode — CREATE TABLE AS SELECT on target."""
        target_config = state.get("target_config")
        if not isinstance(target_config, PostgresTargetConfig):
            raise TransformError("ELT mode requires a Postgres target")

        output_table: str = target_config.table
        if not output_table:
            raise TransformError("ELT mode requires a target table name")

        sql = _transpile_sql(sql, "postgres")

        create_sql = f"CREATE TABLE {output_table} AS ({sql})"

        try:
            import psycopg2
        except ImportError as exc:
            raise TransformError("ELT SQL transform requires 'psycopg2-binary'") from exc

        target_url: str = target_config.url
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


def _duckdb_type(values: list[Any]) -> str:
    """Infer a DuckDB column type from the non-null values of a column."""
    found: set[str] = set()
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            found.add("BOOLEAN")
        elif isinstance(v, int):
            found.add("BIGINT")
        elif isinstance(v, float):
            found.add("DOUBLE")
        elif isinstance(v, (dict, list)):
            found.add("JSON")
        else:
            found.add("VARCHAR")

    if not found:
        return "VARCHAR"
    if found == {"BIGINT"}:
        return "BIGINT"
    if found <= {"BIGINT", "DOUBLE"}:
        return "DOUBLE"
    if found == {"BOOLEAN"}:
        return "BOOLEAN"
    if found == {"JSON"}:
        return "JSON"
    return "VARCHAR"


def _load_rows_into_duckdb(conn: Any, table: str, rows: list[dict[str, Any]]) -> None:
    """Create *table* in DuckDB and insert *rows* (a ``list[dict]``).

    DuckDB cannot replacement-scan a Python ``list[dict]`` directly (that needs
    pandas/pyarrow), so the table is built explicitly: column types are inferred
    per column, nested values are JSON-encoded, and rows are inserted via a
    parameterised statement. Column order follows first-seen key order.
    """
    keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                keys.append(key)

    quoted_table = f'"{table.replace(chr(34), chr(34) * 2)}"'

    if not keys:
        conn.execute(f"CREATE TABLE {quoted_table} (_loafer_empty INTEGER)")
        return

    types = {k: _duckdb_type([r.get(k) for r in rows]) for k in keys}
    col_defs = ", ".join(f'"{k.replace(chr(34), chr(34) * 2)}" {types[k]}' for k in keys)
    conn.execute(f"CREATE TABLE {quoted_table} ({col_defs})")

    placeholders = ", ".join("?" for _ in keys)
    encoded_rows: list[list[Any]] = []
    for row in rows:
        encoded: list[Any] = []
        for key in keys:
            value = row.get(key)
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            encoded.append(value)
        encoded_rows.append(encoded)

    conn.executemany(f"INSERT INTO {quoted_table} VALUES ({placeholders})", encoded_rows)


def _substitute_source(sql: str, table_name: str) -> str:
    """Replace {{source}} with a safely quoted table identifier.

    The name is double-quoted with any embedded quotes doubled — standard SQL
    identifier quoting understood by both DuckDB (ETL) and Postgres (ELT).
    Only an identifier is inserted (never user data), so this cannot inject.
    """
    if not _PLACEHOLDER_RE.search(sql):
        return sql

    safe_name = '"' + table_name.replace('"', '""') + '"'
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
