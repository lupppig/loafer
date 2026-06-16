"""Transform-in-Target Agent — pure function over PipelineState.

ELT-only agent.  Raw data is already in the target database.  This agent
generates SQL via LLM, validates it, and executes CREATE TABLE AS SELECT.

On failure the agent sets ``state["last_error"]`` and returns; the graph
decides whether to retry.
"""

from __future__ import annotations

import time
from typing import Any

from loafer.config import PostgresTargetConfig
from loafer.exceptions import TransformError
from loafer.graph.state import PipelineState
from loafer.llm.base import ELTSQLResult, LLMProvider
from loafer.llm.prompt_builder import build_elt_sql_prompt
from loafer.transform.sql_validator import validate_transform_sql


def transform_in_target_agent(state: PipelineState) -> PipelineState:
    """Generate and execute ELT SQL on the target database.

    Returns the updated PipelineState with rows_loaded and generated_sql set.
    On failure sets ``state["last_error"]`` and bumps the graph retry counter
    so the (pure) conditional router can decide whether to retry. The counter
    must be incremented here — in a node — because LangGraph discards state
    writes made inside routing functions, which is what caused the original
    infinite retry loop.
    """
    start = time.monotonic()

    llm_provider: LLMProvider = state["llm_provider"]
    raw_table_name: str | None = state.get("raw_table_name")
    if not raw_table_name:
        return _fail(state, "ELT mode requires raw_table_name in state", start)

    target_config = state.get("target_config")
    if not isinstance(target_config, PostgresTargetConfig):
        return _fail(state, "ELT mode requires a Postgres target", start)

    output_table: str = target_config.table
    if not output_table:
        return _fail(state, "ELT mode requires a target table name", start)

    write_mode: str = target_config.write_mode
    if write_mode == "error" and _table_exists(target_config.url, output_table):
        return _fail(
            state,
            f"Target table '{output_table}' already exists and write_mode is 'error'",
            start,
        )

    instruction: str = state.get("transform_instruction", "")
    target_schema: dict[str, Any] = state.get("schema_sample", {})
    previous_error: str | None = state.get("last_error")

    try:
        build_elt_sql_prompt(
            target_schema,
            raw_table_name,
            instruction,
            previous_error=previous_error,
        )
    except Exception as exc:
        return _fail(state, f"Failed to build ELT SQL prompt: {exc}", start)

    try:
        result: ELTSQLResult = llm_provider.generate_elt_sql(
            target_schema,
            raw_table_name,
            instruction,
            previous_error=previous_error,
        )
    except Exception as exc:
        return _fail(state, f"LLM call failed: {exc}", start)

    sql = result.sql

    is_valid, reason = validate_transform_sql(sql)
    if not is_valid:
        return _fail(state, f"SQL validation failed: {reason}", start)

    try:
        count = _execute_elt_sql(target_config.url, sql, output_table, write_mode)
    except Exception as exc:
        return _fail(state, f"SQL execution failed: {exc}", start)

    state["rows_loaded"] = count
    state["generated_sql"] = sql
    state["last_error"] = None
    state["duration_ms"]["transform_in_target"] = (time.monotonic() - start) * 1000
    return state


def _fail(state: PipelineState, message: str, start: float) -> PipelineState:
    """Record a failure and bump the persisted graph retry counter.

    The counter lives on the node (not the router) so LangGraph actually
    persists it between retries; otherwise the router reads 0 every pass and
    retries forever.
    """
    state["last_error"] = message
    state["transform_in_target_retry_count"] = state.get("transform_in_target_retry_count", 0) + 1
    state.setdefault("duration_ms", {})["transform_in_target"] = (time.monotonic() - start) * 1000
    return state


def _table_exists(url: str, table: str) -> bool:
    """Check if a table exists in the target database."""
    try:
        import psycopg2
    except ImportError:
        return False

    try:
        conn = psycopg2.connect(url)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            )
            """,
            (table,),
        )
        result = bool(cursor.fetchone()[0])
        cursor.close()
        conn.close()
        return result
    except Exception:
        return False


def _execute_elt_sql(url: str, sql: str, output_table: str, write_mode: str = "append") -> int:
    """Execute CREATE TABLE AS SELECT on the target database.

    Honors ``write_mode``: ``replace`` drops any existing output table first
    so a second run doesn't error on the already-existing table (BUG-7); the
    default creates the table, which is why a plain CTAS without the drop
    fails on a re-run.
    """
    try:
        import psycopg2
    except ImportError as exc:
        raise TransformError("ELT SQL transform requires 'psycopg2-binary'") from exc

    create_sql = f"CREATE TABLE {output_table} AS ({sql})"

    try:
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cursor = conn.cursor()
        if write_mode == "replace":
            cursor.execute(f"DROP TABLE IF EXISTS {output_table}")
        cursor.execute(create_sql)
        cursor.execute(f"SELECT COUNT(*) FROM {output_table}")
        row = cursor.fetchone()
        count: int = int(row[0]) if row else 0
        cursor.close()
        conn.close()
        return count
    except psycopg2.Error as exc:
        raise TransformError(f"ELT SQL execution failed: {exc}") from exc
