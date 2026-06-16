"""Tests for Transform-in-Target Agent (ELT)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from loafer.agents.transform_in_target import (
    _execute_elt_sql,
    _table_exists,
    transform_in_target_agent,
)
from loafer.config import PostgresTargetConfig
from loafer.exceptions import TransformError


class TestTransformInTargetAgent:
    def test_missing_raw_table_sets_error(self) -> None:
        state: dict[str, Any] = {
            "llm_provider": MagicMock(),
            "target_config": PostgresTargetConfig(
                type="postgres", url="postgresql://localhost/db", table="output"
            ),
            "transform_instruction": "noop",
            "schema_sample": {},
            "duration_ms": {},
            "warnings": [],
        }
        result = transform_in_target_agent(state)
        assert "raw_table_name" in result["last_error"]

    def test_missing_target_table_sets_error(self) -> None:
        state: dict[str, Any] = {
            "llm_provider": MagicMock(),
            "raw_table_name": "raw_data",
            "target_config": PostgresTargetConfig(
                type="postgres", url="postgresql://localhost/db", table=""
            ),
            "transform_instruction": "noop",
            "schema_sample": {},
            "duration_ms": {},
            "warnings": [],
        }
        result = transform_in_target_agent(state)
        assert "target table" in result["last_error"]

    def test_llm_failure_sets_error(self) -> None:
        mock_llm = MagicMock()
        mock_llm.generate_elt_sql.side_effect = Exception("LLM error")

        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "raw_table_name": "raw_data",
            "target_config": PostgresTargetConfig(
                type="postgres", url="postgresql://localhost/db", table="output"
            ),
            "transform_instruction": "noop",
            "schema_sample": {},
            "duration_ms": {},
            "warnings": [],
        }
        result = transform_in_target_agent(state)
        assert "LLM error" in result["last_error"]

    def test_invalid_sql_sets_error(self) -> None:
        mock_llm = MagicMock()
        mock_llm.generate_elt_sql.return_value = MagicMock(
            sql="DROP TABLE users",
            raw_response="DROP TABLE...",
            token_usage={"total_tokens": 50},
        )

        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "raw_table_name": "raw_data",
            "target_config": PostgresTargetConfig(
                type="postgres", url="postgresql://localhost/db", table="output"
            ),
            "transform_instruction": "noop",
            "schema_sample": {},
            "duration_ms": {},
            "warnings": [],
        }
        result = transform_in_target_agent(state)
        assert "validation failed" in result["last_error"]

    def test_sql_execution_failure_sets_error(self) -> None:
        mock_llm = MagicMock()
        mock_llm.generate_elt_sql.return_value = MagicMock(
            sql="SELECT id FROM raw",
            raw_response="SELECT...",
            token_usage={"total_tokens": 50},
        )

        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "raw_table_name": "raw_data",
            "target_config": PostgresTargetConfig(
                type="postgres", url="postgresql://localhost/db", table="output"
            ),
            "transform_instruction": "noop",
            "schema_sample": {},
            "duration_ms": {},
            "warnings": [],
            "last_error": None,
        }

        with patch(
            "loafer.agents.transform_in_target._execute_elt_sql",
            side_effect=Exception("connection refused"),
        ):
            result = transform_in_target_agent(state)

        assert "connection refused" in result["last_error"]

    def test_successful_execution(self) -> None:
        mock_llm = MagicMock()
        mock_llm.generate_elt_sql.return_value = MagicMock(
            sql="SELECT id FROM raw",
            raw_response="SELECT...",
            token_usage={"total_tokens": 50},
        )

        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "raw_table_name": "raw_data",
            "target_config": PostgresTargetConfig(
                type="postgres", url="postgresql://localhost/db", table="output"
            ),
            "transform_instruction": "noop",
            "schema_sample": {},
            "duration_ms": {},
            "warnings": [],
            "last_error": None,
        }

        with (
            patch(
                "loafer.agents.transform_in_target._execute_elt_sql",
                return_value=42,
            ),
            patch(
                "loafer.agents.transform_in_target._table_exists",
                return_value=False,
            ),
        ):
            result = transform_in_target_agent(state)

        assert result["rows_loaded"] == 42
        assert result["generated_sql"] == "SELECT id FROM raw"
        assert result["last_error"] is None
        assert "transform_in_target" in result["duration_ms"]

    def test_write_mode_error_when_table_exists(self) -> None:
        mock_llm = MagicMock()

        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "raw_table_name": "raw_data",
            "target_config": PostgresTargetConfig(
                type="postgres", url="postgresql://localhost/db", table="output", write_mode="error"
            ),
            "transform_instruction": "noop",
            "schema_sample": {},
            "duration_ms": {},
            "warnings": [],
            "last_error": None,
        }

        with patch(
            "loafer.agents.transform_in_target._table_exists",
            return_value=True,
        ):
            result = transform_in_target_agent(state)

        assert "already exists" in result["last_error"]
        mock_llm.generate_elt_sql.assert_not_called()

    def test_failure_increments_retry_counter(self) -> None:
        """BUG-3: the node owns the retry counter so the pure router can see it.

        Each failed pass must bump transform_in_target_retry_count in the
        persisted state; otherwise the router reads 0 forever and loops.
        """
        mock_llm = MagicMock()
        mock_llm.generate_elt_sql.side_effect = Exception("LLM error")

        def _state(count: int) -> dict[str, Any]:
            return {
                "llm_provider": mock_llm,
                "raw_table_name": "raw_data",
                "target_config": PostgresTargetConfig(
                    type="postgres", url="postgresql://localhost/db", table="output"
                ),
                "transform_instruction": "noop",
                "schema_sample": {},
                "duration_ms": {},
                "warnings": [],
                "last_error": None,
                "transform_in_target_retry_count": count,
            }

        first = transform_in_target_agent(_state(0))
        assert first["transform_in_target_retry_count"] == 1

        second = transform_in_target_agent(_state(1))
        assert second["transform_in_target_retry_count"] == 2

    def test_early_validation_failure_increments_counter(self) -> None:
        """Even guard-clause failures (missing raw table) bump the counter."""
        state: dict[str, Any] = {
            "llm_provider": MagicMock(),
            "target_config": PostgresTargetConfig(
                type="postgres", url="postgresql://localhost/db", table="output"
            ),
            "transform_instruction": "noop",
            "schema_sample": {},
            "duration_ms": {},
            "warnings": [],
        }
        result = transform_in_target_agent(state)
        assert result["transform_in_target_retry_count"] == 1


class TestTableExists:
    def test_table_exists_true(self) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True,)
        mock_conn.cursor.return_value = mock_cursor

        with patch("psycopg2.connect", return_value=mock_conn):
            result = _table_exists("postgresql://localhost/db", "my_table")

        assert result is True
        mock_cursor.execute.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_table_exists_false(self) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (False,)
        mock_conn.cursor.return_value = mock_cursor

        with patch("psycopg2.connect", return_value=mock_conn):
            result = _table_exists("postgresql://localhost/db", "missing")

        assert result is False

    def test_connection_error_returns_false(self) -> None:
        with patch("psycopg2.connect", side_effect=Exception("refused")):
            result = _table_exists("postgresql://localhost/db", "my_table")

        assert result is False


class TestExecuteEltSql:
    def test_successful_execution(self) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (100,)
        mock_conn.cursor.return_value = mock_cursor

        with patch("psycopg2.connect", return_value=mock_conn):
            count = _execute_elt_sql("postgresql://localhost/db", "SELECT 1", "output")

        assert count == 100
        mock_cursor.execute.assert_any_call("CREATE TABLE output AS (SELECT 1)")

    def test_replace_mode_drops_table_first(self) -> None:
        """BUG-7: write_mode 'replace' must DROP before CREATE so a re-run works."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (5,)
        mock_conn.cursor.return_value = mock_cursor

        with patch("psycopg2.connect", return_value=mock_conn):
            count = _execute_elt_sql(
                "postgresql://localhost/db", "SELECT 1", "output", write_mode="replace"
            )

        assert count == 5
        executed = [c.args[0] for c in mock_cursor.execute.call_args_list]
        assert "DROP TABLE IF EXISTS output" in executed
        # Drop must come before the create.
        assert executed.index("DROP TABLE IF EXISTS output") < executed.index(
            "CREATE TABLE output AS (SELECT 1)"
        )

    def test_append_mode_does_not_drop(self) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (5,)
        mock_conn.cursor.return_value = mock_cursor

        with patch("psycopg2.connect", return_value=mock_conn):
            _execute_elt_sql("postgresql://localhost/db", "SELECT 1", "output", write_mode="append")

        executed = [c.args[0] for c in mock_cursor.execute.call_args_list]
        assert not any("DROP TABLE" in s for s in executed)

    def test_psycopg2_error_raises(self) -> None:
        import psycopg2

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg2.Error("syntax error")
        mock_conn.cursor.return_value = mock_cursor

        with (
            patch("psycopg2.connect", return_value=mock_conn),
            pytest.raises(TransformError, match="syntax error"),
        ):
            _execute_elt_sql("postgresql://localhost/db", "BAD SQL", "output")
