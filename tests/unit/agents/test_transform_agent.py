"""Tests for Transform Agent and runners."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loafer.agents.transform import transform_agent
from loafer.config import AITransformConfig, CustomTransformConfig, SQLTransformConfig
from loafer.exceptions import TransformError


class TestTransformAgent:
    def test_unknown_type_raises(self) -> None:
        state: dict[str, Any] = {
            "transform_config": AITransformConfig(type="ai", instruction="test"),
            "duration_ms": {},
            "warnings": [],
        }
        # Patch the config to have an unknown type
        state["transform_config"] = MagicMock()
        state["transform_config"].type = "unknown"
        with pytest.raises(TransformError, match="Unknown transform type"):
            transform_agent(state)

    def test_routes_to_ai_runner(self) -> None:
        mock_llm = MagicMock()
        mock_llm.generate_transform_function.return_value = MagicMock(
            code="def transform(data): return data",
            raw_response="def transform...",
            token_usage={"total_tokens": 50},
        )

        state: dict[str, Any] = {
            "transform_config": AITransformConfig(type="ai", instruction="noop"),
            "llm_provider": mock_llm,
            "schema_sample": {},
            "transform_instruction": "noop",
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "retry_count": 0,
            "last_error": None,
            "generated_code": "",
            "token_usage": {},
            "duration_ms": {},
            "warnings": [],
        }
        result = transform_agent(state)

        assert "transformed_data" in result

    def test_routes_to_custom_runner(self, tmp_path: Any) -> None:
        transform_file = tmp_path / "t.py"
        transform_file.write_text("def transform(data): return data\n")

        state: dict[str, Any] = {
            "transform_config": CustomTransformConfig(type="custom", path=str(transform_file)),
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "duration_ms": {},
            "warnings": [],
        }
        result = transform_agent(state)

        assert "transformed_data" in result

    def test_routes_to_sql_runner(self) -> None:
        state: dict[str, Any] = {
            "transform_config": SQLTransformConfig(
                type="sql", query="SELECT id, name FROM {{source}}"
            ),
            "raw_data": [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
            "is_streaming": False,
            "mode": "etl",
            "raw_table_name": "loafer_source",
            "auto_confirmed": True,
            "duration_ms": {},
            "warnings": [],
        }
        result = transform_agent(state)

        assert result["transformed_data"] == [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
        ]


class TestAiTransformRunner:
    def test_valid_llm_response(self) -> None:
        from loafer.transform.ai_runner import AiTransformRunner

        mock_llm = MagicMock()
        mock_llm.generate_transform_function.return_value = MagicMock(
            code="def transform(data): return [{**r, 'upper': r.get('name', '').upper()} for r in data]",
            raw_response="def transform...",
            token_usage={"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
        )

        runner = AiTransformRunner()
        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "schema_sample": {"name": {"inferred_type": "string"}},
            "transform_instruction": "uppercase name",
            "raw_data": [{"name": "alice"}, {"name": "bob"}],
            "is_streaming": False,
            "retry_count": 0,
            "last_error": None,
            "generated_code": "",
            "token_usage": {},
            "duration_ms": {},
            "warnings": [],
        }
        result = runner.run(state)

        assert len(result["transformed_data"]) == 2
        assert result["transformed_data"][0]["upper"] == "ALICE"

    def test_validation_failure_triggers_retry(self) -> None:
        from loafer.transform.ai_runner import AiTransformRunner

        mock_llm = MagicMock()
        mock_llm.generate_transform_function.side_effect = [
            MagicMock(
                code="import os\ndef transform(data): return data",
                raw_response="import os...",
                token_usage={"total_tokens": 50},
            ),
            MagicMock(
                code="def transform(data): return data",
                raw_response="def transform...",
                token_usage={"total_tokens": 50},
            ),
        ]

        runner = AiTransformRunner()
        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "schema_sample": {},
            "transform_instruction": "noop",
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "retry_count": 0,
            "last_error": None,
            "generated_code": "",
            "token_usage": {},
            "duration_ms": {},
            "warnings": [],
        }
        result = runner.run(state)

        assert result["retry_count"] == 1
        assert result["last_error"] is None

    def test_three_failures_raises(self) -> None:
        from loafer.transform.ai_runner import AiTransformRunner

        mock_llm = MagicMock()
        mock_llm.generate_transform_function.return_value = MagicMock(
            code="import os\ndef transform(data): return data",
            raw_response="bad code",
            token_usage={"total_tokens": 50},
        )

        runner = AiTransformRunner()
        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "schema_sample": {},
            "transform_instruction": "noop",
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "retry_count": 0,
            "last_error": None,
            "generated_code": "",
            "token_usage": {},
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="failed after 3 attempts"):
            runner.run(state)

    def test_zero_rows_warning(self) -> None:
        from loafer.transform.ai_runner import AiTransformRunner

        mock_llm = MagicMock()
        mock_llm.generate_transform_function.return_value = MagicMock(
            code="def transform(data): return []",
            raw_response="def transform...",
            token_usage={"total_tokens": 50},
        )

        runner = AiTransformRunner()
        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "schema_sample": {},
            "transform_instruction": "filter all",
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "retry_count": 0,
            "last_error": None,
            "generated_code": "",
            "token_usage": {},
            "duration_ms": {},
            "warnings": [],
            "auto_confirmed": True,
        }
        result = runner.run(state)

        assert any("0 rows" in w for w in result["warnings"])

    def test_runtime_error_triggers_retry(self) -> None:
        from loafer.transform.ai_runner import AiTransformRunner

        mock_llm = MagicMock()
        mock_llm.generate_transform_function.side_effect = [
            MagicMock(
                code="def transform(data): raise RuntimeError('bad')",
                raw_response="def transform...",
                token_usage={"total_tokens": 50},
            ),
            MagicMock(
                code="def transform(data): return data",
                raw_response="def transform...",
                token_usage={"total_tokens": 50},
            ),
        ]

        runner = AiTransformRunner()
        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "schema_sample": {},
            "transform_instruction": "noop",
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "retry_count": 0,
            "last_error": None,
            "generated_code": "",
            "token_usage": {},
            "duration_ms": {},
            "warnings": [],
        }
        result = runner.run(state)

        assert result["retry_count"] == 1
        assert result["last_error"] is None


class TestCustomTransformRunner:
    def test_valid_file(self, tmp_path: Any) -> None:
        from loafer.transform.custom_runner import CustomTransformRunner

        transform_file = tmp_path / "transform.py"
        transform_file.write_text("def transform(data): return [{**r, 'x': 1} for r in data]\n")

        runner = CustomTransformRunner()
        state: dict[str, Any] = {
            "transform_config": CustomTransformConfig(type="custom", path=str(transform_file)),
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "duration_ms": {},
            "warnings": [],
        }
        result = runner.run(state)

        assert result["transformed_data"][0]["x"] == 1

    def test_file_not_found_raises(self, tmp_path: Any) -> None:
        from loafer.config import CustomTransformConfig
        from loafer.transform.custom_runner import CustomTransformRunner

        runner = CustomTransformRunner()
        config = CustomTransformConfig.model_construct(
            type="custom", path=str(tmp_path / "nonexistent.py")
        )
        state: dict[str, Any] = {
            "transform_config": config,
            "raw_data": [],
            "is_streaming": False,
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="not found"):
            runner.run(state)

    def test_blocked_import_raises(self, tmp_path: Any) -> None:
        from loafer.transform.custom_runner import CustomTransformRunner

        transform_file = tmp_path / "bad.py"
        transform_file.write_text("import os\ndef transform(data): return data\n")

        runner = CustomTransformRunner()
        state: dict[str, Any] = {
            "transform_config": CustomTransformConfig(type="custom", path=str(transform_file)),
            "raw_data": [],
            "is_streaming": False,
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="validation failed"):
            runner.run(state)

    def test_no_transform_function_raises(self, tmp_path: Any) -> None:
        from loafer.transform.custom_runner import CustomTransformRunner

        transform_file = tmp_path / "empty.py"
        transform_file.write_text("# no function here\n")

        runner = CustomTransformRunner()
        state: dict[str, Any] = {
            "transform_config": CustomTransformConfig(type="custom", path=str(transform_file)),
            "raw_data": [],
            "is_streaming": False,
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="function not defined"):
            runner.run(state)

    def test_runtime_error_raises(self, tmp_path: Any) -> None:
        from loafer.transform.custom_runner import CustomTransformRunner

        transform_file = tmp_path / "bad.py"
        transform_file.write_text("def transform(data): return data['missing']\n")

        runner = CustomTransformRunner()
        state: dict[str, Any] = {
            "transform_config": CustomTransformConfig(type="custom", path=str(transform_file)),
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="failed"):
            runner.run(state)


class TestAuthErrorNotRetried:
    """BUG-6: a bad API key must fail fast with a friendly message, not retry."""

    def test_simple_ai_path_does_not_retry_auth_error(self) -> None:
        from loafer.exceptions import LLMAuthError
        from loafer.transform.ai_runner import AiTransformRunner

        mock_llm = MagicMock()
        mock_llm.generate_transform_function.side_effect = LLMAuthError(
            "400 INVALID_ARGUMENT API_KEY_INVALID"
        )

        runner = AiTransformRunner()
        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "schema_sample": {},
            "transform_instruction": "noop",
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "retry_count": 0,
            "last_error": None,
            "generated_code": "",
            "token_usage": {},
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="Authentication failed"):
            runner.run(state)

        # Called exactly once — no exponential-backoff retries on a bad key.
        assert mock_llm.generate_transform_function.call_count == 1

    def test_config_ai_path_does_not_retry_auth_error(self) -> None:
        from loafer.exceptions import LLMAuthError
        from loafer.transform.ai_runner import AiTransformRunner

        mock_llm = MagicMock()
        mock_llm.generate_transform_function.side_effect = LLMAuthError("API key not valid")

        runner = AiTransformRunner()
        state: dict[str, Any] = {
            "transform_config": AITransformConfig(type="ai", instruction="noop"),
            "llm_provider": mock_llm,
            "schema_sample": {},
            "transform_instruction": "noop",
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "retry_count": 0,
            "last_error": None,
            "generated_code": "",
            "token_usage": {},
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="Authentication failed"):
            runner.run(state)

        assert mock_llm.generate_transform_function.call_count == 1


class TestStreamingInput:
    """BUG-2: transforms must consume stream_iterator in streaming mode.

    In streaming mode the extract agent leaves raw_data empty and exposes a
    chunked iterator via stream_iterator. Runners that transform [] instead
    of the streamed rows silently emit empty output with exit 0 — the
    flagship Postgres → AI → file path always streams (count() is None).
    """

    @staticmethod
    def _chunks(rows: list[dict[str, Any]], size: int = 2) -> Any:
        for i in range(0, len(rows), size):
            yield rows[i : i + size]

    def test_ai_runner_consumes_stream_iterator(self) -> None:
        from loafer.transform.ai_runner import AiTransformRunner

        rows = [{"id": i} for i in range(5)]
        mock_llm = MagicMock()
        mock_llm.generate_transform_function.return_value = MagicMock(
            code="def transform(data): return [{**r, 'seen': True} for r in data]",
            raw_response="def transform...",
            token_usage={"total_tokens": 10},
        )

        runner = AiTransformRunner()
        state: dict[str, Any] = {
            "transform_config": AITransformConfig(type="ai", instruction="mark"),
            "llm_provider": mock_llm,
            "schema_sample": {},
            "transform_instruction": "mark",
            "raw_data": [],
            "is_streaming": True,
            "stream_iterator": self._chunks(rows),
            "retry_count": 0,
            "last_error": None,
            "generated_code": "",
            "token_usage": {},
            "duration_ms": {},
            "warnings": [],
            "auto_confirmed": True,
        }
        result = runner.run(state)

        assert len(result["transformed_data"]) == 5
        assert all(r["seen"] for r in result["transformed_data"])
        # The stream's true count is recorded back into rows_extracted.
        assert result["rows_extracted"] == 5
        assert not any("0 rows" in w for w in result["warnings"])

    def test_simple_ai_path_consumes_stream_iterator(self) -> None:
        """The legacy AI-only path (no AITransformConfig) also streams."""
        from loafer.transform.ai_runner import AiTransformRunner

        rows = [{"id": i} for i in range(4)]
        mock_llm = MagicMock()
        mock_llm.generate_transform_function.return_value = MagicMock(
            code="def transform(data): return data",
            raw_response="def transform...",
            token_usage={"total_tokens": 10},
        )

        runner = AiTransformRunner()
        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "schema_sample": {},
            "transform_instruction": "noop",
            "raw_data": [],
            "is_streaming": True,
            "stream_iterator": self._chunks(rows),
            "retry_count": 0,
            "last_error": None,
            "generated_code": "",
            "token_usage": {},
            "duration_ms": {},
            "warnings": [],
            "auto_confirmed": True,
        }
        result = runner.run(state)

        assert len(result["transformed_data"]) == 4

    def test_custom_runner_consumes_stream_iterator(self, tmp_path: Any) -> None:
        from loafer.transform.custom_runner import CustomTransformRunner

        transform_file = tmp_path / "t.py"
        transform_file.write_text("def transform(data): return data\n")

        rows = [{"id": i} for i in range(3)]
        runner = CustomTransformRunner()
        state: dict[str, Any] = {
            "transform_config": CustomTransformConfig(type="custom", path=str(transform_file)),
            "raw_data": [],
            "is_streaming": True,
            "stream_iterator": self._chunks(rows),
            "duration_ms": {},
            "warnings": [],
        }
        result = runner.run(state)

        assert len(result["transformed_data"]) == 3
        assert result["rows_extracted"] == 3

    def test_streaming_with_none_iterator_raises(self) -> None:
        from loafer.transform.ai_runner import AiTransformRunner

        mock_llm = MagicMock()
        runner = AiTransformRunner()
        state: dict[str, Any] = {
            "transform_config": AITransformConfig(type="ai", instruction="x"),
            "llm_provider": mock_llm,
            "schema_sample": {},
            "transform_instruction": "x",
            "raw_data": [],
            "is_streaming": True,
            "stream_iterator": None,
            "retry_count": 0,
            "last_error": None,
            "generated_code": "",
            "token_usage": {},
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="stream_iterator is None"):
            runner.run(state)


class TestSqlTransformRunner:
    def test_valid_select(self) -> None:
        from loafer.transform.sql_runner import SqlTransformRunner

        runner = SqlTransformRunner()
        state: dict[str, Any] = {
            "transform_config": SQLTransformConfig(
                type="sql", query="SELECT * FROM loafer_source WHERE id > 1"
            ),
            "raw_data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            "is_streaming": False,
            "mode": "etl",
            "raw_table_name": "loafer_source",
            "auto_confirmed": True,
            "duration_ms": {},
            "warnings": [],
        }
        result = runner.run(state)
        assert result["transformed_data"] == [{"id": 2, "name": "Bob"}]

    def test_drop_table_rejected(self) -> None:
        from loafer.transform.sql_runner import SqlTransformRunner

        runner = SqlTransformRunner()
        state: dict[str, Any] = {
            "transform_config": SQLTransformConfig(type="sql", query="DROP TABLE users"),
            "raw_data": [],
            "is_streaming": False,
            "mode": "etl",
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="validation failed"):
            runner.run(state)

    def test_multiple_statements_rejected(self) -> None:
        from loafer.transform.sql_runner import SqlTransformRunner

        runner = SqlTransformRunner()
        state: dict[str, Any] = {
            "transform_config": SQLTransformConfig(type="sql", query="SELECT 1; DELETE FROM users"),
            "raw_data": [],
            "is_streaming": False,
            "mode": "etl",
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="validation failed"):
            runner.run(state)

    def test_source_substitution(self) -> None:
        from loafer.transform.sql_runner import SqlTransformRunner

        runner = SqlTransformRunner()
        state: dict[str, Any] = {
            "transform_config": SQLTransformConfig(type="sql", query="SELECT * FROM {{source}}"),
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "mode": "etl",
            "raw_table_name": "loafer_source",
            "auto_confirmed": True,
            "duration_ms": {},
            "warnings": [],
        }
        result = runner.run(state)
        assert result["transformed_data"] == [{"id": 1}]

    def test_transpile_called(self) -> None:
        from loafer.transform.sql_runner import _transpile_sql

        result = _transpile_sql("SELECT id, name FROM users", "postgres")
        assert isinstance(result, str)
        assert "SELECT" in result

    def test_nonexistent_column_db_error(self) -> None:
        from loafer.transform.sql_runner import SqlTransformRunner

        runner = SqlTransformRunner()
        state: dict[str, Any] = {
            "transform_config": SQLTransformConfig(
                type="sql", query="SELECT nonexistent FROM loafer_source"
            ),
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "mode": "etl",
            "raw_table_name": "loafer_source",
            "auto_confirmed": True,
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="nonexistent"):
            runner.run(state)

    def test_elt_quotes_schema_qualified_adversarial_output(self) -> None:
        from unittest.mock import patch

        from loafer.config import PostgresTargetConfig
        from loafer.transform.sql_runner import SqlTransformRunner
        from tests.postgres_sql import render_sql

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (4,)
        mock_conn.cursor.return_value = mock_cursor
        runner = SqlTransformRunner()
        state: dict[str, Any] = {
            "transform_config": SQLTransformConfig(
                type="sql",
                query="SELECT * FROM {{source}}",
            ),
            "raw_data": [],
            "is_streaming": False,
            "mode": "elt",
            "raw_table_name": "loafer_raw_postgres_1234",
            "target_config": PostgresTargetConfig(
                type="postgres",
                url="postgresql://localhost/db",
                table='analytics.output"; DROP TABLE users; --',
                write_mode="replace",
            ),
            "auto_confirmed": True,
            "duration_ms": {},
            "warnings": [],
        }

        with patch("psycopg2.connect", return_value=mock_conn):
            result = runner.run(state)

        executed = [render_sql(call.args[0]) for call in mock_cursor.execute.call_args_list]
        quoted_table = '"analytics"."output""; DROP TABLE users; --"'
        assert f"DROP TABLE IF EXISTS {quoted_table}" in executed
        assert any(
            statement.startswith(f"CREATE TABLE {quoted_table} AS") for statement in executed
        )
        assert f"SELECT COUNT(*) FROM {quoted_table}" in executed
        assert result["rows_loaded"] == 4
        mock_conn.commit.assert_called_once()
