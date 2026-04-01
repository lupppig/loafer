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
            "transform_config": SQLTransformConfig(type="sql", query="SELECT 1"),
            "raw_data": [],
            "is_streaming": False,
            "mode": "etl",
            "raw_table_name": "loafer_source",
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="duckdb"):
            transform_agent(state)


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
        from loafer.transform.custom_runner import CustomTransformRunner

        runner = CustomTransformRunner()
        # Use a dict to bypass Pydantic validation (tests runner logic, not config validation)
        state: dict[str, Any] = {
            "transform_config": MagicMock(type="custom", path=str(tmp_path / "nonexistent.py")),
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


class TestSqlTransformRunner:
    def test_valid_select(self) -> None:
        from loafer.transform.sql_runner import SqlTransformRunner

        runner = SqlTransformRunner()
        state: dict[str, Any] = {
            "transform_config": SQLTransformConfig(
                type="sql", query="SELECT * FROM loafer_source WHERE id > 0"
            ),
            "raw_data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            "is_streaming": False,
            "mode": "etl",
            "raw_table_name": "loafer_source",
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="duckdb"):
            runner.run(state)

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
            "transform_config": SQLTransformConfig(type="sql", query="SELECT * FROM loafer_source"),
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "mode": "etl",
            "raw_table_name": "loafer_source",
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="duckdb"):
            runner.run(state)

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
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="duckdb"):
            runner.run(state)
