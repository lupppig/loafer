"""Tests for Transform-in-Target Agent (ELT)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from loafer.agents.transform_in_target import transform_in_target_agent
from loafer.config import PostgresTargetConfig


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
