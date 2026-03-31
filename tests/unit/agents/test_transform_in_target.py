"""Tests for Transform-in-Target Agent (ELT)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loafer.agents.transform_in_target import transform_in_target_agent
from loafer.exceptions import TransformError


class TestTransformInTargetAgent:
    def test_missing_raw_table_raises(self) -> None:
        state: dict[str, Any] = {
            "llm_provider": MagicMock(),
            "target_config": {"type": "postgres", "table": "output"},
            "transform_instruction": "noop",
            "schema_sample": {},
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="raw_table_name"):
            transform_in_target_agent(state)

    def test_missing_target_table_raises(self) -> None:
        state: dict[str, Any] = {
            "llm_provider": MagicMock(),
            "raw_table_name": "raw_data",
            "target_config": {"type": "postgres"},
            "transform_instruction": "noop",
            "schema_sample": {},
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(Exception, match="target table"):
            transform_in_target_agent(state)

    def test_llm_failure_retries(self) -> None:
        mock_llm = MagicMock()
        mock_llm.generate_elt_sql.side_effect = Exception("LLM error")

        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "raw_table_name": "raw_data",
            "target_config": {"type": "postgres", "table": "output"},
            "transform_instruction": "noop",
            "schema_sample": {},
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="failed after 3 attempts"):
            transform_in_target_agent(state)

    def test_invalid_sql_retries(self) -> None:
        mock_llm = MagicMock()
        mock_llm.generate_elt_sql.return_value = MagicMock(
            sql="DROP TABLE users",
            raw_response="DROP TABLE...",
            token_usage={"total_tokens": 50},
        )

        state: dict[str, Any] = {
            "llm_provider": mock_llm,
            "raw_table_name": "raw_data",
            "target_config": {"type": "postgres", "table": "output"},
            "transform_instruction": "noop",
            "schema_sample": {},
            "duration_ms": {},
            "warnings": [],
        }
        with pytest.raises(TransformError, match="validation failed"):
            transform_in_target_agent(state)
