"""Tests for the multi-step transform pipeline runner (Phase 11)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loafer.agents.transform import transform_agent
from loafer.config import (
    AITransformConfig,
    CustomTransformConfig,
    PipelineTransformConfig,
    SQLTransformConfig,
)
from loafer.exceptions import TransformError


def _base_state(config: PipelineTransformConfig, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "transform_config": config,
        "raw_data": rows,
        "transformed_data": [],
        "is_streaming": False,
        "stream_iterator": None,
        "mode": "etl",
        "raw_table_name": "loafer_source",
        "schema_sample": {},
        "token_usage": {},
        "retry_count": 0,
        "last_error": None,
        "generated_code": "",
        "duration_ms": {},
        "warnings": [],
        "destructive_warnings": [],
        "auto_confirmed": True,
        "destructive_filter_threshold": 0.3,
        "step_results": [],
    }


def _mock_llm(code: str) -> MagicMock:
    llm = MagicMock()
    llm.generate_transform_function.return_value = MagicMock(
        code=code,
        raw_response=code,
        token_usage={"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    )
    return llm


class TestPipelineTransform:
    def test_two_step_ai_then_sql(self) -> None:
        """AI step then SQL step: both applied in order."""
        rows = [{"id": 1, "email": "A@X.com"}, {"id": 2, "email": "B@Y.com"}]
        config = PipelineTransformConfig(
            type="pipeline",
            steps=[
                AITransformConfig(type="ai", instruction="lowercase email"),
                SQLTransformConfig(type="sql", query="SELECT id FROM {{source}}"),
            ],
        )
        state = _base_state(config, rows)
        state["llm_provider"] = _mock_llm(
            "def transform(data):\n    return [{**r, 'email': r['email'].lower()} for r in data]\n"
        )

        result = transform_agent(state)

        assert result["transformed_data"] == [{"id": 1}, {"id": 2}]

    def test_three_step_schema_recomputed(self) -> None:
        """Schema sample is recomputed between steps (SQL renames a column)."""
        rows = [{"a": 1}, {"a": 2}, {"a": 3}]
        config = PipelineTransformConfig(
            type="pipeline",
            steps=[
                SQLTransformConfig(type="sql", query="SELECT a AS b FROM {{source}}"),
                SQLTransformConfig(type="sql", query="SELECT b AS c FROM {{source}}"),
                SQLTransformConfig(type="sql", query="SELECT c FROM {{source}} WHERE c > 1"),
            ],
        )
        state = _base_state(config, rows)

        result = transform_agent(state)

        assert result["transformed_data"] == [{"c": 2}, {"c": 3}]

    def test_step_failure_reports_index_name_and_prior_rows(self) -> None:
        """A failure in step 2 names the step and reports step 1's output count."""
        rows = [{"id": i} for i in range(10)]
        config = PipelineTransformConfig(
            type="pipeline",
            steps=[
                SQLTransformConfig(name="keep_all", type="sql", query="SELECT id FROM {{source}}"),
                SQLTransformConfig(name="broken", type="sql", query="SELECT nope FROM {{source}}"),
            ],
        )
        state = _base_state(config, rows)

        with pytest.raises(TransformError) as excinfo:
            transform_agent(state)

        msg = str(excinfo.value)
        assert "step 1" in msg
        assert "broken" in msg
        assert "keep_all" in msg
        assert "10 → 10 rows" in msg

    def test_retry_counter_resets_per_step(self) -> None:
        """An AI step that fails twice then succeeds does not carry retry_count forward."""
        rows = [{"id": 1}]
        config = PipelineTransformConfig(
            type="pipeline",
            steps=[
                AITransformConfig(type="ai", instruction="noop one"),
                AITransformConfig(type="ai", instruction="noop two"),
            ],
        )
        state = _base_state(config, rows)

        calls = {"n": 0}

        def _generate(*_args: Any, **_kwargs: Any) -> MagicMock:
            calls["n"] += 1
            # First call in step 0 fails validation, second succeeds.
            if calls["n"] == 1:
                code = "def transform(a, b): return a"  # wrong signature → validation fail
            else:
                code = "def transform(data): return data"
            return MagicMock(
                code=code,
                raw_response=code,
                token_usage={"total_tokens": 5},
            )

        llm = MagicMock()
        llm.generate_transform_function.side_effect = _generate
        state["llm_provider"] = llm

        result = transform_agent(state)

        assert result["transformed_data"] == [{"id": 1}]
        # step 0 retried once (retry_count 1), step 1 succeeded first try (retry_count 0)
        assert result["step_results"][0].index == 0
        assert result["step_results"][1].index == 1

    def test_stop_on_empty_true_halts_at_emptying_step(self) -> None:
        rows = [{"id": 1}, {"id": 2}]
        config = PipelineTransformConfig(
            type="pipeline",
            stop_on_empty=True,
            steps=[
                SQLTransformConfig(
                    name="empty", type="sql", query="SELECT id FROM {{source}} WHERE id > 99"
                ),
                SQLTransformConfig(name="never", type="sql", query="SELECT id FROM {{source}}"),
            ],
        )
        state = _base_state(config, rows)

        with pytest.raises(TransformError) as excinfo:
            transform_agent(state)

        msg = str(excinfo.value)
        assert "step 0" in msg
        assert "empty" in msg
        # step 1 must not have run
        assert not any(r.name == "never" for r in state["step_results"])

    def test_stop_on_empty_false_continues_past_empty(self) -> None:
        rows = [{"id": 1}, {"id": 2}]
        config = PipelineTransformConfig(
            type="pipeline",
            stop_on_empty=False,
            steps=[
                SQLTransformConfig(
                    name="empty", type="sql", query="SELECT id FROM {{source}} WHERE id > 99"
                ),
                SQLTransformConfig(
                    name="passthrough", type="sql", query="SELECT id FROM {{source}}"
                ),
            ],
        )
        state = _base_state(config, rows)

        result = transform_agent(state)

        assert result["transformed_data"] == []
        assert len(result["step_results"]) == 2
        assert result["step_results"][1].name == "passthrough"

    def test_named_steps_recorded(self) -> None:
        rows = [{"id": 1}]
        config = PipelineTransformConfig(
            type="pipeline",
            steps=[SQLTransformConfig(name="only", type="sql", query="SELECT id FROM {{source}}")],
        )
        state = _base_state(config, rows)

        result = transform_agent(state)

        assert result["step_results"][0].name == "only"

    def test_unnamed_steps_default_names(self) -> None:
        rows = [{"id": 1}]
        config = PipelineTransformConfig(
            type="pipeline",
            steps=[
                SQLTransformConfig(type="sql", query="SELECT id FROM {{source}}"),
                SQLTransformConfig(type="sql", query="SELECT id FROM {{source}}"),
            ],
        )
        state = _base_state(config, rows)

        result = transform_agent(state)

        assert result["step_results"][0].name == "step_0"
        assert result["step_results"][1].name == "step_1"

    def test_token_usage_accumulated_across_ai_steps(self) -> None:
        rows = [{"id": 1}]
        config = PipelineTransformConfig(
            type="pipeline",
            steps=[
                AITransformConfig(type="ai", instruction="one"),
                AITransformConfig(type="ai", instruction="two"),
            ],
        )
        state = _base_state(config, rows)
        state["llm_provider"] = _mock_llm("def transform(data): return data")

        result = transform_agent(state)

        # Two AI steps, each 15 total tokens.
        assert result["token_usage"]["total_tokens"] == 30

    def test_timing_recorded_per_step(self) -> None:
        rows = [{"id": 1}]
        config = PipelineTransformConfig(
            type="pipeline",
            steps=[SQLTransformConfig(type="sql", query="SELECT id FROM {{source}}")],
        )
        state = _base_state(config, rows)

        result = transform_agent(state)

        assert result["step_results"][0].duration_ms >= 0
        assert "transform" in result["duration_ms"]

    def test_custom_step_in_pipeline(self, tmp_path: Any) -> None:
        transform_file = tmp_path / "double.py"
        transform_file.write_text(
            "def transform(data):\n    return [{**r, 'id': r['id'] * 2} for r in data]\n"
        )
        rows = [{"id": 1}, {"id": 2}]
        config = PipelineTransformConfig(
            type="pipeline",
            steps=[CustomTransformConfig(type="custom", path=str(transform_file))],
        )
        state = _base_state(config, rows)

        result = transform_agent(state)

        assert result["transformed_data"] == [{"id": 2}, {"id": 4}]
