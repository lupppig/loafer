"""Tests for SQL steps inside a multi-step transform pipeline (Phase 11)."""

from __future__ import annotations

from typing import Any

import pytest

from loafer.agents.transform import transform_agent
from loafer.config import CustomTransformConfig, PipelineTransformConfig, SQLTransformConfig
from loafer.exceptions import TransformError


def _pipeline_state(steps: list[Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "transform_config": PipelineTransformConfig(type="pipeline", steps=steps),
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


def test_sql_step_receives_previous_step_output(tmp_path: Any) -> None:
    """A SQL step's {{source}} sees the prior step's output rows, not the original."""
    custom_file = tmp_path / "add_flag.py"
    custom_file.write_text("def transform(data):\n    return [{**r, 'flag': True} for r in data]\n")

    rows = [{"id": 1}, {"id": 2}]
    steps = [
        CustomTransformConfig(type="custom", path=str(custom_file)),
        SQLTransformConfig(type="sql", query="SELECT id, flag FROM {{source}}"),
    ]
    state = _pipeline_state(steps, rows)

    result = transform_agent(state)

    assert result["transformed_data"] == [
        {"id": 1, "flag": True},
        {"id": 2, "flag": True},
    ]


def test_sql_step_transforms_column() -> None:
    rows = [{"email": "A@X.com"}, {"email": "B@Y.com"}]
    steps = [SQLTransformConfig(type="sql", query="SELECT lower(email) AS email FROM {{source}}")]
    state = _pipeline_state(steps, rows)

    result = transform_agent(state)

    assert result["transformed_data"] == [{"email": "a@x.com"}, {"email": "b@y.com"}]


def test_sql_step_missing_column_errors() -> None:
    rows = [{"id": 1}]
    steps = [
        SQLTransformConfig(name="bad", type="sql", query="SELECT missing_col FROM {{source}}"),
    ]
    state = _pipeline_state(steps, rows)

    with pytest.raises(TransformError) as excinfo:
        transform_agent(state)

    msg = str(excinfo.value).lower()
    assert "missing_col" in msg
