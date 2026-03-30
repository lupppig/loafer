"""Shared test fixtures for Loafer."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest


@pytest.fixture()
def sample_schema() -> dict[str, dict[str, Any]]:
    """Realistic schema sample with 5 columns, mixed types."""
    return {
        "id": {
            "inferred_type": "integer",
            "nullable": False,
            "sample_values": [1, 2, 3, 4, 5],
            "null_count": 0,
            "total_count": 100,
        },
        "name": {
            "inferred_type": "string",
            "nullable": True,
            "sample_values": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "null_count": 3,
            "total_count": 100,
        },
        "email": {
            "inferred_type": "string",
            "nullable": False,
            "sample_values": [
                "alice@example.com",
                "bob@example.com",
                "charlie@example.com",
                "diana@example.com",
                "eve@example.com",
            ],
            "null_count": 0,
            "total_count": 100,
        },
        "score": {
            "inferred_type": "float",
            "nullable": True,
            "sample_values": [98.5, 87.2, 92.1, 76.4, 88.9],
            "null_count": 5,
            "total_count": 100,
        },
        "created_at": {
            "inferred_type": "datetime",
            "nullable": False,
            "sample_values": [
                "2024-01-15T10:30:00",
                "2024-02-20T14:45:00",
                "2024-03-10T09:15:00",
                "2024-04-05T16:00:00",
                "2024-05-12T11:20:00",
            ],
            "null_count": 0,
            "total_count": 100,
        },
    }


def _build_fixture_rows(count: int = 10) -> list[dict[str, Any]]:
    """Build fixture data rows for testing."""
    return [
        {
            "id": i,
            "name": f"User {i}" if i % 4 != 0 else None,
            "email": f"user{i}@example.com",
            "score": round(70 + (i * 2.5), 1) if i % 5 != 0 else None,
            "created_at": f"2024-{(i % 12) + 1:02d}-15T10:30:00",
        }
        for i in range(1, count + 1)
    ]


@pytest.fixture()
def minimal_etl_state(sample_schema: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Fully populated PipelineState for ETL mode with postgres source, CSV target."""
    return {
        "source_config": {
            "type": "postgres",
            "url": "postgresql://loafer:loafer@localhost:5432/loafer_dev",
            "query": "SELECT * FROM users",
            "timeout": 30,
        },
        "target_config": {
            "type": "csv",
            "path": "/tmp/output.csv",
            "write_mode": "overwrite",
        },
        "transform_config": {
            "type": "ai",
            "instruction": "lowercase all name values",
        },
        "llm_config": {
            "provider": "gemini",
            "model": "gemini-1.5-flash",
            "api_key": "test-key",
        },
        "transform_instruction": "lowercase all name values",
        "mode": "etl",
        "chunk_size": 500,
        "streaming_threshold": 10_000,
        "destructive_filter_threshold": 0.3,
        "raw_data": _build_fixture_rows(10),
        "transformed_data": [],
        "schema_sample": sample_schema,
        "validation_report": {},
        "validation_passed": False,
        "max_null_rate": 0.5,
        "strict_validation": False,
        "generated_code": "",
        "retry_count": 0,
        "last_error": None,
        "token_usage": {},
        "raw_table_name": None,
        "generated_sql": None,
        "run_id": "test-run-001",
        "rows_extracted": 0,
        "rows_loaded": 0,
        "duration_ms": {},
        "warnings": [],
        "is_streaming": False,
        "stream_iterator": None,
        "destructive_warnings": [],
        "auto_confirmed": False,
    }


@pytest.fixture()
def minimal_elt_state(sample_schema: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Fully populated PipelineState for ELT mode with postgres target."""
    return {
        "source_config": {
            "type": "postgres",
            "url": "postgresql://loafer:loafer@localhost:5432/loafer_dev",
            "query": "SELECT * FROM users",
            "timeout": 30,
        },
        "target_config": {
            "type": "postgres",
            "url": "postgresql://loafer:loafer@localhost:5432/loafer_dev",
            "table": "users_transformed",
            "write_mode": "append",
        },
        "transform_config": {
            "type": "ai",
            "instruction": "lowercase all name values",
        },
        "llm_config": {
            "provider": "gemini",
            "model": "gemini-1.5-flash",
            "api_key": "test-key",
        },
        "transform_instruction": "lowercase all name values",
        "mode": "elt",
        "chunk_size": 500,
        "streaming_threshold": 10_000,
        "destructive_filter_threshold": 0.3,
        "raw_data": _build_fixture_rows(10),
        "transformed_data": [],
        "schema_sample": sample_schema,
        "validation_report": {},
        "validation_passed": False,
        "max_null_rate": 0.5,
        "strict_validation": False,
        "generated_code": "",
        "retry_count": 0,
        "last_error": None,
        "token_usage": {},
        "raw_table_name": "users_raw",
        "generated_sql": None,
        "run_id": "test-run-002",
        "rows_extracted": 0,
        "rows_loaded": 0,
        "duration_ms": {},
        "warnings": [],
        "is_streaming": False,
        "stream_iterator": None,
        "destructive_warnings": [],
        "auto_confirmed": False,
    }


@pytest.fixture()
def mock_llm_provider() -> MagicMock:
    """Mock LLMProvider that returns a hardcoded valid transform function."""
    provider = MagicMock()
    provider.generate_transform_function.return_value = MagicMock(
        code='def transform(data: list[dict]) -> list[dict]:\n    return [{k: v.lower() if isinstance(v, str) else v for k, v in row.items()} for row in data]\n',
        raw_response="def transform ...",
        token_usage={"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
    )
    provider.generate_elt_sql.return_value = MagicMock(
        sql="SELECT id, LOWER(name) AS name, email, score, created_at FROM users_raw",
        raw_response="SELECT ...",
        token_usage={"prompt_tokens": 80, "completion_tokens": 30, "total_tokens": 110},
    )
    return provider
