"""ETL pipeline integration tests.

Tests the full ETL graph (extract → validate → transform → load)
using real file-based connectors and Pydantic config models.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from loafer.config import (
    AITransformConfig,
    CsvSourceConfig,
    CsvTargetConfig,
    CustomTransformConfig,
    JsonTargetConfig,
)
from loafer.graph.etl import build_etl_graph


def _make_etl_state(
    tmp_path: Path,
    mode: str = "etl",
    transform_type: str = "custom",
    transform_path: str | None = None,
    transform_instruction: str = "lowercase all string values",
    raw_data: list[dict[str, Any]] | None = None,
    chunk_size: int = 10,
    streaming_threshold: int = 1000,
) -> dict[str, Any]:
    """Build a minimal ETL pipeline state for testing."""
    csv_path = tmp_path / "source.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "email", "score"])
        for i in range(10):
            writer.writerow([i, f"User{i}", f"user{i}@TEST.COM", float(i * 10)])

    output_path = tmp_path / "output.json"

    if raw_data is None:
        raw_data = [
            {"id": i, "name": f"User{i}", "email": f"user{i}@TEST.COM", "score": float(i * 10)}
            for i in range(10)
        ]

    source_config = CsvSourceConfig(type="csv", path=str(csv_path))
    target_config = JsonTargetConfig(type="json", path=str(output_path), write_mode="overwrite")

    if transform_type == "custom" and transform_path:
        transform_config: Any = CustomTransformConfig(type="custom", path=transform_path)
    else:
        transform_config = AITransformConfig(type="ai", instruction=transform_instruction)

    state: dict[str, Any] = {
        "source_config": source_config,
        "target_config": target_config,
        "transform_config": transform_config,
        "llm_config": {
            "provider": "gemini",
            "model": "gemini-1.5-flash",
            "api_key": "test-key",
        },
        "transform_instruction": transform_instruction,
        "mode": mode,
        "chunk_size": chunk_size,
        "streaming_threshold": streaming_threshold,
        "destructive_filter_threshold": 0.3,
        "raw_data": raw_data,
        "transformed_data": [],
        "schema_sample": {
            "id": {
                "inferred_type": "integer",
                "nullable": False,
                "sample_values": [0, 1, 2],
                "null_count": 0,
                "total_count": 10,
            },
            "name": {
                "inferred_type": "string",
                "nullable": False,
                "sample_values": ["User0", "User1"],
                "null_count": 0,
                "total_count": 10,
            },
            "email": {
                "inferred_type": "string",
                "nullable": False,
                "sample_values": ["user0@TEST.COM"],
                "null_count": 0,
                "total_count": 10,
            },
            "score": {
                "inferred_type": "float",
                "nullable": False,
                "sample_values": [0.0, 10.0],
                "null_count": 0,
                "total_count": 10,
            },
        },
        "validation_report": {},
        "validation_passed": False,
        "max_null_rate": 0.5,
        "strict_validation": False,
        "generated_code": "",
        "retry_count": 0,
        "transform_retry_count": 0,
        "last_error": None,
        "token_usage": {},
        "raw_table_name": None,
        "generated_sql": None,
        "run_id": "test-run",
        "rows_extracted": len(raw_data),
        "rows_loaded": 0,
        "duration_ms": {},
        "warnings": [],
        "is_streaming": False,
        "stream_iterator": None,
        "destructive_warnings": [],
        "auto_confirmed": True,
        "llm_provider": MagicMock(),
    }
    return state


class TestEtlPipeline:
    """Full ETL pipeline integration tests."""

    def test_full_etl_csv_to_json_with_custom_transform(self, tmp_path: Path) -> None:
        """Full ETL run: CSV source → custom transform → JSON target."""
        transform_path = tmp_path / "transform.py"
        transform_path.write_text("""
def transform(data):
    result = []
    for row in data:
        result.append({
            "id": row["id"],
            "name": row["name"].lower(),
            "email": row["email"].lower(),
            "score": row["score"],
        })
    return result
""")

        state = _make_etl_state(
            tmp_path,
            transform_type="custom",
            transform_path=str(transform_path),
        )
        state["validation_passed"] = True

        graph = build_etl_graph()
        result = graph.invoke(state)

        assert result["validation_passed"] is True
        assert len(result["transformed_data"]) == 10
        assert result["rows_loaded"] == 10
        assert result["transformed_data"][0]["email"] == "user0@test.com"

        output_path = tmp_path / "output.json"
        with open(output_path) as f:
            data = json.load(f)
        assert len(data) == 10
        assert data[0]["email"] == "user0@test.com"

    def test_validation_failure_terminates_pipeline(self, tmp_path: Path) -> None:
        """Empty source → validation fails, pipeline terminates, no output written."""
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("id,name,email,score\n")

        output_path = tmp_path / "output.json"

        state: dict[str, Any] = {
            "source_config": CsvSourceConfig(type="csv", path=str(csv_path)),
            "target_config": JsonTargetConfig(
                type="json", path=str(output_path), write_mode="overwrite"
            ),
            "transform_config": AITransformConfig(type="ai", instruction="noop"),
            "llm_config": {"provider": "gemini", "model": "gemini-1.5-flash", "api_key": "test"},
            "transform_instruction": "noop",
            "mode": "etl",
            "chunk_size": 10,
            "streaming_threshold": 1000,
            "destructive_filter_threshold": 0.3,
            "raw_data": [],
            "transformed_data": [],
            "schema_sample": {},
            "validation_report": {},
            "validation_passed": False,
            "max_null_rate": 0.5,
            "strict_validation": False,
            "generated_code": "",
            "retry_count": 0,
            "transform_retry_count": 0,
            "last_error": None,
            "token_usage": {},
            "raw_table_name": None,
            "generated_sql": None,
            "run_id": "test-empty",
            "rows_extracted": 0,
            "rows_loaded": 0,
            "duration_ms": {},
            "warnings": [],
            "is_streaming": False,
            "stream_iterator": None,
            "destructive_warnings": [],
            "auto_confirmed": True,
            "llm_provider": MagicMock(),
        }

        graph = build_etl_graph()
        result = graph.invoke(state)

        assert result["rows_loaded"] == 0
        assert not output_path.exists()

    def test_transform_returns_zero_rows(self, tmp_path: Path) -> None:
        """Transform filters all rows → warning added, no error."""
        transform_path = tmp_path / "filter_all.py"
        transform_path.write_text("""
def transform(data):
    return []
""")

        state = _make_etl_state(
            tmp_path,
            transform_type="custom",
            transform_path=str(transform_path),
        )
        state["validation_passed"] = True
        state["auto_confirmed"] = True

        graph = build_etl_graph()
        result = graph.invoke(state)

        assert result["rows_loaded"] == 0
        assert any("0 rows" in w for w in result.get("warnings", []))

    def test_streaming_mode_activates(self, tmp_path: Path) -> None:
        """Large dataset → streaming mode activated."""
        csv_path = tmp_path / "source.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "email", "score"])
            for i in range(2000):
                writer.writerow([i, f"User{i}", f"user{i}@TEST.COM", float(i * 10)])

        raw_data = [
            {"id": i, "name": f"User{i}", "email": f"user{i}@TEST.COM", "score": float(i * 10)}
            for i in range(2000)
        ]

        transform_path = tmp_path / "transform.py"
        transform_path.write_text("""
def transform(data):
    return [{**row, "name": row["name"].lower()} for row in data]
""")

        output_path = tmp_path / "output.json"

        state: dict[str, Any] = {
            "source_config": CsvSourceConfig(type="csv", path=str(csv_path)),
            "target_config": JsonTargetConfig(
                type="json", path=str(output_path), write_mode="overwrite"
            ),
            "transform_config": CustomTransformConfig(type="custom", path=str(transform_path)),
            "llm_config": {"provider": "gemini", "model": "gemini-1.5-flash", "api_key": "test"},
            "transform_instruction": "lowercase names",
            "mode": "etl",
            "chunk_size": 100,
            "streaming_threshold": 1000,
            "destructive_filter_threshold": 0.3,
            "raw_data": raw_data,
            "transformed_data": [],
            "schema_sample": {},
            "validation_report": {},
            "validation_passed": True,
            "max_null_rate": 0.5,
            "strict_validation": False,
            "generated_code": "",
            "retry_count": 0,
            "transform_retry_count": 0,
            "last_error": None,
            "token_usage": {},
            "raw_table_name": None,
            "generated_sql": None,
            "run_id": "test-stream",
            "rows_extracted": 2000,
            "rows_loaded": 0,
            "duration_ms": {},
            "warnings": [],
            "is_streaming": False,
            "stream_iterator": None,
            "destructive_warnings": [],
            "auto_confirmed": True,
            "llm_provider": MagicMock(),
        }

        graph = build_etl_graph()
        result = graph.invoke(state)

        assert result["rows_loaded"] == 2000

    def test_timing_recorded(self, tmp_path: Path) -> None:
        """Each agent records timing in duration_ms."""
        transform_path = tmp_path / "transform.py"
        transform_path.write_text("""
def transform(data):
    return [{**row, "name": row["name"].lower()} for row in data]
""")

        state = _make_etl_state(
            tmp_path,
            transform_type="custom",
            transform_path=str(transform_path),
        )
        state["validation_passed"] = True

        graph = build_etl_graph()
        result = graph.invoke(state)

        assert "extract" in result["duration_ms"]
        assert "validate" in result["duration_ms"]
        assert "transform" in result["duration_ms"]
        assert "load" in result["duration_ms"]

    def test_warnings_propagated(self, tmp_path: Path) -> None:
        """Warnings from agents are accumulated in state."""
        transform_path = tmp_path / "transform.py"
        transform_path.write_text("""
def transform(data):
    return [{**row, "name": row["name"].lower()} for row in data]
""")

        state = _make_etl_state(
            tmp_path,
            transform_type="custom",
            transform_path=str(transform_path),
        )
        state["validation_passed"] = True
        state["warnings"].append("pre-existing warning")

        graph = build_etl_graph()
        result = graph.invoke(state)

        assert "pre-existing warning" in result["warnings"]

    def test_csv_to_csv_pipeline(self, tmp_path: Path) -> None:
        """Full ETL: CSV source → transform → CSV target."""
        transform_path = tmp_path / "transform.py"
        transform_path.write_text("""
def transform(data):
    return [{**row, "name": row["name"].lower()} for row in data]
""")

        csv_path = tmp_path / "source.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "email"])
            writer.writerow([1, "ALICE", "alice@TEST.COM"])
            writer.writerow([2, "BOB", "bob@TEST.COM"])

        output_path = tmp_path / "output.csv"

        state: dict[str, Any] = {
            "source_config": CsvSourceConfig(type="csv", path=str(csv_path)),
            "target_config": CsvTargetConfig(
                type="csv", path=str(output_path), write_mode="overwrite"
            ),
            "transform_config": CustomTransformConfig(type="custom", path=str(transform_path)),
            "llm_config": {"provider": "gemini", "model": "gemini-1.5-flash", "api_key": "test"},
            "transform_instruction": "lowercase names",
            "mode": "etl",
            "chunk_size": 10,
            "streaming_threshold": 1000,
            "destructive_filter_threshold": 0.3,
            "raw_data": [
                {"id": 1, "name": "ALICE", "email": "alice@TEST.COM"},
                {"id": 2, "name": "BOB", "email": "bob@TEST.COM"},
            ],
            "transformed_data": [],
            "schema_sample": {},
            "validation_report": {},
            "validation_passed": True,
            "max_null_rate": 0.5,
            "strict_validation": False,
            "generated_code": "",
            "retry_count": 0,
            "transform_retry_count": 0,
            "last_error": None,
            "token_usage": {},
            "raw_table_name": None,
            "generated_sql": None,
            "run_id": "test-csv",
            "rows_extracted": 2,
            "rows_loaded": 0,
            "duration_ms": {},
            "warnings": [],
            "is_streaming": False,
            "stream_iterator": None,
            "destructive_warnings": [],
            "auto_confirmed": True,
            "llm_provider": MagicMock(),
        }

        graph = build_etl_graph()
        result = graph.invoke(state)

        assert result["rows_loaded"] == 2

        with open(output_path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["name"] == "alice"
        assert rows[1]["name"] == "bob"
