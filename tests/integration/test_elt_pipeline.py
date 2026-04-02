"""ELT pipeline integration tests.

Tests the ELT graph structure. Full ELT with real databases
requires running services; these tests verify graph wiring and
file-based connectors.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

from loafer.config import (
    CsvSourceConfig,
    CsvTargetConfig,
    JsonTargetConfig,
)
from loafer.graph.elt import build_elt_graph


def _make_elt_state(
    tmp_path: Path,
    raw_data: list[dict[str, Any]] | None = None,
    chunk_size: int = 10,
    streaming_threshold: int = 1000,
) -> dict[str, Any]:
    """Build a minimal ELT pipeline state for testing."""
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

    state: dict[str, Any] = {
        "source_config": CsvSourceConfig(type="csv", path=str(csv_path)),
        "target_config": JsonTargetConfig(
            type="json", path=str(output_path), write_mode="overwrite"
        ),
        "transform_config": {"type": "ai", "instruction": "lowercase all string values"},
        "llm_config": {
            "provider": "gemini",
            "model": "gemini-1.5-flash",
            "api_key": "test-key",
        },
        "transform_instruction": "lowercase all string values",
        "mode": "elt",
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
        },
        "validation_report": {},
        "validation_passed": True,
        "max_null_rate": 0.5,
        "strict_validation": False,
        "generated_code": "",
        "retry_count": 0,
        "transform_retry_count": 0,
        "last_error": None,
        "token_usage": {},
        "raw_table_name": "raw_users",
        "generated_sql": None,
        "run_id": "test-elt-run",
        "rows_extracted": len(raw_data),
        "rows_loaded": 0,
        "duration_ms": {},
        "warnings": [],
        "is_streaming": False,
        "stream_iterator": None,
        "destructive_warnings": [],
        "auto_confirmed": True,
    }
    return state


def _mock_load_raw(state: dict) -> dict:
    """Mock load_raw that writes raw_data to the target connector."""
    import time

    start = time.monotonic()
    target_config = state["target_config"]
    raw_data = state.get("raw_data", [])

    if target_config.type == "json":
        with open(target_config.path, "w") as f:
            json.dump(raw_data, f, indent=2)
    elif target_config.type == "csv" and raw_data:
        with open(target_config.path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(raw_data[0].keys()))
            writer.writeheader()
            writer.writerows(raw_data)

    state["raw_table_name"] = "mock_staging_table"
    state["rows_loaded"] = len(raw_data)
    state["duration_ms"]["load_raw"] = (time.monotonic() - start) * 1000
    return state


def _mock_transform_in_target(state: dict) -> dict:
    """Mock transform_in_target that passes through."""
    import time

    start = time.monotonic()
    state["duration_ms"]["transform_in_target"] = (time.monotonic() - start) * 1000
    return state


class TestEltPipeline:
    """Full ELT pipeline integration tests."""

    def test_elt_extract_and_load_raw(self, tmp_path: Path) -> None:
        """ELT: extract → load_raw completes successfully."""
        state = _make_elt_state(tmp_path)

        with (
            patch("loafer.graph.elt.load_raw_agent", _mock_load_raw),
            patch("loafer.graph.elt.transform_in_target_agent", _mock_transform_in_target),
        ):
            graph = build_elt_graph()
            result = graph.invoke(state)

        assert result["rows_extracted"] == 10
        assert result["rows_loaded"] == 10

        output_path = tmp_path / "output.json"
        with open(output_path) as f:
            data = json.load(f)
        assert len(data) == 10

    def test_elt_large_dataset(self, tmp_path: Path) -> None:
        """ELT with large dataset processes all rows."""
        raw_data = [
            {"id": i, "name": f"User{i}", "email": f"user{i}@TEST.COM", "score": float(i * 10)}
            for i in range(500)
        ]

        csv_path = tmp_path / "source.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "email", "score"])
            for row in raw_data:
                writer.writerow([row["id"], row["name"], row["email"], row["score"]])

        output_path = tmp_path / "output.json"

        state: dict[str, Any] = {
            "source_config": CsvSourceConfig(type="csv", path=str(csv_path)),
            "target_config": JsonTargetConfig(
                type="json", path=str(output_path), write_mode="overwrite"
            ),
            "transform_config": {"type": "ai", "instruction": "lowercase names"},
            "llm_config": {
                "provider": "gemini",
                "model": "gemini-1.5-flash",
                "api_key": "test-key",
            },
            "transform_instruction": "lowercase names",
            "mode": "elt",
            "chunk_size": 50,
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
            "raw_table_name": "raw_data",
            "generated_sql": None,
            "run_id": "test-elt-large",
            "rows_extracted": 500,
            "rows_loaded": 0,
            "duration_ms": {},
            "warnings": [],
            "is_streaming": False,
            "stream_iterator": None,
            "destructive_warnings": [],
            "auto_confirmed": True,
        }

        with (
            patch("loafer.graph.elt.load_raw_agent", _mock_load_raw),
            patch("loafer.graph.elt.transform_in_target_agent", _mock_transform_in_target),
        ):
            graph = build_elt_graph()
            result = graph.invoke(state)

        assert result["rows_extracted"] == 500
        assert result["rows_loaded"] == 500

        with open(output_path) as f:
            data = json.load(f)
        assert len(data) == 500

    def test_elt_csv_to_csv(self, tmp_path: Path) -> None:
        """Full ELT: CSV source → load_raw → CSV target."""
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
            "transform_config": {"type": "ai", "instruction": "lowercase names"},
            "llm_config": {
                "provider": "gemini",
                "model": "gemini-1.5-flash",
                "api_key": "test-key",
            },
            "transform_instruction": "lowercase names",
            "mode": "elt",
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
            "raw_table_name": "raw_data",
            "generated_sql": None,
            "run_id": "test-elt-csv",
            "rows_extracted": 2,
            "rows_loaded": 0,
            "duration_ms": {},
            "warnings": [],
            "is_streaming": False,
            "stream_iterator": None,
            "destructive_warnings": [],
            "auto_confirmed": True,
        }

        with (
            patch("loafer.graph.elt.load_raw_agent", _mock_load_raw),
            patch("loafer.graph.elt.transform_in_target_agent", _mock_transform_in_target),
        ):
            graph = build_elt_graph()
            result = graph.invoke(state)

        assert result["rows_loaded"] == 2

        with open(output_path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["name"] == "ALICE"
        assert rows[1]["name"] == "BOB"

    def test_elt_timing_recorded(self, tmp_path: Path) -> None:
        """Each agent records timing in duration_ms."""
        state = _make_elt_state(tmp_path)

        with (
            patch("loafer.graph.elt.load_raw_agent", _mock_load_raw),
            patch("loafer.graph.elt.transform_in_target_agent", _mock_transform_in_target),
        ):
            graph = build_elt_graph()
            result = graph.invoke(state)

        assert "extract" in result["duration_ms"]
        assert "load_raw" in result["duration_ms"]
        assert "transform_in_target" in result["duration_ms"]
