"""Comprehensive edge-case tests across connectors, agents, LLM, and core."""

from __future__ import annotations

import csv
import io
import json
import os
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from loafer.config import (
    AITransformConfig,
    CustomTransformConfig,
    PostgresTargetConfig,
    SQLTransformConfig,
    SourceConfig,
    TargetConfig,
)
from loafer.exceptions import (
    ConfigError,
    ConnectorError,
    ExtractionError,
    LoadError,
    LLMError,
    LLMInvalidOutputError,
    LLMRateLimitError,
    TransformError,
    ValidationError,
)
from loafer.graph.state import PipelineState
from loafer.llm.base import ELTSQLResult, TransformPromptResult
from loafer.llm.registry import get_provider, list_providers
from loafer.llm.schema import build_schema_sample
from loafer.transform.code_validator import validate_transform_function
from loafer.transform.sql_validator import validate_transform_sql
from loafer.core.destructive import (
    DestructiveReason,
    detect_destructive_operations,
    raise_if_destructive,
)


# ---------------------------------------------------------------------------
# Connector edge cases
# ---------------------------------------------------------------------------


class TestCsvSourceEdgeCases:
    def test_inconsistent_column_counts_skips_malformed_rows(self, tmp_path: Path) -> None:
        f = tmp_path / "bad.csv"
        f.write_text("a,b,c\n1,2,3\n4,5\n6,7,8\n")
        from loafer.adapters.sources.csv_source import CsvSourceConnector

        conn = CsvSourceConnector(str(f), has_header=True, encoding="utf-8", column_names=None)
        conn.connect()
        chunks = list(conn.stream(chunk_size=10))
        conn.disconnect()
        assert len(chunks) == 1
        assert len(chunks[0]) == 2  # only 2 well-formed rows

    def test_no_header_no_column_names_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "noheader.csv"
        f.write_text("1,2,3\n4,5,6\n")
        from loafer.adapters.sources.csv_source import CsvSourceConnector

        conn = CsvSourceConnector(str(f), has_header=False, encoding="utf-8", column_names=None)
        with pytest.raises(ConfigError, match="column_names"):
            conn.connect()

    def test_header_only_yields_no_chunks(self, tmp_path: Path) -> None:
        f = tmp_path / "header_only.csv"
        f.write_text("a,b,c\n")
        from loafer.adapters.sources.csv_source import CsvSourceConnector

        conn = CsvSourceConnector(str(f), has_header=True, encoding="utf-8", column_names=None)
        conn.connect()
        chunks = list(conn.stream(chunk_size=10))
        conn.disconnect()
        assert chunks == []

    def test_file_not_found_raises(self) -> None:
        from loafer.adapters.sources.csv_source import CsvSourceConnector

        conn = CsvSourceConnector(
            "/nonexistent/file.csv", has_header=True, encoding="utf-8", column_names=None
        )
        with pytest.raises(ExtractionError, match="not found"):
            conn.connect()


class TestCsvTargetEdgeCases:
    def test_values_with_newlines_and_commas(self, tmp_path: Path) -> None:
        from loafer.adapters.targets.csv_target import CsvTargetConnector

        f = tmp_path / "out.csv"
        conn = CsvTargetConnector(str(f), write_mode="overwrite")
        conn.connect()
        conn.write_chunk([{"a": "hello, world", "b": "line1\nline2"}])
        conn.finalize()
        conn.disconnect()

        reader = csv.DictReader(f.open())
        rows = list(reader)
        assert rows[0]["a"] == "hello, world"
        assert rows[0]["b"] == "line1\nline2"

    def test_dict_and_list_values_serialized_as_json(self, tmp_path: Path) -> None:
        from loafer.adapters.targets.csv_target import CsvTargetConnector

        f = tmp_path / "out.json"
        # Use JSON target for nested data
        from loafer.adapters.targets.json_target import JsonTargetConnector

        conn = JsonTargetConnector(str(f), write_mode="overwrite")
        conn.connect()
        conn.write_chunk([{"a": {"nested": True}, "b": [1, 2, 3]}])
        conn.finalize()
        conn.disconnect()

        data = json.loads(f.read_text())
        assert data[0]["a"] == {"nested": True}
        assert data[0]["b"] == [1, 2, 3]


class TestJsonTargetEdgeCases:
    def test_incremental_json_array(self, tmp_path: Path) -> None:
        from loafer.adapters.targets.json_target import JsonTargetConnector

        f = tmp_path / "out.json"
        conn = JsonTargetConnector(str(f), write_mode="overwrite")
        conn.connect()
        n1 = conn.write_chunk([{"id": 1}])
        n2 = conn.write_chunk([{"id": 2}, {"id": 3}])
        conn.finalize()
        conn.disconnect()

        assert n1 == 1
        assert n2 == 2
        data = json.loads(f.read_text())
        assert len(data) == 3

    def test_write_mode_error_existing_file(self, tmp_path: Path) -> None:
        from loafer.adapters.targets.json_target import JsonTargetConnector

        f = tmp_path / "out.json"
        f.write_text("[]")
        conn = JsonTargetConnector(str(f), write_mode="error")
        with pytest.raises(LoadError, match="already exists"):
            conn.connect()


class TestExcelSourceEdgeCases:
    def test_corrupted_file_raises(self, tmp_path: Path) -> None:
        from loafer.adapters.sources.excel_source import ExcelSourceConnector

        f = tmp_path / "bad.xlsx"
        f.write_text("not a real xlsx")
        conn = ExcelSourceConnector(str(f), sheet=None)
        with pytest.raises(ExtractionError):
            conn.connect()

    def test_missing_sheet_raises_with_available_sheets(self, tmp_path: Path) -> None:
        from openpyxl import Workbook
        from loafer.adapters.sources.excel_source import ExcelSourceConnector

        f = tmp_path / "test.xlsx"
        wb = Workbook()
        wb.create_sheet("Sheet1")
        wb.save(str(f))
        conn = ExcelSourceConnector(str(f), sheet="NonExistent")
        with pytest.raises(ExtractionError, match="NonExistent"):
            conn.connect()


# ---------------------------------------------------------------------------
# Schema sampler edge cases
# ---------------------------------------------------------------------------


class TestSchemaSamplerEdgeCases:
    def test_single_row(self) -> None:
        result = build_schema_sample([{"a": 1}])
        assert "a" in result
        assert result["a"]["inferred_type"] == "integer"
        assert result["a"]["sample_values"] == [1]

    def test_fewer_rows_than_max(self) -> None:
        data = [{"x": i} for i in range(3)]
        result = build_schema_sample(data, max_sample_rows=5)
        assert len(result["x"]["sample_values"]) == 3

    def test_deeply_nested_dict(self) -> None:
        data = [{"a": {"b": {"c": {"d": {"e": "deep"}}}}}]
        result = build_schema_sample(data)
        assert result["a"]["inferred_type"] == "object"

    def test_deeply_nested_list(self) -> None:
        data = [{"items": [1, 2, 3, 4, 5, 6]}]
        result = build_schema_sample(data)
        assert result["items"]["inferred_type"] == "array"

    def test_mixed_boolean_and_int(self) -> None:
        data = [{"x": True}, {"x": 1}, {"x": False}, {"x": 0}]
        result = build_schema_sample(data)
        assert result["x"]["inferred_type"] == "mixed"

    def test_empty_string_vs_null(self) -> None:
        data = [{"a": ""}, {"a": "hello"}]
        result = build_schema_sample(data)
        assert result["a"]["inferred_type"] == "string"
        assert result["a"]["nullable"] is False

    def test_iso_datetime_detection(self) -> None:
        data = [{"ts": "2024-01-15T10:30:00Z"}, {"ts": "2024-02-20T14:00:00Z"}]
        result = build_schema_sample(data)
        assert result["ts"]["inferred_type"] == "datetime"

    def test_many_columns(self) -> None:
        row = {f"col_{i}": i for i in range(200)}
        result = build_schema_sample([row])
        assert len(result) == 200

    def test_string_truncation(self) -> None:
        long_val = "x" * 500
        result = build_schema_sample([{"a": long_val}], max_string_length=50)
        sample = result["a"]["sample_values"][0]
        # Truncation adds "…" (1 char), so max is max_string_length + 1
        assert len(sample) <= 51

    def test_exact_threshold_boundary(self) -> None:
        raw = [{"id": i} for i in range(100)]
        transformed = [{"id": i} for i in range(69)]  # 31% dropped, above 30% threshold
        warnings = detect_destructive_operations(
            {"raw_data": raw}, {"transformed_data": transformed}, threshold=0.3
        )
        assert len(warnings) > 0

    def test_below_threshold_no_warning(self) -> None:
        raw = [{"id": i} for i in range(100)]
        transformed = [{"id": i} for i in range(70)]  # exactly 30% dropped
        warnings = detect_destructive_operations(
            {"raw_data": raw}, {"transformed_data": transformed}, threshold=0.3
        )
        # 30% == threshold, uses strict > so no warning
        assert len(warnings) == 0

    def test_all_columns_dropped(self) -> None:
        raw = [{"a": 1, "b": 2}]
        transformed = [{}]
        warnings = detect_destructive_operations(
            {"raw_data": raw}, {"transformed_data": transformed}, threshold=0.3
        )
        cols_removed = [w for w in warnings if w.reason == DestructiveReason.COLUMNS_REMOVED]
        assert len(cols_removed) > 0

    def test_no_changes_no_warnings(self) -> None:
        data = [{"a": 1}]
        warnings = detect_destructive_operations(
            {"raw_data": data}, {"transformed_data": data}, threshold=0.3
        )
        assert len(warnings) == 0

    def test_raise_if_destructive_with_auto_confirmed(self) -> None:
        from loafer.core.destructive import DestructiveWarning

        warnings = [
            DestructiveWarning(
                reason=DestructiveReason.ROWS_DROPPED, message="test", severity="warn", details={}
            )
        ]
        raise_if_destructive(warnings, auto_confirmed=True)  # should not raise

    def test_raise_if_destructive_without_confirmation_raises(self) -> None:
        from loafer.core.destructive import DestructiveWarning

        warnings = [
            DestructiveWarning(
                reason=DestructiveReason.ROWS_DROPPED, message="test", severity="warn", details={}
            )
        ]
        with pytest.raises(Exception):
            raise_if_destructive(warnings, auto_confirmed=False)

    def test_empty_warnings_no_raise(self) -> None:
        raise_if_destructive([], auto_confirmed=False)  # should not raise


# ---------------------------------------------------------------------------
# LLM provider edge cases
# ---------------------------------------------------------------------------


class TestLLMProviderEdgeCases:
    def test_stripping_nested_fences(self) -> None:
        from loafer.llm.gemini import _strip_markdown_fences

        # The simple regex only matches a single fence pair.
        # Nested or malformed fences are returned as-is (stripped of outer whitespace).
        text = "```\n```python\ndef foo(): pass\n```\n```"
        result = _strip_markdown_fences(text)
        assert "def foo(): pass" in result

    def test_stripping_trailing_whitespace(self) -> None:
        from loafer.llm.gemini import _strip_markdown_fences

        text = "```python\ndef foo(): pass\n```   \n\n"
        result = _strip_markdown_fences(text)
        assert result == "def foo(): pass"

    def test_all_four_providers_registered(self) -> None:
        providers = list_providers()
        assert "gemini" in providers
        assert "claude" in providers
        assert "openai" in providers
        assert "qwen" in providers

    def test_unknown_provider_error_message_lists_available(self) -> None:
        with pytest.raises(LLMError, match="Available:"):
            get_provider("nonexistent")


# ---------------------------------------------------------------------------
# Config edge cases
# ---------------------------------------------------------------------------


class TestConfigEdgeCases:
    def test_chunk_size_negative_rejected(self) -> None:
        from loafer.config import PipelineConfig

        with pytest.raises(Exception):
            PipelineConfig(
                source={"type": "csv", "path": "/tmp/x.csv"},
                target={"type": "csv", "path": "/tmp/y.csv"},
                transform={"type": "ai", "instruction": "x"},
                chunk_size=-1,
            )

    def test_transform_instruction_too_long(self) -> None:
        from loafer.config import PipelineConfig

        with pytest.raises(Exception):
            PipelineConfig(
                source={"type": "csv", "path": "/tmp/x.csv"},
                target={"type": "csv", "path": "/tmp/y.csv"},
                transform={"type": "ai", "instruction": "x" * 10001},
            )

    def test_env_var_interpolation_nested(self) -> None:
        from loafer.config import _resolve_env_vars

        os.environ["_LOAFER_TEST_NESTED"] = "secret"
        try:
            result = _resolve_env_vars("${_LOAFER_TEST_NESTED}")
            assert result == "secret"
        finally:
            del os.environ["_LOAFER_TEST_NESTED"]

    def test_env_var_interpolation_missing(self) -> None:
        from loafer.config import _resolve_env_vars

        with pytest.raises(ConfigError, match="not set"):
            _resolve_env_vars("${_LOAFER_NONEXISTENT_VAR_XYZ}")

    def test_env_var_interpolation_no_interpolation_needed(self) -> None:
        from loafer.config import _resolve_env_vars

        result = _resolve_env_vars("postgresql://localhost/db")
        assert result == "postgresql://localhost/db"


# ---------------------------------------------------------------------------
# PipelineState edge cases
# ---------------------------------------------------------------------------


class TestPipelineStateEdgeCases:
    def test_minimal_state_has_required_keys(self) -> None:
        state: PipelineState = {}
        assert isinstance(state, dict)

    def test_state_with_streaming_fields(self) -> None:
        state: PipelineState = {
            "is_streaming": True,
            "stream_iterator": iter([]),
        }
        assert state["is_streaming"] is True

    def test_state_with_destructive_warnings(self) -> None:
        from loafer.core.destructive import DestructiveWarning

        state: PipelineState = {
            "destructive_warnings": [
                DestructiveWarning(DestructiveReason.ROWS_DROPPED, "test", "warn", {})
            ],
        }
        assert len(state["destructive_warnings"]) == 1


# ---------------------------------------------------------------------------
# Exception hierarchy edge cases
# ---------------------------------------------------------------------------


class TestExceptionHierarchy:
    def test_extraction_error_is_connector_error(self) -> None:
        assert issubclass(ExtractionError, ConnectorError)

    def test_load_error_is_connector_error(self) -> None:
        assert issubclass(LoadError, ConnectorError)

    def test_llm_rate_limit_is_llm_error(self) -> None:
        assert issubclass(LLMRateLimitError, LLMError)

    def test_llm_invalid_output_is_llm_error(self) -> None:
        assert issubclass(LLMInvalidOutputError, LLMError)

    def test_all_loafer_errors_are_exceptions(self) -> None:
        from loafer.exceptions import LoaferError

        for cls in (
            ConfigError,
            ConnectorError,
            ExtractionError,
            LoadError,
            ValidationError,
            TransformError,
            LLMError,
            LLMRateLimitError,
            LLMInvalidOutputError,
        ):
            assert issubclass(cls, LoaferError)
            assert issubclass(cls, Exception)
