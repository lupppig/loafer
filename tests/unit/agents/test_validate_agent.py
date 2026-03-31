"""Tests for Validate Agent."""

from __future__ import annotations

from typing import Any

from loafer.agents.validate import validate_agent


class TestValidateAgent:
    def test_valid_data_passes(self) -> None:
        state: dict[str, Any] = {
            "raw_data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            "schema_sample": {
                "id": {
                    "inferred_type": "integer",
                    "nullable": False,
                    "sample_values": [1, 2],
                    "null_count": 0,
                    "total_count": 2,
                },
                "name": {
                    "inferred_type": "string",
                    "nullable": True,
                    "sample_values": ["Alice", "Bob"],
                    "null_count": 0,
                    "total_count": 2,
                },
            },
            "is_streaming": False,
            "strict_validation": False,
            "max_null_rate": 0.5,
            "duration_ms": {},
            "warnings": [],
        }
        result = validate_agent(state)

        assert result["validation_passed"] is True
        assert "validate" in result["duration_ms"]

    def test_empty_data_fails(self) -> None:
        state: dict[str, Any] = {
            "raw_data": [],
            "schema_sample": {},
            "is_streaming": False,
            "strict_validation": False,
            "max_null_rate": 0.5,
            "duration_ms": {},
            "warnings": [],
        }
        result = validate_agent(state)

        assert result["validation_passed"] is False
        assert "hard_failures" in result["validation_report"]

    def test_high_null_rate_strict_fails(self) -> None:
        state: dict[str, Any] = {
            "raw_data": [{"id": 1, "name": None}, {"id": 2, "name": None}],
            "schema_sample": {
                "name": {
                    "inferred_type": "string",
                    "nullable": True,
                    "sample_values": [],
                    "null_count": 2,
                    "total_count": 2,
                },
            },
            "is_streaming": False,
            "strict_validation": True,
            "max_null_rate": 0.5,
            "duration_ms": {},
            "warnings": [],
        }
        result = validate_agent(state)

        assert result["validation_passed"] is False
        assert any("null rate" in f for f in result["validation_report"]["hard_failures"])

    def test_high_null_rate_non_strict_warns(self) -> None:
        state: dict[str, Any] = {
            "raw_data": [{"id": 1, "name": None}, {"id": 2, "name": None}],
            "schema_sample": {
                "name": {
                    "inferred_type": "string",
                    "nullable": True,
                    "sample_values": [],
                    "null_count": 2,
                    "total_count": 2,
                },
            },
            "is_streaming": False,
            "strict_validation": False,
            "max_null_rate": 0.5,
            "duration_ms": {},
            "warnings": [],
        }
        result = validate_agent(state)

        assert result["validation_passed"] is True
        assert any("null rate" in w for w in result["warnings"])

    def test_mixed_type_warning(self) -> None:
        state: dict[str, Any] = {
            "raw_data": [{"val": 1}, {"val": "two"}],
            "schema_sample": {
                "val": {
                    "inferred_type": "mixed",
                    "nullable": True,
                    "sample_values": [1, "two"],
                    "null_count": 0,
                    "total_count": 2,
                },
            },
            "is_streaming": False,
            "strict_validation": False,
            "max_null_rate": 0.5,
            "duration_ms": {},
            "warnings": [],
        }
        result = validate_agent(state)

        assert result["validation_passed"] is True
        assert any("mixed" in w for w in result["warnings"])

    def test_empty_schema_sample_fails(self) -> None:
        state: dict[str, Any] = {
            "raw_data": [{"id": 1}],
            "schema_sample": {},
            "is_streaming": False,
            "strict_validation": False,
            "max_null_rate": 0.5,
            "duration_ms": {},
            "warnings": [],
        }
        result = validate_agent(state)

        assert result["validation_passed"] is False
