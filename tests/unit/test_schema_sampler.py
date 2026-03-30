"""Tests for loafer.llm.schema — the token-efficient schema sampler."""

from __future__ import annotations

from loafer.llm.schema import build_schema_sample


class TestBuildSchemaSample:
    """build_schema_sample edge cases from the spec."""

    def test_empty_list_returns_empty_dict(self) -> None:
        assert build_schema_sample([]) == {}

    def test_1000_rows_exactly_5_sample_values(self) -> None:
        data = [{"x": i} for i in range(1000)]
        schema = build_schema_sample(data, max_sample_rows=5)
        assert len(schema["x"]["sample_values"]) == 5

    def test_all_null_column(self) -> None:
        data = [{"col": None} for _ in range(10)]
        schema = build_schema_sample(data)
        assert schema["col"]["inferred_type"] == "null"
        assert schema["col"]["nullable"] is True
        assert schema["col"]["null_count"] == 10

    def test_mixed_int_string_column(self) -> None:
        data = [{"val": 1}, {"val": "two"}, {"val": 3}]
        schema = build_schema_sample(data)
        assert schema["val"]["inferred_type"] == "mixed"

    def test_nested_dict_value(self) -> None:
        data = [{"meta": {"nested": True}}, {"meta": {"nested": False}}]
        schema = build_schema_sample(data)
        assert schema["meta"]["inferred_type"] == "object"

    def test_nested_list_value(self) -> None:
        data = [{"tags": ["a", "b"]}, {"tags": ["c"]}]
        schema = build_schema_sample(data)
        assert schema["tags"]["inferred_type"] == "array"

    def test_long_string_truncated(self) -> None:
        long_str = "x" * 500
        data = [{"bio": long_str}]
        schema = build_schema_sample(data, max_string_length=100)
        sample = schema["bio"]["sample_values"][0]
        assert len(sample) == 101  # 100 chars + "…"
        assert sample.endswith("…")

    def test_datetime_strings_detected(self) -> None:
        data = [
            {"ts": "2024-01-15T10:30:00"},
            {"ts": "2024-02-20T14:45:00Z"},
            {"ts": "2024-03-10"},
        ]
        schema = build_schema_sample(data)
        assert schema["ts"]["inferred_type"] == "datetime"

    def test_boolean_type(self) -> None:
        data = [{"active": True}, {"active": False}]
        schema = build_schema_sample(data)
        assert schema["active"]["inferred_type"] == "boolean"

    def test_float_type(self) -> None:
        data = [{"score": 3.14}, {"score": 2.71}]
        schema = build_schema_sample(data)
        assert schema["score"]["inferred_type"] == "float"

    def test_missing_keys_treated_as_null(self) -> None:
        data = [{"a": 1, "b": 2}, {"a": 3}]
        schema = build_schema_sample(data)
        assert schema["b"]["null_count"] == 1
        assert schema["b"]["nullable"] is True

    def test_total_count_matches(self) -> None:
        data = [{"x": i} for i in range(42)]
        schema = build_schema_sample(data)
        assert schema["x"]["total_count"] == 42

    def test_many_columns(self) -> None:
        """100+ columns are all processed."""
        row = {f"col_{i}": i for i in range(120)}
        data = [row]
        schema = build_schema_sample(data)
        assert len(schema) == 120
