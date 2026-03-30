"""Tests for loafer.llm.prompt_builder."""

from __future__ import annotations

from loafer.llm.prompt_builder import build_elt_sql_prompt, build_etl_transform_prompt


class TestBuildEtlTransformPrompt:
    """ETL transform prompt construction."""

    def test_contains_schema_sample(self, sample_schema: dict) -> None:
        prompt = build_etl_transform_prompt(sample_schema, "uppercase names")
        # The schema should be serialised in the prompt.
        for col in sample_schema:
            assert col in prompt

    def test_contains_instruction(self) -> None:
        prompt = build_etl_transform_prompt({}, "merge first and last name")
        assert "merge first and last name" in prompt

    def test_contains_transform_rules(self) -> None:
        prompt = build_etl_transform_prompt({}, "noop")
        assert "def transform" in prompt or "`transform`" in prompt

    def test_previous_error_included(self) -> None:
        prompt = build_etl_transform_prompt(
            {},
            "noop",
            previous_error="KeyError: 'name'",
            previous_code="def transform(data): return data",
        )
        assert "KeyError: 'name'" in prompt
        assert "def transform(data): return data" in prompt

    def test_no_previous_error_section_when_none(self) -> None:
        prompt = build_etl_transform_prompt({}, "noop")
        assert "Previous Attempt" not in prompt

    def test_prompt_length_is_sane(self, sample_schema: dict) -> None:
        prompt = build_etl_transform_prompt(sample_schema, "do something")
        # A reasonable upper bound — prompt should not be excessively long.
        assert len(prompt) < 10_000


class TestBuildEltSqlPrompt:
    """ELT SQL prompt construction."""

    def test_contains_raw_table_name(self) -> None:
        prompt = build_elt_sql_prompt({}, "users_raw", "lowercase names")
        assert "users_raw" in prompt

    def test_contains_target_schema(self, sample_schema: dict) -> None:
        prompt = build_elt_sql_prompt(sample_schema, "t", "noop")
        for col in sample_schema:
            assert col in prompt

    def test_contains_instruction(self) -> None:
        prompt = build_elt_sql_prompt({}, "t", "add computed column")
        assert "add computed column" in prompt

    def test_previous_error_included(self) -> None:
        prompt = build_elt_sql_prompt({}, "t", "noop", previous_error="column does not exist")
        assert "column does not exist" in prompt

    def test_no_previous_error_section_when_none(self) -> None:
        prompt = build_elt_sql_prompt({}, "t", "noop")
        assert "Previous Attempt" not in prompt

    def test_prompt_length_is_sane(self, sample_schema: dict) -> None:
        prompt = build_elt_sql_prompt(sample_schema, "raw", "do something")
        assert len(prompt) < 10_000
