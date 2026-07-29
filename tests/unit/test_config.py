"""Tests for config parsing and validation."""

from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from loafer.config import LLMConfig, PipelineConfig, load_config
from loafer.exceptions import ConfigError


def _write_yaml(tmp_path: Path, content: str) -> Path:
    """Write a YAML string to a temp file and return the path."""
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(textwrap.dedent(content), encoding="utf-8")
    return config_path


class TestValidConfigParsing:
    def test_postgres_source_csv_target(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: postgres
              url: postgresql://user:pass@localhost:5432/db
              query: SELECT * FROM orders
            target:
              type: csv
              path: /tmp/output.csv
            transform:
              type: ai
              instruction: lowercase all names
            """,
        )
        config = load_config(path)
        assert config.source.type == "postgres"
        assert config.target.type == "csv"
        assert config.mode == "etl"

    def test_mysql_source(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: mysql
              url: mysql://user:pass@localhost:3306/db
              query: SELECT * FROM orders
            target:
              type: json
              path: /tmp/output.json
            transform:
              type: ai
              instruction: filter inactive users
            """,
        )
        config = load_config(path)
        assert config.source.type == "mysql"

    def test_mongo_source(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: mongo
              url: mongodb://localhost:27017
              database: mydb
              collection: users
            target:
              type: json
              path: /tmp/output.json
            transform:
              type: ai
              instruction: flatten nested addresses
            """,
        )
        config = load_config(path)
        assert config.source.type == "mongo"

    def test_rest_api_source(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: rest_api
              url: https://api.example.com/users
              method: GET
              headers:
                Authorization: Bearer token123
              response_key: data
            target:
              type: json
              path: /tmp/output.json
            transform:
              type: ai
              instruction: extract names only
            """,
        )
        config = load_config(path)
        assert config.source.type == "rest_api"

    def test_postgres_target(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: mongo
              url: mongodb://localhost:27017
              database: mydb
              collection: users
            target:
              type: postgres
              url: postgresql://user:pass@localhost:5432/db
              table: users_clean
              write_mode: replace
            transform:
              type: ai
              instruction: clean the data
            mode: elt
            """,
        )
        config = load_config(path)
        assert config.target.type == "postgres"
        assert config.mode == "elt"

    def test_sql_transform(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: mongo
              url: mongodb://localhost:27017
              database: mydb
              collection: orders
            target:
              type: json
              path: /tmp/output.json
            transform:
              type: sql
              query: "SELECT id, LOWER(name) AS name FROM {{source}}"
            """,
        )
        config = load_config(path)
        assert config.transform.type == "sql"

    def test_custom_transform(self, tmp_path: Path) -> None:
        transform_file = tmp_path / "my_transform.py"
        transform_file.write_text("def transform(data): return data\n")
        path = _write_yaml(
            tmp_path,
            f"""
            source:
              type: mongo
              url: mongodb://localhost:27017
              database: mydb
              collection: orders
            target:
              type: json
              path: /tmp/output.json
            transform:
              type: custom
              path: "{transform_file}"
            """,
        )
        config = load_config(path)
        assert config.transform.type == "custom"


class TestPlainStringTransformCoercion:
    def test_plain_string_becomes_ai_transform(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: mongo
              url: mongodb://localhost:27017
              database: mydb
              collection: users
            target:
              type: json
              path: /tmp/output.json
            transform: lowercase all names and filter inactive
            """,
        )
        config = load_config(path)
        assert config.transform.type == "ai"
        assert config.transform.instruction == "lowercase all names and filter inactive"


class TestDefaultValues:
    def test_default_mode_is_etl(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: mongo
              url: mongodb://localhost:27017
              database: mydb
              collection: users
            target:
              type: json
              path: /tmp/output.json
            transform:
              type: ai
              instruction: do stuff
            """,
        )
        config = load_config(path)
        assert config.mode == "etl"
        assert config.chunk_size == 500

    def test_default_validation_config(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: mongo
              url: mongodb://localhost:27017
              database: mydb
              collection: users
            target:
              type: json
              path: /tmp/output.json
            transform:
              type: ai
              instruction: do stuff
            """,
        )
        config = load_config(path)
        assert config.validation.max_null_rate == 0.5
        assert config.validation.strict is False


class TestConfigErrors:
    def test_missing_source(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            target:
              type: csv
              path: /tmp/output.csv
            transform:
              type: ai
              instruction: do things
            """,
        )
        with pytest.raises(ConfigError, match="source"):
            load_config(path)

    def test_missing_target(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: mongo
              url: mongodb://localhost:27017
              database: mydb
              collection: users
            transform:
              type: ai
              instruction: do things
            """,
        )
        with pytest.raises(ConfigError, match="target"):
            load_config(path)

    def test_missing_transform(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: mongo
              url: mongodb://localhost:27017
              database: mydb
              collection: users
            target:
              type: csv
              path: /tmp/output.csv
            """,
        )
        with pytest.raises(ConfigError, match="transform"):
            load_config(path)

    def test_invalid_chunk_size_string(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: mongo
              url: mongodb://localhost:27017
              database: mydb
              collection: users
            target:
              type: csv
              path: /tmp/output.csv
            transform:
              type: ai
              instruction: do stuff
            chunk_size: abc
            """,
        )
        with pytest.raises(ConfigError):
            load_config(path)

    def test_invalid_chunk_size_zero(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: mongo
              url: mongodb://localhost:27017
              database: mydb
              collection: users
            target:
              type: csv
              path: /tmp/output.csv
            transform:
              type: ai
              instruction: do stuff
            chunk_size: 0
            """,
        )
        with pytest.raises(ConfigError, match="chunk_size"):
            load_config(path)

    def test_invalid_source_type(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: oracle
              url: oracle://localhost
            target:
              type: csv
              path: /tmp/output.csv
            transform:
              type: ai
              instruction: do stuff
            """,
        )
        with pytest.raises(ConfigError):
            load_config(path)

    def test_config_file_not_found(self) -> None:
        with pytest.raises(ConfigError, match="not found"):
            load_config("/nonexistent/path/config.yaml")

    def test_invalid_yaml(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.yaml"
        path.write_text(": : : invalid: yaml: [", encoding="utf-8")
        with pytest.raises(ConfigError, match="invalid YAML"):
            load_config(path)

    def test_yaml_is_not_mapping(self, tmp_path: Path) -> None:
        path = tmp_path / "list.yaml"
        path.write_text("- item1\n- item2\n", encoding="utf-8")
        with pytest.raises(ConfigError, match="YAML mapping"):
            load_config(path)


class TestEnvVarInterpolation:
    def test_resolves_env_var(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_DB_URL", "postgresql://u:p@localhost/testdb")
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: postgres
              url: ${TEST_DB_URL}
              query: SELECT 1
            target:
              type: json
              path: /tmp/out.json
            transform:
              type: ai
              instruction: noop
            """,
        )
        config = load_config(path)
        assert config.source.url == "postgresql://u:p@localhost/testdb"

    def test_missing_env_var_raises_config_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("UNSET_VAR_12345", raising=False)
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: postgres
              url: ${UNSET_VAR_12345}
              query: SELECT 1
            target:
              type: json
              path: /tmp/out.json
            transform:
              type: ai
              instruction: noop
            """,
        )
        with pytest.raises(ConfigError, match="UNSET_VAR_12345"):
            load_config(path)


class TestPipelineConfigModel:
    def test_direct_construction(self) -> None:
        config = PipelineConfig(
            source={
                "type": "mongo",
                "url": "mongodb://localhost",
                "database": "db",
                "collection": "c",
            },
            target={"type": "json", "path": "/tmp/out.json"},
            transform={"type": "ai", "instruction": "do stuff"},
        )
        assert config.source.type == "mongo"
        assert config.transform.type == "ai"

    def test_plain_string_transform_via_model_validator(self) -> None:
        config = PipelineConfig(
            source={
                "type": "mongo",
                "url": "mongodb://localhost",
                "database": "db",
                "collection": "c",
            },
            target={"type": "json", "path": "/tmp/out.json"},
            transform="do something cool",
        )
        assert config.transform.type == "ai"
        assert config.transform.instruction == "do something cool"


class TestLLMConfig:
    @pytest.mark.parametrize(
        ("provider", "expected_model"),
        [
            ("gemini", "gemini-3.6-flash"),
            ("claude", "claude-sonnet-5"),
            ("openai", "gpt-5.6-terra"),
            ("qwen", "qwen3.7-plus"),
        ],
    )
    def test_model_defaults_follow_provider(self, provider: str, expected_model: str) -> None:
        config = LLMConfig(provider=provider)  # type: ignore[arg-type]

        assert config.model == expected_model

    def test_explicit_model_pin_is_preserved(self) -> None:
        config = LLMConfig(provider="claude", model="claude-haiku-4-5-20251001")

        assert config.model == "claude-haiku-4-5-20251001"


def _base_source_target() -> dict[str, dict[str, str]]:
    return {
        "source": {
            "type": "mongo",
            "url": "mongodb://localhost",
            "database": "db",
            "collection": "c",
        },
        "target": {"type": "json", "path": "/tmp/out.json"},
    }


class TestPipelineTransformConfig:
    def test_explicit_pipeline_with_mixed_steps(self) -> None:
        config = PipelineConfig(
            **_base_source_target(),
            transform={
                "type": "pipeline",
                "steps": [
                    {"type": "ai", "instruction": "filter paid"},
                    {"type": "sql", "query": "SELECT id FROM {{source}}"},
                ],
            },
        )
        assert config.transform.type == "pipeline"
        assert len(config.transform.steps) == 2
        assert config.transform.steps[0].type == "ai"
        assert config.transform.steps[1].type == "sql"

    def test_inferred_from_list_value(self) -> None:
        config = PipelineConfig(
            **_base_source_target(),
            transform=[
                {"instruction": "filter paid"},
                {"query": "SELECT id FROM {{source}}"},
            ],
        )
        assert config.transform.type == "pipeline"
        assert config.transform.steps[0].type == "ai"
        assert config.transform.steps[1].type == "sql"

    def test_inferred_from_steps_key_without_type(self) -> None:
        config = PipelineConfig(
            **_base_source_target(),
            transform={
                "steps": [
                    {"instruction": "do thing"},
                ],
            },
        )
        assert config.transform.type == "pipeline"
        assert config.transform.steps[0].type == "ai"

    def test_string_step_classified_by_content(self) -> None:
        config = PipelineConfig(
            **_base_source_target(),
            transform=[
                "SELECT id FROM {{source}}",
                "WITH x AS (SELECT 1) SELECT * FROM x",
                "lowercase all emails",
            ],
        )
        assert config.transform.type == "pipeline"
        types = [s.type for s in config.transform.steps]
        assert types == ["sql", "sql", "ai"]
        assert config.transform.steps[0].query.startswith("SELECT")
        assert config.transform.steps[2].instruction == "lowercase all emails"

    def test_plain_string_top_level_select_becomes_sql(self) -> None:
        config = PipelineConfig(
            **_base_source_target(),
            transform="SELECT id FROM {{source}}",
        )
        assert config.transform.type == "sql"
        assert config.transform.query.startswith("SELECT")

    def test_empty_steps_raises(self, tmp_path: Path) -> None:
        path = _write_yaml(
            tmp_path,
            """
            source:
              type: mongo
              url: mongodb://localhost
              database: db
              collection: c
            target:
              type: json
              path: /tmp/out.json
            transform:
              type: pipeline
              steps: []
            """,
        )
        with pytest.raises(ConfigError):
            load_config(path)

    def test_step_name_is_optional_and_preserved(self) -> None:
        config = PipelineConfig(
            **_base_source_target(),
            transform={
                "type": "pipeline",
                "steps": [
                    {"type": "ai", "name": "filter", "instruction": "filter paid"},
                    {"type": "ai", "instruction": "normalise emails"},
                ],
            },
        )
        assert config.transform.steps[0].name == "filter"
        assert config.transform.steps[1].name is None
