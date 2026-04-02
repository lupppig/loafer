"""Pydantic v2 configuration models and YAML parser.

All pipeline configuration is validated at parse time, not at runtime inside agents.
Environment variable interpolation is supported: ${VAR_NAME} in YAML values.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from loafer.exceptions import ConfigError

_ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)}")


def _resolve_env_vars(value: str) -> str:
    """Replace ${VAR} placeholders with environment variable values."""

    def _replacer(match: re.Match[str]) -> str:
        var_name = match.group(1)
        env_value = os.environ.get(var_name)
        if env_value is None:
            raise ConfigError(
                f"environment variable '{var_name}' is not set "
                f"(referenced in config as ${{{var_name}}})"
            )
        return env_value

    return _ENV_VAR_PATTERN.sub(_replacer, value)


def _walk_and_resolve(obj: Any) -> Any:
    """Recursively resolve env vars in strings within a nested structure."""
    if isinstance(obj, str):
        return _resolve_env_vars(obj)
    if isinstance(obj, dict):
        return {k: _walk_and_resolve(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk_and_resolve(item) for item in obj]
    return obj


# ---------------------------------------------------------------------------
# Source configs
# ---------------------------------------------------------------------------


class PostgresSourceConfig(BaseModel):
    type: Literal["postgres"]
    url: str
    query: str
    timeout: int = 30


class MySQLSourceConfig(BaseModel):
    type: Literal["mysql"]
    url: str
    query: str
    timeout: int = 30


class MongoSourceConfig(BaseModel):
    type: Literal["mongo"]
    url: str
    database: str
    collection: str
    filter: dict[str, Any] = Field(default_factory=dict)


class CsvSourceConfig(BaseModel):
    type: Literal["csv"]
    path: str
    has_header: bool = True
    encoding: str = "utf-8"
    column_names: list[str] | None = None

    @field_validator("path")
    @classmethod
    def path_must_exist(cls, v: str) -> str:
        if not Path(v).exists():
            raise ValueError(f"CSV file not found: {v}")
        return v


class ExcelSourceConfig(BaseModel):
    type: Literal["excel"]
    path: str
    sheet: str | None = None

    @field_validator("path")
    @classmethod
    def path_must_exist(cls, v: str) -> str:
        if not Path(v).exists():
            raise ValueError(f"Excel file not found: {v}")
        return v


class RestApiSourceConfig(BaseModel):
    type: Literal["rest_api"]
    url: str
    method: Literal["GET", "POST"] = "GET"
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, str] = Field(default_factory=dict)
    body: dict[str, Any] | None = None
    response_key: str | None = None
    pagination: dict[str, Any] | None = None
    auth_token: str | None = None
    verify_ssl: bool = True
    timeout: int = 30


class SqliteSourceConfig(BaseModel):
    type: Literal["sqlite"]
    path: str
    query: str

    @field_validator("path")
    @classmethod
    def path_must_exist(cls, v: str) -> str:
        if not Path(v).exists():
            raise ValueError(f"SQLite database not found: {v}")
        return v


class PdfSourceConfig(BaseModel):
    type: Literal["pdf"]
    path: str
    extract_tables: bool = True

    @field_validator("path")
    @classmethod
    def path_must_exist(cls, v: str) -> str:
        if not Path(v).exists():
            raise ValueError(f"PDF file not found: {v}")
        return v


SourceConfig = Annotated[
    PostgresSourceConfig
    | MySQLSourceConfig
    | MongoSourceConfig
    | CsvSourceConfig
    | ExcelSourceConfig
    | RestApiSourceConfig
    | SqliteSourceConfig
    | PdfSourceConfig,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Target configs
# ---------------------------------------------------------------------------


class PostgresTargetConfig(BaseModel):
    type: Literal["postgres"]
    url: str
    table: str
    write_mode: Literal["append", "replace", "error"] = "append"


class CsvTargetConfig(BaseModel):
    type: Literal["csv"]
    path: str
    write_mode: Literal["overwrite", "error"] = "overwrite"


class JsonTargetConfig(BaseModel):
    type: Literal["json"]
    path: str
    write_mode: Literal["overwrite", "error"] = "overwrite"


class MongoTargetConfig(BaseModel):
    type: Literal["mongo"]
    url: str
    database: str
    collection: str
    write_mode: Literal["append", "replace", "error"] = "append"


TargetConfig = Annotated[
    PostgresTargetConfig | CsvTargetConfig | JsonTargetConfig | MongoTargetConfig,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Transform configs
# ---------------------------------------------------------------------------


class AITransformConfig(BaseModel):
    type: Literal["ai"] = "ai"
    instruction: str


class CustomTransformConfig(BaseModel):
    type: Literal["custom"]
    path: str

    @field_validator("path")
    @classmethod
    def path_must_exist(cls, v: str) -> str:
        if not Path(v).exists():
            raise ValueError(f"transform file not found: {v}")
        return v


class SQLTransformConfig(BaseModel):
    type: Literal["sql"]
    query: str


TransformConfig = Annotated[
    AITransformConfig | CustomTransformConfig | SQLTransformConfig,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Validation config
# ---------------------------------------------------------------------------


class ValidationConfig(BaseModel):
    max_null_rate: float = 0.5
    strict: bool = False


# ---------------------------------------------------------------------------
# LLM config
# ---------------------------------------------------------------------------


class LLMConfig(BaseModel):
    provider: Literal["gemini", "claude", "openai", "qwen"] = "gemini"
    model: str = "gemini-1.5-flash"
    api_key: str | None = None


# ---------------------------------------------------------------------------
# Pipeline config (top-level)
# ---------------------------------------------------------------------------


class PipelineConfig(BaseModel):
    name: str = ""
    source: SourceConfig
    target: TargetConfig
    transform: TransformConfig
    mode: Literal["etl", "elt"] = "etl"
    chunk_size: int = 500
    streaming_threshold: int = 10_000
    destructive_filter_threshold: float = 0.3
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)

    @field_validator("chunk_size")
    @classmethod
    def chunk_size_must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("chunk_size must be a positive integer")
        return v

    @model_validator(mode="before")
    @classmethod
    def coerce_plain_string_transform(cls, data: Any) -> Any:
        """Allow `transform: "some instruction"` as shorthand for ai mode."""
        if isinstance(data, dict) and isinstance(data.get("transform"), str):
            data["transform"] = {"type": "ai", "instruction": data["transform"]}
        return data


def load_config(path: str | Path) -> PipelineConfig:
    """Load and validate a pipeline config from a YAML file."""
    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"config file not found: {config_path}")

    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ConfigError(f"invalid YAML in {config_path}: {e}") from e

    if not isinstance(raw, dict):
        raise ConfigError(f"config file must contain a YAML mapping, got {type(raw).__name__}")

    resolved = _walk_and_resolve(raw)

    try:
        return PipelineConfig(**resolved)
    except Exception as e:
        raise ConfigError(str(e)) from e
