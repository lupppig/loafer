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


def _walk_and_resolve(obj: Any, base_dir: Path | None = None) -> Any:
    """Recursively resolve env vars in strings within a nested structure.

    When *base_dir* is provided, relative file paths (starting with ``.`` or ``..``)
    are resolved to absolute paths so they work regardless of the current working
    directory.
    """
    if isinstance(obj, str):
        value = _resolve_env_vars(obj)
        if base_dir is not None:
            raw = value.lstrip()
            if raw.startswith("./") or raw.startswith("../") or raw == ".":
                resolved = (base_dir / raw).resolve()
                return str(resolved)
        return value
    if isinstance(obj, dict):
        return {k: _walk_and_resolve(v, base_dir) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk_and_resolve(item, base_dir) for item in obj]
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


def _validate_upsert_key(cfg: Any) -> Any:
    """Normalise ``key`` to a list and require it when ``write_mode`` is ``upsert``."""
    if isinstance(cfg.key, str):
        cfg.key = [cfg.key]
    if cfg.write_mode == "upsert" and not cfg.key:
        raise ValueError("write_mode 'upsert' requires a non-empty 'key' (column or columns)")
    return cfg


class PostgresTargetConfig(BaseModel):
    type: Literal["postgres"]
    url: str
    table: str
    write_mode: Literal["append", "replace", "error", "upsert"] = "append"
    key: str | list[str] | None = None

    @model_validator(mode="after")
    def key_required_for_upsert(self) -> PostgresTargetConfig:
        return _validate_upsert_key(self)


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
    write_mode: Literal["append", "replace", "error", "upsert"] = "append"
    key: str | list[str] | None = None

    @model_validator(mode="after")
    def key_required_for_upsert(self) -> MongoTargetConfig:
        return _validate_upsert_key(self)


TargetConfig = Annotated[
    PostgresTargetConfig | CsvTargetConfig | JsonTargetConfig | MongoTargetConfig,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Transform configs
# ---------------------------------------------------------------------------


class AITransformConfig(BaseModel):
    type: Literal["ai"] = "ai"
    name: str | None = None
    instruction: str
    # Optional custom Python transform to combine with AI
    custom_path: str | None = None
    # Order: "custom_first" (default) runs custom then AI,
    # "ai_first" runs AI then custom
    custom_order: Literal["custom_first", "ai_first"] = "custom_first"
    # If true, skip AI entirely and just run custom transform
    bypass_ai: bool = False
    # If true, show generated AI code and wait for user confirmation before executing
    review: bool = False

    @field_validator("custom_path")
    @classmethod
    def custom_path_must_exist(cls, v: str | None) -> str | None:
        if v is not None and not Path(v).exists():
            raise ValueError(f"custom transform file not found: {v}")
        return v


class CustomTransformConfig(BaseModel):
    type: Literal["custom"]
    name: str | None = None
    path: str

    @field_validator("path")
    @classmethod
    def path_must_exist(cls, v: str) -> str:
        if not Path(v).exists():
            raise ValueError(f"transform file not found: {v}")
        return v


class SQLTransformConfig(BaseModel):
    type: Literal["sql"]
    name: str | None = None
    query: str


_PipelineStepConfig = Annotated[
    AITransformConfig | CustomTransformConfig | SQLTransformConfig,
    Field(discriminator="type"),
]


class PipelineTransformConfig(BaseModel):
    type: Literal["pipeline"] = "pipeline"
    name: str | None = None
    steps: list[_PipelineStepConfig]
    stop_on_empty: bool = True

    @field_validator("steps")
    @classmethod
    def steps_must_be_non_empty(cls, v: list[Any]) -> list[Any]:
        if not v:
            raise ValueError("pipeline transform requires at least one step")
        return v


TransformConfig = Annotated[
    AITransformConfig | CustomTransformConfig | SQLTransformConfig | PipelineTransformConfig,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Validation config
# ---------------------------------------------------------------------------


class ValidationConfig(BaseModel):
    max_null_rate: float = 0.5
    strict: bool = False


# ---------------------------------------------------------------------------
# Sandbox config
# ---------------------------------------------------------------------------


class SandboxConfig(BaseModel):
    """Resource limits for executing custom/AI transform code."""

    timeout: int = 60
    max_memory_mb: int = 512

    @field_validator("timeout", "max_memory_mb")
    @classmethod
    def must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("sandbox limits must be positive integers")
        return v


# ---------------------------------------------------------------------------
# Incremental loading config
# ---------------------------------------------------------------------------


class IncrementalConfig(BaseModel):
    """Cursor-based incremental extraction.

    Only rows whose ``column`` exceeds the last-seen watermark are extracted.
    """

    column: str
    initial: Any = None
    # REST only: query-param name carrying the cursor (defaults to ``column``).
    param: str | None = None


# ---------------------------------------------------------------------------
# LLM config
# ---------------------------------------------------------------------------


class LLMConfig(BaseModel):
    provider: Literal["gemini", "claude", "openai", "qwen"] = "gemini"
    model: str = "gemini-2.0-flash"
    api_key: str | None = None


# ---------------------------------------------------------------------------
# Auto-detection helpers
# ---------------------------------------------------------------------------

# Mapping from URL scheme prefixes to source type names
_SOURCE_URL_SCHEMES: list[tuple[str, str]] = [
    ("postgresql://", "postgres"),
    ("postgres://", "postgres"),
    ("mysql://", "mysql"),
    ("mysql+pymysql://", "mysql"),
    ("mongodb://", "mongo"),
    ("mongodb+srv://", "mongo"),
    ("http://", "rest_api"),
    ("https://", "rest_api"),
]

# Mapping from file extension to source type name
_SOURCE_EXT_MAP: dict[str, str] = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".pdf": "pdf",
    ".db": "sqlite",
    ".sqlite": "sqlite",
    ".sqlite3": "sqlite",
}

# Mapping from URL scheme prefixes to target type names
_TARGET_URL_SCHEMES: list[tuple[str, str]] = [
    ("postgresql://", "postgres"),
    ("postgres://", "postgres"),
    ("mongodb://", "mongo"),
    ("mongodb+srv://", "mongo"),
]

# Mapping from file extension to target type name
_TARGET_EXT_MAP: dict[str, str] = {
    ".csv": "csv",
    ".json": "json",
    ".jsonl": "json",
}


def _infer_source_type(data: dict[str, Any]) -> str | None:
    """Infer the source connector type from url or path fields.

    Returns the type string if detected, or *None* if detection fails.
    """
    url = data.get("url")
    if isinstance(url, str):
        lower = url.lower()
        for prefix, stype in _SOURCE_URL_SCHEMES:
            if lower.startswith(prefix):
                return stype

    path = data.get("path")
    if isinstance(path, str):
        ext = Path(path).suffix.lower()
        if ext in _SOURCE_EXT_MAP:
            return _SOURCE_EXT_MAP[ext]

    return None


def _infer_target_type(data: dict[str, Any]) -> str | None:
    """Infer the target connector type from url or path fields.

    Returns the type string if detected, or *None* if detection fails.
    """
    url = data.get("url")
    if isinstance(url, str):
        lower = url.lower()
        for prefix, ttype in _TARGET_URL_SCHEMES:
            if lower.startswith(prefix):
                return ttype

    path = data.get("path")
    if isinstance(path, str):
        ext = Path(path).suffix.lower()
        if ext in _TARGET_EXT_MAP:
            return _TARGET_EXT_MAP[ext]

    return None


def _infer_transform_type(data: dict[str, Any]) -> str | None:
    """Infer the transform type from which fields are present.

    Returns the type string if detected, or *None* if detection fails.
    """
    if "steps" in data:
        return "pipeline"
    if "instruction" in data:
        return "ai"
    if "path" in data:
        return "custom"
    if "query" in data:
        return "sql"
    return None


def _coerce_transform_string(s: str) -> dict[str, Any]:
    """Classify a bare string transform value by content.

    - Starts with ``SELECT`` or ``WITH`` (case-insensitive, stripped) → sql.
    - Ends with ``.py`` (stripped) → custom.
    - Otherwise → ai instruction.
    """
    stripped = s.strip()
    lower = stripped.lower()
    if lower.startswith("select") or lower.startswith("with"):
        return {"type": "sql", "query": s}
    if stripped.endswith(".py"):
        return {"type": "custom", "path": stripped}
    return {"type": "ai", "instruction": s}


def _normalise_steps(steps: list[Any]) -> list[Any]:
    """Coerce string entries and infer missing step types for a pipeline."""
    out: list[Any] = []
    for entry in steps:
        if isinstance(entry, str):
            out.append(_coerce_transform_string(entry))
        elif isinstance(entry, dict):
            step = dict(entry)
            if "type" not in step:
                inferred = _infer_transform_type(step)
                if inferred is None:
                    raise ConfigError(
                        "cannot auto-detect transform step type — add a 'type' "
                        "field or include 'instruction', 'path', or 'query'"
                    )
                step["type"] = inferred
            out.append(step)
        else:
            out.append(entry)
    return out


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
    incremental: IncrementalConfig | None = None
    sandbox: SandboxConfig = Field(default_factory=SandboxConfig)

    @field_validator("chunk_size")
    @classmethod
    def chunk_size_must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("chunk_size must be a positive integer")
        return v

    @model_validator(mode="before")
    @classmethod
    def normalise_transform(cls, data: Any) -> Any:
        """Coerce shorthand transform forms into dict configs.

        - Bare string at the top level → classified by ``_coerce_transform_string``.
        - List value → wrapped into a pipeline; bare-string entries inside the list
          are classified the same way, and dict steps without ``type`` get inferred.
        """
        if not isinstance(data, dict):
            return data

        tfm = data.get("transform")
        if isinstance(tfm, str):
            data["transform"] = _coerce_transform_string(tfm)
        elif isinstance(tfm, list):
            data["transform"] = {"type": "pipeline", "steps": _normalise_steps(tfm)}
        elif isinstance(tfm, dict) and tfm.get("type") == "pipeline":
            steps = tfm.get("steps")
            if isinstance(steps, list):
                tfm["steps"] = _normalise_steps(steps)
        return data

    @model_validator(mode="before")
    @classmethod
    def infer_missing_types(cls, data: Any) -> Any:
        """Auto-detect connector types when ``type`` is omitted."""
        if not isinstance(data, dict):
            return data

        # --- source ---
        src = data.get("source")
        if isinstance(src, dict) and "type" not in src:
            inferred = _infer_source_type(src)
            if inferred is None:
                raise ConfigError(
                    "cannot auto-detect source type — add a 'type' field "
                    "or use a recognisable URL scheme / file extension"
                )
            src["type"] = inferred

        # --- target ---
        tgt = data.get("target")
        if isinstance(tgt, dict) and "type" not in tgt:
            inferred = _infer_target_type(tgt)
            if inferred is None:
                raise ConfigError(
                    "cannot auto-detect target type — add a 'type' field "
                    "or use a recognisable URL scheme / file extension"
                )
            tgt["type"] = inferred

        # --- transform ---
        tfm = data.get("transform")
        if isinstance(tfm, dict) and "type" not in tfm:
            inferred = _infer_transform_type(tfm)
            if inferred is None:
                raise ConfigError(
                    "cannot auto-detect transform type — add a 'type' field "
                    "or include 'instruction', 'path', 'query', or 'steps'"
                )
            tfm["type"] = inferred

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

    config_dir = config_path.resolve().parent
    resolved = _walk_and_resolve(raw, base_dir=config_dir)

    try:
        return PipelineConfig(**resolved)
    except Exception as e:
        raise ConfigError(str(e)) from e
