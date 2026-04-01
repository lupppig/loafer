"""PipelineState — the single source of truth for all data flowing through the system.

Every agent receives this state, operates on it, and returns an updated copy.
LangGraph nodes must return updated state, never mutate in place.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, TypedDict

from loafer.config import LLMConfig, SourceConfig, TargetConfig, TransformConfig


class PipelineState(TypedDict, total=False):
    # Config
    source_config: SourceConfig
    target_config: TargetConfig
    transform_config: TransformConfig
    llm_config: LLMConfig
    transform_instruction: str
    mode: str
    chunk_size: int
    streaming_threshold: int
    destructive_filter_threshold: float

    # Data (mutated per agent)
    raw_data: list[dict[str, Any]]
    transformed_data: list[dict[str, Any]]

    # Schema (set by Extract Agent, read by Transform Agent for LLM prompt)
    schema_sample: dict[str, Any]

    # Validation
    validation_report: dict[str, Any]
    validation_passed: bool
    max_null_rate: float
    strict_validation: bool

    # LLM
    generated_code: str
    retry_count: int
    last_error: str | None
    token_usage: dict[str, int]
    transform_retry_count: int

    # ELT specific
    raw_table_name: str | None
    generated_sql: str | None

    # Execution metadata
    run_id: str
    rows_extracted: int
    rows_loaded: int
    duration_ms: dict[str, float]
    warnings: list[str]
    is_streaming: bool
    stream_iterator: Iterator[list[dict[str, Any]]] | None

    # Destructive operation detection
    destructive_warnings: list[Any]
    auto_confirmed: bool
