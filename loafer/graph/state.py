"""Ephemeral in-process state for the current LangGraph execution.

Every agent receives this state, operates on it, and returns an updated copy.
LangGraph nodes must return updated state, never mutate in place.

This is deliberately not a persistence contract: it may contain live
connectors, iterators, providers, and review callbacks. Durable clients use
the sanitized contracts in :mod:`loafer.application.contracts`.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, TypedDict

from loafer.config import LLMConfig, SourceConfig, TargetConfig, TransformConfig


@dataclass
class StepResult:
    """Outcome of a single step inside a multi-step transform pipeline."""

    index: int
    name: str
    type: str
    rows_in: int
    rows_out: int
    duration_ms: float
    success: bool
    error: str | None = None
    token_usage: dict[str, int] | None = None


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
    execution_config: Any

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
    llm_provider: Any
    reviewer: Any
    generated_code: str
    retry_count: int
    last_error: str | None
    token_usage: dict[str, int]
    transform_retry_count: int
    transform_in_target_retry_count: int

    # ELT specific
    raw_table_name: str | None
    generated_sql: str | None

    # Execution metadata
    run_id: str
    rows_extracted: int
    rows_transformed: int
    rows_loaded: int
    rows_rejected: int
    rows_filtered: int
    batches_completed: int
    bytes_in: int
    bytes_out: int
    input_checksum: str | None
    output_checksum: str | None
    schema_version: str | None
    transform_artifact_version: str | None
    last_batch_envelope: Any | None
    target_published: bool
    duration_ms: dict[str, float]
    warnings: list[str]
    is_streaming: bool
    stream_iterator: Iterator[list[dict[str, Any]]] | None

    # Destructive operation detection
    destructive_warnings: list[Any]
    auto_confirmed: bool

    # Multi-step transform pipeline
    step_results: list[StepResult]

    # Sandbox limits for custom/AI transform execution
    sandbox_config: Any

    # Incremental loading (cursor watermark)
    incremental_config: Any | None
    cursor_value: Any
    new_cursor: Any
    state_key: str
    state_store_path: str | None

    # Internal (not exposed to agents)
    _source_connector: Any | None
