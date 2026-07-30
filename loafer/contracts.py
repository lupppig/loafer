"""Serializable engine/application contracts shared by clients and workers."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator


def _utc_now() -> datetime:
    return datetime.now(UTC)


class ContractModel(BaseModel):
    """Strict, immutable base for durable application contracts."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class StageStatus(StrEnum):
    RUNNING = "running"
    DONE = "done"
    SKIPPED = "skipped"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RunStatus(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RunRequest(ContractModel):
    """Command to execute one immutable local pipeline configuration."""

    config_path: str
    run_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    dry_run: bool = False
    auto_confirm: bool = False
    full_refresh: bool = False


class ExecutionPlan(ContractModel):
    """Credential-free description of a validated pipeline execution."""

    contract_version: Literal[1] = 1
    plan_id: str
    config_digest: str
    config_path: str
    pipeline_name: str
    mode: Literal["etl", "elt"]
    source_type: str
    target_type: str
    transform_type: str
    transform_class: str = "materialized"
    chunk_size: int = Field(gt=0)
    streaming_threshold: int = Field(gt=0)
    validation_strict: bool
    schema_drift_policy: str = "fail"
    delivery_guarantee: str = "target_defined"
    llm_provider: str
    llm_model: str
    incremental_column: str | None = None
    cursor_value: JsonValue = None
    dry_run: bool = False
    auto_confirm: bool = False
    full_refresh: bool = False

    @field_validator("config_digest")
    @classmethod
    def digest_is_sha256(cls, value: str) -> str:
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError("config_digest must be a lowercase SHA-256 hex digest")
        return value


class BatchEnvelope(ContractModel):
    """Durable metadata for one bounded data-plane batch."""

    contract_version: Literal[1] = 1
    run_id: str
    stage_id: str
    partition_id: str
    batch_id: str
    attempt: int = Field(ge=0)
    source_position_start: JsonValue = None
    source_position_end: JsonValue = None
    schema_version: str
    transform_artifact_version: str | None = None
    rows_in: int = Field(ge=0)
    rows_out: int = Field(ge=0)
    rows_rejected: int = Field(ge=0)
    rows_filtered: int = Field(default=0, ge=0)
    bytes_in: int = Field(ge=0)
    bytes_out: int = Field(ge=0)
    checksum: str | None = None
    input_checksum: str | None = None
    output_checksum: str | None = None
    duration_ms: float = Field(default=0.0, ge=0)


class Checkpoint(ContractModel):
    """Last durable source position for a committed target effect."""

    contract_version: Literal[1] = 1
    checkpoint_id: str
    run_id: str
    partition_id: str
    batch_id: str
    source_position: JsonValue
    committed_at: datetime = Field(default_factory=_utc_now)


class TransformStepResult(ContractModel):
    """Serializable outcome of one multi-step transform entry."""

    index: int = Field(ge=0)
    name: str
    type: str
    rows_in: int = Field(ge=0)
    rows_out: int = Field(ge=0)
    duration_ms: float = Field(ge=0)
    success: bool
    error: str | None = None
    token_usage: dict[str, int] = Field(default_factory=dict)


class RunSnapshot(ContractModel):
    """Sanitized durable projection of ephemeral graph state."""

    run_id: str
    plan_id: str
    pipeline_name: str
    mode: Literal["etl", "elt"]
    source_type: str
    target_type: str
    transform_type: str
    rows_extracted: int = Field(ge=0)
    rows_transformed: int = Field(ge=0)
    rows_loaded: int = Field(ge=0)
    rows_rejected: int = Field(default=0, ge=0)
    rows_filtered: int = Field(default=0, ge=0)
    batches_completed: int = Field(default=0, ge=0)
    bytes_in: int = Field(default=0, ge=0)
    bytes_out: int = Field(default=0, ge=0)
    input_checksum: str | None = None
    output_checksum: str | None = None
    schema_version: str | None = None
    transform_artifact_version: str | None = None
    validation_passed: bool
    duration_ms: dict[str, float] = Field(default_factory=dict)
    warnings: tuple[str, ...] = ()
    token_usage: dict[str, int] = Field(default_factory=dict)
    step_results: tuple[TransformStepResult, ...] = ()
    error: str | None = None


class RunEvent(ContractModel):
    """Append-friendly stage event emitted by the application service."""

    contract_version: Literal[1] = 1
    run_id: str
    plan_id: str
    sequence: int = Field(gt=0)
    stage: str
    status: StageStatus
    occurred_at: datetime = Field(default_factory=_utc_now)
    snapshot: RunSnapshot
    batch: BatchEnvelope | None = None


class RunResult(ContractModel):
    """Final sanitized result of an application-level pipeline run."""

    contract_version: Literal[1] = 1
    run_id: str
    plan_id: str
    status: RunStatus
    started_at: datetime
    finished_at: datetime
    output_published: bool
    snapshot: RunSnapshot


class ValidationResult(ContractModel):
    """Successful validation response for a client."""

    valid: Literal[True] = True
    plan: ExecutionPlan


class ConnectorCatalog(ContractModel):
    """Serializable list of connector types available to clients."""

    sources: tuple[str, ...]
    targets: tuple[str, ...]
