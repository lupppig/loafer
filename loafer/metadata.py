"""Serializable contracts for durable single-node execution metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from loafer.contracts import BatchEnvelope, Checkpoint
from loafer.core.roles import WorkerRole
from loafer.core.run_state import RetryCategory, RunState


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class PipelineVersion:
    id: str
    workspace_id: str
    pipeline_key: str
    config_digest: str
    config: dict[str, Any]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class RunRecord:
    id: str
    workspace_id: str
    pipeline_version_id: str
    command_key: str
    state: RunState
    attempt: int
    retry_category: RetryCategory | None
    cancel_requested: bool
    fencing_token: int
    lease_owner: str | None
    lease_expires_at: datetime | None
    heartbeat_at: datetime | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    parent_run_id: str | None = None
    error: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class RunLease:
    run: RunRecord
    worker_id: str
    fencing_token: int
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class StoredEvent:
    run_id: str
    sequence: int
    event_type: str
    payload: dict[str, Any]
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class StoredArtifact:
    id: str
    run_id: str | None
    kind: str
    uri: str
    checksum: str
    size_bytes: int
    metadata: dict[str, Any]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class BatchCommit:
    envelope: BatchEnvelope
    checkpoint: Checkpoint
    artifact: StoredArtifact


@dataclass(frozen=True, slots=True)
class RecoveredBatch:
    envelope: BatchEnvelope
    checkpoint: Checkpoint
    artifact: StoredArtifact
    rows: tuple[dict[str, Any], ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class ScheduleRecord:
    id: str
    workspace_id: str
    pipeline_version_id: str
    trigger_kind: str
    trigger_spec: str
    timezone: str
    enabled: bool
    next_run_at: datetime
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    id: str
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload: dict[str, Any]
    available_at: datetime
    published_at: datetime | None
    attempts: int
    role: str = WorkerRole.ETL.value
    claimed_until: datetime | None = None
    last_error: str | None = None
