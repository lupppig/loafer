"""Durable metadata seam for scheduler and worker processes."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any, Protocol

from loafer.contracts import BatchEnvelope, Checkpoint, JobEnvelope
from loafer.core.roles import WorkerRole
from loafer.core.run_state import RetryCategory, RunState, StageState
from loafer.metadata import (
    BatchCommit,
    OutboxRecord,
    PipelineVersion,
    RunLease,
    RunRecord,
    ScheduleRecord,
    StoredArtifact,
    StoredEvent,
)


class MetadataStore(Protocol):
    """Persist and advance one authoritative durable-execution model."""

    def migrate(self, target_version: int | None = None) -> int:
        """Move the metadata schema to a supported version and return it."""

    def current_schema_version(self) -> int:
        """Return the installed metadata schema version without modifying it."""

    def verify_schema(self) -> int:
        """Require the schema version supported by this build without modifying it."""

    def register_pipeline_version(
        self,
        *,
        workspace_id: str,
        pipeline_key: str,
        config_digest: str,
        config: dict[str, Any],
    ) -> PipelineVersion:
        """Return the immutable version for this exact config digest."""

    def get_pipeline_version(self, version_id: str) -> PipelineVersion:
        """Resolve the immutable configuration claimed by a worker."""

    def create_run(
        self,
        *,
        workspace_id: str,
        pipeline_version_id: str,
        command_key: str,
        run_id: str | None = None,
        parent_run_id: str | None = None,
        retry_category: RetryCategory | None = None,
        role: WorkerRole = WorkerRole.ETL,
        environment_id: str | None = None,
    ) -> RunRecord:
        """Create or return the run identified by an idempotent command."""

    def get_run(self, run_id: str) -> RunRecord:
        """Return one durable run."""

    def claim_run(
        self,
        worker_id: str,
        lease_for: timedelta,
        *,
        role: WorkerRole | None = None,
    ) -> RunLease | None:
        """Claim the next runnable run and issue a new fencing token."""

    def claim_run_by_id(
        self,
        run_id: str,
        worker_id: str,
        lease_for: timedelta,
    ) -> RunLease | None:
        """Claim one dispatched run, or return None when it is not claimable."""

    def list_runnable(
        self,
        *,
        role: WorkerRole | None = None,
        limit: int = 10,
    ) -> list[JobEnvelope]:
        """Return dispatchable job identities without claiming them."""

    def running_count(self, workspace_id: str, *, environment_id: str | None = None) -> int:
        """Count runs currently holding or awaiting a lease for one tenant."""

    def concurrency_limit(
        self,
        workspace_id: str,
        *,
        environment_id: str | None = None,
    ) -> int | None:
        """Return the tightest configured concurrency limit, or None when unset."""

    def quarantine_run(self, lease: RunLease, reason: str) -> RunRecord:
        """Fail a run permanently so the transport stops redelivering it."""

    def heartbeat(self, lease: RunLease, lease_for: timedelta) -> RunLease:
        """Renew a current lease or reject its stale fencing token."""

    def transition_run(
        self,
        lease: RunLease,
        target: RunState,
        *,
        error: dict[str, Any] | None = None,
        retry_category: RetryCategory | None = None,
        retry_at: datetime | None = None,
    ) -> RunRecord:
        """Advance the run state under its active fence."""

    def transition_stage(
        self,
        lease: RunLease,
        stage_name: str,
        target: StageState,
    ) -> None:
        """Create or advance a named stage under its active fence."""

    def append_event(
        self,
        lease: RunLease,
        event_type: str,
        payload: dict[str, Any],
    ) -> StoredEvent:
        """Append one event and allocate its monotonic per-run sequence."""

    def commit_batch(
        self,
        lease: RunLease,
        envelope: BatchEnvelope,
        checkpoint: Checkpoint,
        artifact: StoredArtifact,
    ) -> BatchCommit:
        """Atomically record a committed batch, checkpoint, event, and outbox row."""

    def list_batch_commits(self, run_id: str, partition_id: str) -> list[BatchCommit]:
        """Return committed batches in source order for recovery."""

    def latest_checkpoint(self, run_id: str, partition_id: str) -> Checkpoint | None:
        """Return the last committed checkpoint for one partition."""

    def request_cancel(self, run_id: str) -> RunRecord:
        """Idempotently request cooperative cancellation."""

    def cancellation_requested(self, run_id: str) -> bool:
        """Return whether the worker should cancel at its next safe boundary."""

    def upsert_schedule(
        self,
        schedule: ScheduleRecord,
    ) -> ScheduleRecord:
        """Idempotently create or replace a durable schedule."""

    def enqueue_due_schedules(self, now: datetime) -> list[RunRecord]:
        """Create idempotent run commands for due schedules and advance them."""

    def list_events(self, run_id: str, after: int = 0) -> list[StoredEvent]:
        """Return the append-only event stream after a sequence."""

    def pending_outbox(self, limit: int = 100) -> Sequence[OutboxRecord]:
        """Return unpublished transport records without exposing job contents."""

    def claim_outbox(
        self,
        *,
        limit: int = 100,
        role: WorkerRole | None = None,
        lease_for: timedelta = timedelta(seconds=30),
        event_types: tuple[str, ...] = (),
    ) -> Sequence[OutboxRecord]:
        """Lease unpublished transport records so concurrent relays cannot overlap."""

    def release_outbox(
        self,
        outbox_id: str,
        *,
        error: str,
        retry_after: timedelta = timedelta(seconds=5),
    ) -> None:
        """Return an unpublished record for retry and record why it failed."""

    def mark_outbox_published(self, outbox_id: str, published_at: datetime) -> None:
        """Idempotently mark a transport record as published."""


class BatchRecoveryPort(Protocol):
    """Make bounded batches durable and replay them after a worker crash."""

    def restore(self, run_id: str, partition_id: str) -> list[BatchCommit]:
        """Return durable batches in source order."""

    def read_rows(self, commit: BatchCommit) -> list[dict[str, Any]]:
        """Read the staged output rows for a committed batch."""

    def commit(self, envelope: BatchEnvelope, rows: list[dict[str, Any]]) -> Checkpoint:
        """Stage output and atomically advance the durable checkpoint."""
