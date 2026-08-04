"""SQLAlchemy metadata adapter shared by PostgreSQL and local SQLite."""

from __future__ import annotations

import hashlib
import uuid
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, event, insert, or_, select, text, update
from sqlalchemy.engine import Connection, Engine, RowMapping, create_engine
from sqlalchemy.exc import IntegrityError

from loafer.adapters import metadata_schema as schema
from loafer.contracts import BatchEnvelope, Checkpoint
from loafer.core.run_state import (
    BatchState,
    RetryCategory,
    RunState,
    StageState,
    require_batch_transition,
    require_run_transition,
    require_stage_transition,
)
from loafer.exceptions import IdempotencyConflictError, MetadataError, StaleFenceError
from loafer.metadata import (
    BatchCommit,
    OutboxRecord,
    PipelineVersion,
    RunLease,
    RunRecord,
    ScheduleRecord,
    StoredArtifact,
    StoredEvent,
    utc_now,
)

_ACTIVE_STATES = (RunState.CLAIMED.value, RunState.RUNNING.value, RunState.CANCELLING.value)


class SqlMetadataStore:
    """Authoritative durable state with PostgreSQL and restricted SQLite profiles.

    PostgreSQL uses row locks for concurrent claims and event allocation. SQLite is
    intentionally advertised only for one scheduler and one worker process.
    """

    def __init__(
        self,
        url: str,
        *,
        clock: Any = utc_now,
        engine: Engine | None = None,
    ) -> None:
        self._clock = clock
        self._engine = engine or create_engine(url, future=True)
        if self._engine.dialect.name == "sqlite":
            event.listen(self._engine, "connect", _enable_sqlite_foreign_keys)

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def profile(self) -> str:
        return self._engine.dialect.name

    def close(self) -> None:
        self._engine.dispose()

    def migrate(self, target_version: int | None = None) -> int:
        return schema.migrate(self._engine, target_version)

    def current_schema_version(self) -> int:
        return schema.current_version(self._engine)

    def verify_schema(self) -> int:
        """Fail unless the database matches this build, without running DDL."""
        return schema.require_current_version(self._engine)

    def _transaction(self, connection: Connection | None) -> Any:
        return nullcontext(connection) if connection is not None else self._engine.begin()

    def register_pipeline_version(
        self,
        *,
        workspace_id: str,
        pipeline_key: str,
        config_digest: str,
        config: dict[str, Any],
        connection: Connection | None = None,
    ) -> PipelineVersion:
        version_id = hashlib.sha256(
            f"{workspace_id}\0{pipeline_key}\0{config_digest}".encode()
        ).hexdigest()[:32]
        now = self._clock()
        with self._transaction(connection) as connection:
            self._set_workspace_scope(connection, workspace_id)
            row = (
                connection.execute(
                    select(schema.pipeline_versions).where(
                        schema.pipeline_versions.c.id == version_id
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                connection.execute(
                    insert(schema.pipeline_versions).values(
                        id=version_id,
                        workspace_id=workspace_id,
                        pipeline_key=pipeline_key,
                        config_digest=config_digest,
                        config_json=config,
                        created_at=now,
                    )
                )
                row = (
                    connection.execute(
                        select(schema.pipeline_versions).where(
                            schema.pipeline_versions.c.id == version_id
                        )
                    )
                    .mappings()
                    .one()
                )
            elif row["config_json"] != config:
                raise IdempotencyConflictError(
                    "pipeline version digest was reused for different configuration"
                )
            return _pipeline_version(row)

    def get_pipeline_version(self, version_id: str) -> PipelineVersion:
        with self._engine.connect() as connection:
            row = (
                connection.execute(
                    select(schema.pipeline_versions).where(
                        schema.pipeline_versions.c.id == version_id
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise MetadataError(f"pipeline version not found: {version_id}")
            return _pipeline_version(row)

    def create_run(
        self,
        *,
        workspace_id: str,
        pipeline_version_id: str,
        command_key: str,
        run_id: str | None = None,
        parent_run_id: str | None = None,
        retry_category: RetryCategory | None = None,
        connection: Connection | None = None,
    ) -> RunRecord:
        now = self._clock()
        with self._transaction(connection) as connection:
            self._set_workspace_scope(connection, workspace_id)
            return self._create_run(
                connection,
                workspace_id=workspace_id,
                pipeline_version_id=pipeline_version_id,
                command_key=command_key,
                run_id=run_id or uuid.uuid4().hex,
                parent_run_id=parent_run_id,
                retry_category=retry_category,
                now=now,
            )

    def get_run(self, run_id: str) -> RunRecord:
        with self._engine.connect() as connection:
            return _run_record(self._run_row(connection, run_id))

    def claim_run(self, worker_id: str, lease_for: timedelta) -> RunLease | None:
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        now = self._clock()
        expires_at = now + lease_for
        runnable = or_(
            schema.runs.c.state == RunState.QUEUED.value,
            and_(
                schema.runs.c.state == RunState.RETRY_WAIT.value,
                schema.runs.c.retry_at <= now,
            ),
            and_(
                schema.runs.c.state.in_(_ACTIVE_STATES),
                schema.runs.c.lease_expires_at <= now,
            ),
        )
        with self._engine.begin() as connection:
            query = (
                select(schema.runs)
                .where(runnable)
                .order_by(schema.runs.c.created_at, schema.runs.c.id)
                .limit(1)
            )
            if self.profile == "postgresql":
                query = query.with_for_update(skip_locked=True)
            row = connection.execute(query).mappings().one_or_none()
            if row is None:
                return None
            current_state = RunState(row["state"])
            if current_state == RunState.RETRY_WAIT:
                require_run_transition(current_state, RunState.QUEUED)
                current_state = RunState.QUEUED
            elif current_state in {
                RunState.CLAIMED,
                RunState.RUNNING,
                RunState.CANCELLING,
            }:
                require_run_transition(current_state, RunState.RETRY_WAIT)
                require_run_transition(RunState.RETRY_WAIT, RunState.QUEUED)
                current_state = RunState.QUEUED
            require_run_transition(current_state, RunState.CLAIMED)
            token = int(row["fencing_token"]) + 1
            attempt = int(row["attempt"])
            if row["state"] in _ACTIVE_STATES:
                attempt += 1
            result = connection.execute(
                update(schema.runs)
                .where(
                    schema.runs.c.id == row["id"],
                    schema.runs.c.fencing_token == row["fencing_token"],
                )
                .values(
                    state=RunState.CLAIMED.value,
                    attempt=attempt,
                    retry_category=(
                        RetryCategory.INFRASTRUCTURE.value
                        if row["state"] in _ACTIVE_STATES
                        else row["retry_category"]
                    ),
                    retry_at=None,
                    lease_owner=worker_id,
                    fencing_token=token,
                    lease_expires_at=expires_at,
                    heartbeat_at=now,
                    finished_at=None,
                )
            )
            if result.rowcount != 1:
                return None
            claimed = self._run_row(connection, row["id"])
            self._append_event(
                connection,
                claimed,
                "run.claimed",
                {"worker_id": worker_id, "fencing_token": token, "attempt": attempt},
                now,
            )
            run = _run_record(claimed)
            return RunLease(
                run=run,
                worker_id=worker_id,
                fencing_token=token,
                expires_at=expires_at,
            )

    def heartbeat(self, lease: RunLease, lease_for: timedelta) -> RunLease:
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        now = self._clock()
        expires_at = now + lease_for
        with self._engine.begin() as connection:
            row = self._require_fence(connection, lease, now)
            result = connection.execute(
                update(schema.runs)
                .where(
                    schema.runs.c.id == lease.run.id,
                    schema.runs.c.fencing_token == lease.fencing_token,
                    schema.runs.c.lease_owner == lease.worker_id,
                )
                .values(heartbeat_at=now, lease_expires_at=expires_at)
            )
            if result.rowcount != 1:
                raise StaleFenceError(f"stale fencing token for run {lease.run.id}")
            updated = _run_record(self._run_row(connection, row["id"]))
            return RunLease(updated, lease.worker_id, lease.fencing_token, expires_at)

    def transition_run(
        self,
        lease: RunLease,
        target: RunState,
        *,
        error: dict[str, Any] | None = None,
        retry_category: RetryCategory | None = None,
        retry_at: datetime | None = None,
    ) -> RunRecord:
        now = self._clock()
        with self._engine.begin() as connection:
            row = self._require_fence(connection, lease, now)
            current = RunState(row["state"])
            require_run_transition(current, target)
            values: dict[str, Any] = {"state": target.value}
            if target == RunState.RUNNING and row["started_at"] is None:
                values["started_at"] = now
            if target in {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}:
                values.update(
                    finished_at=now,
                    lease_owner=None,
                    lease_expires_at=None,
                    heartbeat_at=None,
                )
            if target == RunState.RETRY_WAIT:
                if retry_at is None or retry_category is None:
                    raise MetadataError("retry transitions require retry_at and retry_category")
                values.update(
                    retry_at=retry_at,
                    retry_category=retry_category.value,
                    lease_owner=None,
                    lease_expires_at=None,
                    heartbeat_at=None,
                )
            if error is not None:
                values["error_json"] = error
            result = connection.execute(
                update(schema.runs)
                .where(
                    schema.runs.c.id == lease.run.id,
                    schema.runs.c.fencing_token == lease.fencing_token,
                )
                .values(**values)
            )
            if result.rowcount != 1:
                raise StaleFenceError(f"stale fencing token for run {lease.run.id}")
            updated = self._run_row(connection, lease.run.id)
            self._append_event(
                connection,
                updated,
                f"run.{target.value}",
                {"state": target.value, "error": error, "retry_category": retry_category},
                now,
            )
            return _run_record(self._run_row(connection, lease.run.id))

    def transition_stage(
        self,
        lease: RunLease,
        stage_name: str,
        target: StageState,
    ) -> None:
        now = self._clock()
        with self._engine.begin() as connection:
            run = self._require_fence(connection, lease, now)
            stage_id = _stage_id(lease.run.id, stage_name, int(run["attempt"]))
            row = (
                connection.execute(select(schema.stages).where(schema.stages.c.id == stage_id))
                .mappings()
                .one_or_none()
            )
            if row is None:
                connection.execute(
                    insert(schema.stages).values(
                        id=stage_id,
                        run_id=lease.run.id,
                        name=stage_name,
                        state=StageState.PENDING.value,
                        attempt=run["attempt"],
                        created_at=now,
                    )
                )
                row = (
                    connection.execute(select(schema.stages).where(schema.stages.c.id == stage_id))
                    .mappings()
                    .one()
                )
            current = StageState(row["state"])
            require_stage_transition(current, target)
            values: dict[str, Any] = {"state": target.value}
            if target == StageState.RUNNING and row["started_at"] is None:
                values["started_at"] = now
            if target in {
                StageState.SUCCEEDED,
                StageState.FAILED,
                StageState.CANCELLED,
                StageState.SKIPPED,
            }:
                values["finished_at"] = now
            connection.execute(
                update(schema.stages).where(schema.stages.c.id == stage_id).values(**values)
            )
            self._append_event(
                connection,
                run,
                f"stage.{target.value}",
                {"stage": stage_name, "state": target.value},
                now,
                stage_id=stage_id,
            )

    def append_event(
        self,
        lease: RunLease,
        event_type: str,
        payload: dict[str, Any],
    ) -> StoredEvent:
        now = self._clock()
        with self._engine.begin() as connection:
            run = self._require_fence(connection, lease, now)
            return self._append_event(connection, run, event_type, payload, now)

    def commit_batch(
        self,
        lease: RunLease,
        envelope: BatchEnvelope,
        checkpoint: Checkpoint,
        artifact: StoredArtifact,
    ) -> BatchCommit:
        if envelope.run_id != lease.run.id or checkpoint.run_id != lease.run.id:
            raise MetadataError("batch, checkpoint, and lease must belong to the same run")
        if checkpoint.batch_id != envelope.batch_id:
            raise MetadataError("checkpoint batch_id must match the batch envelope")
        now = self._clock()
        with self._engine.begin() as connection:
            run = self._require_fence(connection, lease, now)
            stage_id = _stage_id(lease.run.id, envelope.stage_id, int(run["attempt"]))
            self._ensure_stage(connection, run, stage_id, envelope.stage_id, now)
            partition_id = _partition_id(lease.run.id, envelope.partition_id)
            self._ensure_partition(
                connection,
                lease.run.id,
                stage_id,
                partition_id,
                envelope.partition_id,
                now,
            )
            batch_id = _batch_id(lease.run.id, envelope.partition_id, envelope.batch_id)
            existing = (
                connection.execute(select(schema.batches).where(schema.batches.c.id == batch_id))
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                saved = BatchEnvelope.model_validate(existing["envelope_json"])
                if saved.output_checksum != envelope.output_checksum:
                    raise IdempotencyConflictError(
                        f"batch {envelope.batch_id} was recommitted with different output"
                    )
                return self._batch_commit(connection, existing)

            self._ensure_artifact(connection, artifact)
            envelope_json = envelope.model_dump(mode="json")
            connection.execute(
                insert(schema.batches).values(
                    id=batch_id,
                    run_id=lease.run.id,
                    partition_id=partition_id,
                    batch_key=envelope.batch_id,
                    state=BatchState.PENDING.value,
                    attempt=envelope.attempt,
                    envelope_json=envelope_json,
                    artifact_id=None,
                    created_at=now,
                )
            )
            require_batch_transition(BatchState.PENDING, BatchState.RUNNING)
            connection.execute(
                update(schema.batches)
                .where(schema.batches.c.id == batch_id)
                .values(state=BatchState.RUNNING.value)
            )
            require_batch_transition(BatchState.RUNNING, BatchState.COMMITTED)
            connection.execute(
                update(schema.batches)
                .where(schema.batches.c.id == batch_id)
                .values(
                    state=BatchState.COMMITTED.value,
                    artifact_id=artifact.id,
                    committed_at=checkpoint.committed_at,
                )
            )
            connection.execute(
                insert(schema.checkpoints).values(
                    id=checkpoint.checkpoint_id,
                    run_id=lease.run.id,
                    partition_id=partition_id,
                    batch_id=envelope.batch_id,
                    source_position_json=checkpoint.source_position,
                    fencing_token=lease.fencing_token,
                    committed_at=checkpoint.committed_at,
                )
            )
            event = self._append_event(
                connection,
                run,
                "batch.committed",
                {
                    "batch_id": envelope.batch_id,
                    "partition_id": envelope.partition_id,
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "artifact_id": artifact.id,
                    "rows_in": envelope.rows_in,
                    "rows_out": envelope.rows_out,
                },
                now,
                stage_id=stage_id,
                batch_id=envelope.batch_id,
            )
            connection.execute(
                insert(schema.outbox).values(
                    id=uuid.uuid4().hex,
                    aggregate_type="run",
                    aggregate_id=lease.run.id,
                    event_type="batch.committed",
                    payload_json={
                        "run_id": lease.run.id,
                        "sequence": event.sequence,
                        "batch_id": envelope.batch_id,
                    },
                    available_at=now,
                    attempts=0,
                )
            )
            row = (
                connection.execute(select(schema.batches).where(schema.batches.c.id == batch_id))
                .mappings()
                .one()
            )
            return self._batch_commit(connection, row)

    def list_batch_commits(self, run_id: str, partition_id: str) -> list[BatchCommit]:
        internal_partition_id = _partition_id(run_id, partition_id)
        with self._engine.connect() as connection:
            rows = connection.execute(
                select(schema.batches)
                .where(
                    schema.batches.c.run_id == run_id,
                    schema.batches.c.partition_id == internal_partition_id,
                    schema.batches.c.state == BatchState.COMMITTED.value,
                )
                .order_by(schema.batches.c.committed_at, schema.batches.c.batch_key)
            ).mappings()
            return [self._batch_commit(connection, row) for row in rows]

    def latest_checkpoint(self, run_id: str, partition_id: str) -> Checkpoint | None:
        internal_partition_id = _partition_id(run_id, partition_id)
        with self._engine.connect() as connection:
            row = (
                connection.execute(
                    select(schema.checkpoints)
                    .where(
                        schema.checkpoints.c.run_id == run_id,
                        schema.checkpoints.c.partition_id == internal_partition_id,
                    )
                    .order_by(schema.checkpoints.c.committed_at.desc())
                    .limit(1)
                )
                .mappings()
                .one_or_none()
            )
            return _checkpoint(row, partition_id) if row is not None else None

    def request_cancel(
        self,
        run_id: str,
        *,
        workspace_id: str | None = None,
        connection: Connection | None = None,
    ) -> RunRecord:
        now = self._clock()
        with self._transaction(connection) as connection:
            if workspace_id is not None:
                self._set_workspace_scope(connection, workspace_id)
            row = self._run_row(connection, run_id, for_update=True)
            state = RunState(row["state"])
            if state in {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}:
                return _run_record(row)
            values: dict[str, Any] = {"cancel_requested": True}
            if state in {RunState.QUEUED, RunState.RETRY_WAIT}:
                require_run_transition(state, RunState.CANCELLED)
                values.update(state=RunState.CANCELLED.value, finished_at=now)
            elif state in {RunState.CLAIMED, RunState.RUNNING}:
                require_run_transition(state, RunState.CANCELLING)
                values["state"] = RunState.CANCELLING.value
            connection.execute(
                update(schema.runs).where(schema.runs.c.id == run_id).values(**values)
            )
            updated = self._run_row(connection, run_id)
            self._append_event(
                connection,
                updated,
                "run.cancel_requested",
                {"state": updated["state"]},
                now,
            )
            return _run_record(self._run_row(connection, run_id))

    def cancellation_requested(self, run_id: str) -> bool:
        with self._engine.connect() as connection:
            value = connection.execute(
                select(schema.runs.c.cancel_requested).where(schema.runs.c.id == run_id)
            ).scalar_one_or_none()
            if value is None:
                raise MetadataError(f"run not found: {run_id}")
            return bool(value)

    def upsert_schedule(
        self,
        schedule: ScheduleRecord,
        *,
        connection: Connection | None = None,
    ) -> ScheduleRecord:
        with self._transaction(connection) as connection:
            self._set_workspace_scope(connection, schedule.workspace_id)
            existing = (
                connection.execute(
                    select(schema.schedules).where(schema.schedules.c.id == schedule.id)
                )
                .mappings()
                .one_or_none()
            )
            values = _schedule_values(schedule)
            if existing is None:
                connection.execute(insert(schema.schedules).values(**values))
            else:
                if existing["workspace_id"] != schedule.workspace_id:
                    raise IdempotencyConflictError("schedule id belongs to another workspace")
                connection.execute(
                    update(schema.schedules)
                    .where(schema.schedules.c.id == schedule.id)
                    .values(**values)
                )
            row = (
                connection.execute(
                    select(schema.schedules).where(schema.schedules.c.id == schedule.id)
                )
                .mappings()
                .one()
            )
            return _schedule(row)

    def enqueue_due_schedules(self, now: datetime) -> list[RunRecord]:
        created: list[RunRecord] = []
        with self._engine.begin() as connection:
            query = (
                select(schema.schedules)
                .where(
                    schema.schedules.c.enabled.is_(True),
                    schema.schedules.c.next_run_at <= now,
                )
                .order_by(schema.schedules.c.next_run_at, schema.schedules.c.id)
            )
            if self.profile == "postgresql":
                query = query.with_for_update(skip_locked=True)
            for item in connection.execute(query).mappings():
                due_at = _as_utc(item["next_run_at"])
                command_key = f"schedule:{item['id']}:{due_at.isoformat()}"
                run_id = hashlib.sha256(command_key.encode()).hexdigest()[:24]
                created.append(
                    self._create_run(
                        connection,
                        workspace_id=item["workspace_id"],
                        pipeline_version_id=item["pipeline_version_id"],
                        command_key=command_key,
                        run_id=run_id,
                        parent_run_id=None,
                        retry_category=None,
                        now=now,
                    )
                )
                connection.execute(
                    update(schema.schedules)
                    .where(schema.schedules.c.id == item["id"])
                    .values(next_run_at=_next_fire(item, due_at), updated_at=now)
                )
        return created

    def list_events(
        self, run_id: str, after: int = 0, *, workspace_id: str | None = None
    ) -> list[StoredEvent]:
        with self._engine.connect() as connection:
            if workspace_id is not None:
                self._set_workspace_scope(connection, workspace_id)
            rows = connection.execute(
                select(schema.run_events)
                .where(
                    schema.run_events.c.run_id == run_id,
                    schema.run_events.c.sequence > after,
                )
                .order_by(schema.run_events.c.sequence)
            ).mappings()
            return [_stored_event(row) for row in rows]

    def _set_workspace_scope(self, connection: Connection, workspace_id: str) -> None:
        if self.profile == "postgresql":
            connection.execute(
                text("SELECT set_config('loafer.workspace_id', :workspace_id, true)"),
                {"workspace_id": workspace_id},
            )

    def pending_outbox(self, limit: int = 100) -> list[OutboxRecord]:
        with self._engine.connect() as connection:
            rows = connection.execute(
                select(schema.outbox)
                .where(
                    schema.outbox.c.published_at.is_(None),
                    schema.outbox.c.available_at <= self._clock(),
                )
                .order_by(schema.outbox.c.available_at, schema.outbox.c.id)
                .limit(limit)
            ).mappings()
            return [_outbox(row) for row in rows]

    def mark_outbox_published(self, outbox_id: str, published_at: datetime) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                update(schema.outbox)
                .where(
                    schema.outbox.c.id == outbox_id,
                    schema.outbox.c.published_at.is_(None),
                )
                .values(published_at=published_at)
            )

    def _create_run(
        self,
        connection: Connection,
        *,
        workspace_id: str,
        pipeline_version_id: str,
        command_key: str,
        run_id: str,
        parent_run_id: str | None,
        retry_category: RetryCategory | None,
        now: datetime,
    ) -> RunRecord:
        existing = (
            connection.execute(
                select(schema.runs).where(
                    schema.runs.c.workspace_id == workspace_id,
                    schema.runs.c.command_key == command_key,
                )
            )
            .mappings()
            .one_or_none()
        )
        if existing is not None:
            if existing["pipeline_version_id"] != pipeline_version_id:
                raise IdempotencyConflictError(
                    "run command key was reused for another pipeline version"
                )
            return _run_record(existing)
        try:
            connection.execute(
                insert(schema.runs).values(
                    id=run_id,
                    workspace_id=workspace_id,
                    pipeline_version_id=pipeline_version_id,
                    command_key=command_key,
                    state=RunState.QUEUED.value,
                    attempt=0,
                    retry_category=retry_category.value if retry_category else None,
                    cancel_requested=False,
                    next_event_sequence=1,
                    fencing_token=0,
                    created_at=now,
                    parent_run_id=parent_run_id,
                )
            )
        except IntegrityError as exc:
            raise MetadataError(f"could not create run {run_id}: {exc}") from exc
        row = self._run_row(connection, run_id)
        event = self._append_event(
            connection,
            row,
            "run.created",
            {"pipeline_version_id": pipeline_version_id},
            now,
        )
        connection.execute(
            insert(schema.outbox).values(
                id=uuid.uuid4().hex,
                aggregate_type="run",
                aggregate_id=run_id,
                event_type="run.created",
                payload_json={"run_id": run_id, "sequence": event.sequence},
                available_at=now,
                attempts=0,
            )
        )
        return _run_record(self._run_row(connection, run_id))

    def _run_row(
        self,
        connection: Connection,
        run_id: str,
        *,
        for_update: bool = False,
    ) -> RowMapping:
        query = select(schema.runs).where(schema.runs.c.id == run_id)
        if for_update and self.profile == "postgresql":
            query = query.with_for_update()
        row = connection.execute(query).mappings().one_or_none()
        if row is None:
            raise MetadataError(f"run not found: {run_id}")
        return row

    def _require_fence(
        self,
        connection: Connection,
        lease: RunLease,
        now: datetime,
    ) -> RowMapping:
        row = self._run_row(connection, lease.run.id, for_update=True)
        expires_at = row["lease_expires_at"]
        valid = (
            row["lease_owner"] == lease.worker_id
            and int(row["fencing_token"]) == lease.fencing_token
            and row["state"] in _ACTIVE_STATES
            and expires_at is not None
            and _as_utc(expires_at) > now
        )
        if not valid:
            raise StaleFenceError(
                f"stale or expired fencing token {lease.fencing_token} for run {lease.run.id}"
            )
        return row

    def _append_event(
        self,
        connection: Connection,
        run: RowMapping,
        event_type: str,
        payload: dict[str, Any],
        occurred_at: datetime,
        *,
        stage_id: str | None = None,
        batch_id: str | None = None,
    ) -> StoredEvent:
        sequence = int(run["next_event_sequence"])
        event_id = _stable_id("event", str(run["id"]), str(sequence))
        connection.execute(
            insert(schema.run_events).values(
                id=event_id,
                run_id=run["id"],
                sequence=sequence,
                event_type=event_type,
                stage_id=stage_id,
                batch_id=batch_id,
                payload_json=_json(payload),
                occurred_at=occurred_at,
            )
        )
        result = connection.execute(
            update(schema.runs)
            .where(
                schema.runs.c.id == run["id"],
                schema.runs.c.next_event_sequence == sequence,
            )
            .values(next_event_sequence=sequence + 1)
        )
        if result.rowcount != 1:
            raise MetadataError(f"event sequence allocation raced for run {run['id']}")
        return StoredEvent(run["id"], sequence, event_type, _json(payload), occurred_at)

    @staticmethod
    def _ensure_stage(
        connection: Connection,
        run: RowMapping,
        stage_id: str,
        stage_name: str,
        now: datetime,
    ) -> None:
        exists = connection.execute(
            select(schema.stages.c.id).where(schema.stages.c.id == stage_id)
        ).scalar_one_or_none()
        if exists is None:
            require_stage_transition(StageState.PENDING, StageState.RUNNING)
            connection.execute(
                insert(schema.stages).values(
                    id=stage_id,
                    run_id=run["id"],
                    name=stage_name,
                    state=StageState.RUNNING.value,
                    attempt=run["attempt"],
                    created_at=now,
                    started_at=now,
                )
            )

    @staticmethod
    def _ensure_partition(
        connection: Connection,
        run_id: str,
        stage_id: str,
        partition_id: str,
        partition_key: str,
        now: datetime,
    ) -> None:
        exists = connection.execute(
            select(schema.partitions.c.id).where(schema.partitions.c.id == partition_id)
        ).scalar_one_or_none()
        if exists is None:
            connection.execute(
                insert(schema.partitions).values(
                    id=partition_id,
                    run_id=run_id,
                    stage_id=stage_id,
                    partition_key=partition_key,
                    created_at=now,
                )
            )

    @staticmethod
    def _ensure_artifact(connection: Connection, artifact: StoredArtifact) -> None:
        row = (
            connection.execute(select(schema.artifacts).where(schema.artifacts.c.id == artifact.id))
            .mappings()
            .one_or_none()
        )
        if row is None:
            connection.execute(
                insert(schema.artifacts).values(
                    id=artifact.id,
                    run_id=artifact.run_id,
                    kind=artifact.kind,
                    uri=artifact.uri,
                    checksum=artifact.checksum,
                    size_bytes=artifact.size_bytes,
                    metadata_json=artifact.metadata,
                    created_at=artifact.created_at,
                )
            )
        elif row["checksum"] != artifact.checksum:
            raise IdempotencyConflictError(f"artifact id {artifact.id} has conflicting content")

    @staticmethod
    def _batch_commit(connection: Connection, row: RowMapping) -> BatchCommit:
        envelope = BatchEnvelope.model_validate(row["envelope_json"])
        checkpoint_row = (
            connection.execute(
                select(schema.checkpoints).where(
                    schema.checkpoints.c.run_id == row["run_id"],
                    schema.checkpoints.c.partition_id == row["partition_id"],
                    schema.checkpoints.c.batch_id == row["batch_key"],
                )
            )
            .mappings()
            .one()
        )
        artifact_row = (
            connection.execute(
                select(schema.artifacts).where(schema.artifacts.c.id == row["artifact_id"])
            )
            .mappings()
            .one()
        )
        return BatchCommit(
            envelope,
            _checkpoint(checkpoint_row, envelope.partition_id),
            _artifact(artifact_row),
        )


def _enable_sqlite_foreign_keys(dbapi_connection: Any, _connection_record: Any) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def _pipeline_version(row: RowMapping) -> PipelineVersion:
    return PipelineVersion(
        id=row["id"],
        workspace_id=row["workspace_id"],
        pipeline_key=row["pipeline_key"],
        config_digest=row["config_digest"],
        config=dict(row["config_json"]),
        created_at=_as_utc(row["created_at"]),
    )


def _run_record(row: RowMapping) -> RunRecord:
    retry = row["retry_category"]
    return RunRecord(
        id=row["id"],
        workspace_id=row["workspace_id"],
        pipeline_version_id=row["pipeline_version_id"],
        command_key=row["command_key"],
        state=RunState(row["state"]),
        attempt=int(row["attempt"]),
        retry_category=RetryCategory(retry) if retry else None,
        cancel_requested=bool(row["cancel_requested"]),
        fencing_token=int(row["fencing_token"]),
        lease_owner=row["lease_owner"],
        lease_expires_at=_optional_utc(row["lease_expires_at"]),
        heartbeat_at=_optional_utc(row["heartbeat_at"]),
        created_at=_as_utc(row["created_at"]),
        started_at=_optional_utc(row["started_at"]),
        finished_at=_optional_utc(row["finished_at"]),
        parent_run_id=row["parent_run_id"],
        error=dict(row["error_json"]) if row["error_json"] else None,
    )


def _stored_event(row: RowMapping) -> StoredEvent:
    return StoredEvent(
        run_id=row["run_id"],
        sequence=int(row["sequence"]),
        event_type=row["event_type"],
        payload=dict(row["payload_json"]),
        occurred_at=_as_utc(row["occurred_at"]),
    )


def _artifact(row: RowMapping) -> StoredArtifact:
    return StoredArtifact(
        id=row["id"],
        run_id=row["run_id"],
        kind=row["kind"],
        uri=row["uri"],
        checksum=row["checksum"],
        size_bytes=int(row["size_bytes"]),
        metadata=dict(row["metadata_json"]),
        created_at=_as_utc(row["created_at"]),
    )


def _checkpoint(row: RowMapping, partition_key: str) -> Checkpoint:
    return Checkpoint(
        checkpoint_id=row["id"],
        run_id=row["run_id"],
        partition_id=partition_key,
        batch_id=row["batch_id"],
        source_position=row["source_position_json"],
        committed_at=_as_utc(row["committed_at"]),
    )


def _schedule(row: RowMapping) -> ScheduleRecord:
    return ScheduleRecord(
        id=row["id"],
        workspace_id=row["workspace_id"],
        pipeline_version_id=row["pipeline_version_id"],
        trigger_kind=row["trigger_kind"],
        trigger_spec=row["trigger_spec"],
        timezone=row["timezone"],
        enabled=bool(row["enabled"]),
        next_run_at=_as_utc(row["next_run_at"]),
        created_at=_as_utc(row["created_at"]),
        updated_at=_as_utc(row["updated_at"]),
    )


def _schedule_values(item: ScheduleRecord) -> dict[str, Any]:
    return {
        "id": item.id,
        "workspace_id": item.workspace_id,
        "pipeline_version_id": item.pipeline_version_id,
        "trigger_kind": item.trigger_kind,
        "trigger_spec": item.trigger_spec,
        "timezone": item.timezone,
        "enabled": item.enabled,
        "next_run_at": item.next_run_at,
        "created_at": item.created_at,
        "updated_at": item.updated_at,
    }


def _outbox(row: RowMapping) -> OutboxRecord:
    return OutboxRecord(
        id=row["id"],
        aggregate_type=row["aggregate_type"],
        aggregate_id=row["aggregate_id"],
        event_type=row["event_type"],
        payload=dict(row["payload_json"]),
        available_at=_as_utc(row["available_at"]),
        published_at=_optional_utc(row["published_at"]),
        attempts=int(row["attempts"]),
    )


def _stage_id(run_id: str, name: str, attempt: int) -> str:
    return _stable_id("stage", run_id, name, str(attempt))


def _partition_id(run_id: str, partition_key: str) -> str:
    return _stable_id("partition", run_id, partition_key)


def _batch_id(run_id: str, partition_key: str, batch_key: str) -> str:
    return _stable_id("batch", run_id, partition_key, batch_key)


def _stable_id(kind: str, *parts: str) -> str:
    digest = hashlib.sha256("\0".join(parts).encode()).hexdigest()[:40]
    return f"{kind}-{digest}"


def _json(value: Any) -> Any:
    if isinstance(value, RetryCategory):
        return value.value
    if isinstance(value, dict):
        return {str(key): _json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _optional_utc(value: datetime | None) -> datetime | None:
    return _as_utc(value) if value is not None else None


def _next_fire(schedule: RowMapping, previous: datetime) -> datetime:
    if schedule["trigger_kind"] == "interval":
        return previous + _parse_interval(schedule["trigger_spec"])
    from apscheduler.triggers.cron import CronTrigger

    trigger = CronTrigger.from_crontab(schedule["trigger_spec"], timezone=schedule["timezone"])
    next_fire = trigger.get_next_fire_time(previous, previous + timedelta(microseconds=1))
    if next_fire is None:
        raise MetadataError(f"schedule {schedule['id']} has no next fire time")
    return _as_utc(next_fire)


def _parse_interval(spec: str) -> timedelta:
    units = {
        "s": "seconds",
        "m": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
    }
    if len(spec) < 2 or spec[-1] not in units:
        raise MetadataError(f"invalid interval: {spec}")
    try:
        value = int(spec[:-1])
    except ValueError as exc:
        raise MetadataError(f"invalid interval: {spec}") from exc
    if value <= 0:
        raise MetadataError(f"invalid interval: {spec}")
    return timedelta(**{units[spec[-1]]: value})
