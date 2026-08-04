from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import inspect, select
from sqlalchemy.exc import IntegrityError

from loafer.adapters import metadata_schema as schema
from loafer.adapters.metadata import SqlMetadataStore
from loafer.adapters.object_storage import MemoryObjectStorage
from loafer.adapters.runtime import DurableBatchRecovery
from loafer.contracts import BatchEnvelope
from loafer.core.run_state import RunState
from loafer.exceptions import IdempotencyConflictError, StaleFenceError
from loafer.metadata import ScheduleRecord


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 4, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs: int) -> None:
        self.now += timedelta(**kwargs)


@pytest.fixture()
def store(tmp_path: Path) -> tuple[SqlMetadataStore, Clock]:
    clock = Clock()
    metadata = SqlMetadataStore(f"sqlite:///{tmp_path / 'metadata.db'}", clock=clock)
    assert metadata.migrate() == 2
    try:
        yield metadata, clock
    finally:
        metadata.close()


def _version(metadata: SqlMetadataStore) -> str:
    return metadata.register_pipeline_version(
        workspace_id="workspace-1",
        pipeline_key="customers",
        config_digest="a" * 64,
        config={"document": {"name": "customers"}},
    ).id


def _envelope(run_id: str, batch_id: str = "batch-00000001") -> BatchEnvelope:
    return BatchEnvelope(
        run_id=run_id,
        stage_id="load",
        partition_id="default",
        batch_id=batch_id,
        attempt=0,
        source_position_start={"offset": 0},
        source_position_end={"offset": 1},
        schema_version="schema-1",
        rows_in=2,
        rows_out=2,
        rows_rejected=0,
        bytes_in=10,
        bytes_out=10,
        output_checksum="b" * 64,
    )


def test_run_creation_is_idempotent_and_conflicts_are_rejected(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    version_id = _version(metadata)

    first = metadata.create_run(
        workspace_id="workspace-1",
        pipeline_version_id=version_id,
        command_key="request-1",
        run_id="run-1",
    )
    repeated = metadata.create_run(
        workspace_id="workspace-1",
        pipeline_version_id=version_id,
        command_key="request-1",
        run_id="ignored",
    )

    assert first == repeated
    assert first.state is RunState.QUEUED
    assert [event.sequence for event in metadata.list_events("run-1")] == [1]

    other_version = metadata.register_pipeline_version(
        workspace_id="workspace-1",
        pipeline_key="customers",
        config_digest="c" * 64,
        config={"document": {"name": "changed"}},
    )
    with pytest.raises(IdempotencyConflictError):
        metadata.create_run(
            workspace_id="workspace-1",
            pipeline_version_id=other_version.id,
            command_key="request-1",
        )


def test_batch_commit_is_replayable_and_event_sequences_are_monotonic(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    run = metadata.create_run(
        workspace_id="workspace-1",
        pipeline_version_id=_version(metadata),
        command_key="request-1",
        run_id="run-1",
    )
    lease = metadata.claim_run("worker-a", timedelta(seconds=30))
    assert lease is not None and lease.run.id == run.id
    metadata.transition_run(lease, RunState.RUNNING)
    recovery = DurableBatchRecovery(metadata, MemoryObjectStorage(), lease)
    envelope = _envelope(run.id)

    checkpoint = recovery.commit(envelope, [{"id": 1}, {"id": 2}])
    repeated = recovery.commit(envelope, [{"id": 1}, {"id": 2}])
    commits = recovery.restore(run.id, "default")

    assert repeated == checkpoint
    assert metadata.latest_checkpoint(run.id, "default") == checkpoint
    assert recovery.read_rows(commits[0]) == [{"id": 1}, {"id": 2}]
    sequences = [event.sequence for event in metadata.list_events(run.id)]
    assert sequences == list(range(1, len(sequences) + 1))
    assert [event.event_type for event in metadata.list_events(run.id)].count(
        "batch.committed"
    ) == 1


def test_expired_worker_is_fenced_after_reclaim(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, clock = store
    metadata.create_run(
        workspace_id="workspace-1",
        pipeline_version_id=_version(metadata),
        command_key="request-1",
        run_id="run-1",
    )
    stale = metadata.claim_run("worker-a", timedelta(seconds=5))
    assert stale is not None
    metadata.transition_run(stale, RunState.RUNNING)

    clock.advance(seconds=6)
    current = metadata.claim_run("worker-b", timedelta(seconds=30))
    assert current is not None
    assert current.fencing_token == stale.fencing_token + 1

    with pytest.raises(StaleFenceError):
        metadata.append_event(stale, "worker.late", {})
    with pytest.raises(StaleFenceError):
        DurableBatchRecovery(metadata, MemoryObjectStorage(), stale).commit(
            _envelope("run-1"), [{"id": 1}]
        )


def test_migrations_upgrade_previous_schema_rollback_and_reapply(tmp_path: Path) -> None:
    metadata = SqlMetadataStore(f"sqlite:///{tmp_path / 'migration.db'}")
    try:
        assert metadata.migrate(1) == 1
        version_id = _version(metadata)

        assert metadata.migrate() == 2
        assert metadata.get_pipeline_version(version_id).pipeline_key == "customers"
        assert "loafer_outbox" in inspect(metadata.engine).get_table_names()

        assert metadata.migrate(1) == 1
        assert "loafer_outbox" not in inspect(metadata.engine).get_table_names()
        assert metadata.get_pipeline_version(version_id).pipeline_key == "customers"

        assert metadata.migrate() == 2
        assert "loafer_outbox" in inspect(metadata.engine).get_table_names()
    finally:
        metadata.close()


def test_database_constraints_enforce_null_unique_foreign_key_and_check(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, clock = store
    version_id = _version(metadata)

    with (
        pytest.raises(IntegrityError, match=r"NOT NULL constraint failed|not-null constraint"),
        metadata.engine.begin() as connection,
    ):
        connection.execute(
            schema.pipeline_versions.insert().values(
                id="missing-key",
                workspace_id="workspace-1",
                config_digest="d" * 64,
                config_json={},
                created_at=clock(),
            )
        )

    with (
        pytest.raises(IntegrityError, match=r"UNIQUE constraint failed|unique constraint"),
        metadata.engine.begin() as connection,
    ):
        connection.execute(
            schema.pipeline_versions.insert().values(
                id="duplicate-version",
                workspace_id="workspace-1",
                pipeline_key="customers",
                config_digest="a" * 64,
                config_json={},
                created_at=clock(),
            )
        )

    with (
        pytest.raises(
            IntegrityError, match=r"FOREIGN KEY constraint failed|foreign key constraint"
        ),
        metadata.engine.begin() as connection,
    ):
        connection.execute(
            schema.runs.insert().values(
                id="dangling",
                workspace_id="workspace-1",
                pipeline_version_id="does-not-exist",
                command_key="dangling",
                state=RunState.QUEUED.value,
                attempt=0,
                cancel_requested=False,
                next_event_sequence=1,
                fencing_token=0,
                created_at=clock(),
            )
        )

    with (
        pytest.raises(IntegrityError, match=r"ck_loafer_run_state|CHECK constraint failed"),
        metadata.engine.begin() as connection,
    ):
        connection.execute(
            schema.runs.insert().values(
                id="invalid",
                workspace_id="workspace-1",
                pipeline_version_id=version_id,
                command_key="invalid",
                state="teleported",
                attempt=0,
                cancel_requested=False,
                next_event_sequence=1,
                fencing_token=0,
                created_at=clock(),
            )
        )

    with metadata.engine.connect() as connection:
        assert (
            connection.execute(
                select(schema.runs.c.id).where(schema.runs.c.id == "invalid")
            ).scalar_one_or_none()
            is None
        )


def test_due_schedule_creates_one_idempotent_command_and_advances(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, clock = store
    version_id = _version(metadata)
    schedule = ScheduleRecord(
        id="hourly-customers",
        workspace_id="workspace-1",
        pipeline_version_id=version_id,
        trigger_kind="interval",
        trigger_spec="1h",
        timezone="UTC",
        enabled=True,
        next_run_at=clock(),
        created_at=clock(),
        updated_at=clock(),
    )
    metadata.upsert_schedule(schedule)

    first = metadata.enqueue_due_schedules(clock())
    repeated = metadata.enqueue_due_schedules(clock())

    assert len(first) == 1
    assert repeated == []
    assert first[0].command_key == f"schedule:hourly-customers:{clock().isoformat()}"
    assert [item.event_type for item in metadata.pending_outbox()] == ["run.created"]


def test_cancel_command_is_idempotent_before_claim(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    run = metadata.create_run(
        workspace_id="workspace-1",
        pipeline_version_id=_version(metadata),
        command_key="request-cancel",
        run_id="cancel-me",
    )

    cancelled = metadata.request_cancel(run.id)
    repeated = metadata.request_cancel(run.id)

    assert cancelled.state is RunState.CANCELLED
    assert repeated.state is RunState.CANCELLED
    assert metadata.claim_run("worker-a", timedelta(seconds=30)) is None
