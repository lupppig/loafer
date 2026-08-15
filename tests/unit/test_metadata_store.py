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
from loafer.application import durable as durable_application
from loafer.application.durable import get_durable_worker
from loafer.contracts import BatchEnvelope
from loafer.core.run_state import RunState
from loafer.exceptions import IdempotencyConflictError, MetadataError, StaleFenceError
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
    assert metadata.migrate() == schema.LATEST_SCHEMA_VERSION
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

        assert metadata.migrate() == schema.LATEST_SCHEMA_VERSION
        assert metadata.get_pipeline_version(version_id).pipeline_key == "customers"
        assert "loafer_outbox" in inspect(metadata.engine).get_table_names()
        assert "role" in _columns(metadata, "loafer_runs")

        assert metadata.migrate(1) == 1
        assert "loafer_outbox" not in inspect(metadata.engine).get_table_names()
        assert metadata.get_pipeline_version(version_id).pipeline_key == "customers"

        assert metadata.migrate() == schema.LATEST_SCHEMA_VERSION
        assert "loafer_outbox" in inspect(metadata.engine).get_table_names()
        assert "role" in _columns(metadata, "loafer_runs")
    finally:
        metadata.close()


def test_v4_columns_and_indexes_round_trip(tmp_path: Path) -> None:
    metadata = SqlMetadataStore(f"sqlite:///{tmp_path / 'v4.db'}")
    try:
        metadata.migrate()
        assert {"role", "environment_id", "quarantined"} <= _columns(metadata, "loafer_runs")
        assert {"role", "claimed_until", "last_error"} <= _columns(metadata, "loafer_outbox")
        assert "max_concurrent_runs" in _columns(metadata, "loafer_workspaces")
        assert "max_concurrent_runs" in _columns(metadata, "loafer_environments")
        assert "ix_loafer_runs_tenant_active" in _indexes(metadata, "loafer_runs")

        assert metadata.migrate(3) == 3
        assert not {"role", "environment_id", "quarantined"} & _columns(metadata, "loafer_runs")
        assert "ix_loafer_runs_tenant_active" not in _indexes(metadata, "loafer_runs")

        assert metadata.migrate() == schema.LATEST_SCHEMA_VERSION
        assert {"role", "environment_id", "quarantined"} <= _columns(metadata, "loafer_runs")
    finally:
        metadata.close()


def _columns(metadata: SqlMetadataStore, table_name: str) -> set[str]:
    return {column["name"] for column in inspect(metadata.engine).get_columns(table_name)}


def _indexes(metadata: SqlMetadataStore, table_name: str) -> set[str]:
    return {index["name"] for index in inspect(metadata.engine).get_indexes(table_name)}


def test_schema_verification_rejects_older_and_newer_versions_without_migrating(
    tmp_path: Path,
) -> None:
    metadata = SqlMetadataStore(f"sqlite:///{tmp_path / 'version-check.db'}")
    try:
        latest = schema.LATEST_SCHEMA_VERSION
        unsupported = latest + 1

        metadata.migrate(1)
        with pytest.raises(MetadataError, match=rf"version 1.*expected {latest}"):
            metadata.verify_schema()
        assert metadata.current_schema_version() == 1

        metadata.migrate()
        with metadata.engine.begin() as connection:
            connection.execute(schema.schema_migrations.insert().values(version=unsupported))
        with pytest.raises(MetadataError, match=rf"version {unsupported}.*expected {latest}"):
            metadata.verify_schema()
        assert metadata.current_schema_version() == unsupported
        with pytest.raises(
            MetadataError,
            match=rf"version {unsupported} is newer.*supports \({latest}\)",
        ):
            metadata.migrate()
        assert metadata.current_schema_version() == unsupported
    finally:
        metadata.close()


def test_durable_worker_composition_rejects_unmigrated_schema(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'worker.db'}"

    with pytest.raises(MetadataError, match=r"version 0.*loafer metadata migrate"):
        get_durable_worker(
            worker_id="worker-1",
            metadata_url=database_url,
            object_root=tmp_path / "objects",
        )

    metadata = SqlMetadataStore(database_url)
    try:
        assert metadata.current_schema_version() == 0
    finally:
        metadata.close()


def test_durable_worker_closes_resources_when_consumer_creation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Metadata:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    class Transport:
        def __init__(self) -> None:
            self.closed = False

        def consumer(self, role: object, *, max_ack_pending: int) -> object:
            del role, max_ack_pending
            raise RuntimeError("consumer setup failed")

        def close(self) -> None:
            self.closed = True

    metadata = Metadata()
    transport = Transport()
    monkeypatch.setenv("LOAFER_NATS_URL", "nats://nats:4222")
    monkeypatch.setattr(durable_application, "_get_ready_metadata_store", lambda _url: metadata)
    monkeypatch.setattr(
        durable_application,
        "_get_nats_transport",
        lambda _url, *, manage_stream: transport,
    )

    with pytest.raises(RuntimeError, match="consumer setup failed"):
        get_durable_worker(
            worker_id="worker-1",
            metadata_url="sqlite:///unused.db",
            object_root=tmp_path / "objects",
        )

    assert transport.closed is True
    assert metadata.closed is True


def test_nats_transport_reads_compose_secret_without_putting_it_in_the_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    password_file = tmp_path / "nats-password"
    password_file.write_text("worker-password\n", encoding="utf-8")
    captured: dict[str, object] = {}

    def _transport(url: str, **kwargs: object) -> object:
        captured["url"] = url
        captured.update(kwargs)
        return object()

    monkeypatch.setenv("LOAFER_NATS_USER", "loafer-etl")
    monkeypatch.setenv("LOAFER_NATS_PASSWORD_FILE", str(password_file))
    monkeypatch.setattr(durable_application, "JetStreamTransport", _transport)

    durable_application._get_nats_transport(
        "nats://loafer-etl@nats:4222",
        manage_stream=False,
    )

    assert captured == {
        "url": "nats://loafer-etl@nats:4222",
        "user": "loafer-etl",
        "password": "worker-password",
        "manage_stream": False,
    }


def test_nats_transport_rejects_incomplete_auth_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOAFER_NATS_USER", "loafer-etl")
    monkeypatch.delenv("LOAFER_NATS_PASSWORD_FILE", raising=False)

    with pytest.raises(ValueError, match="must be configured together"):
        durable_application._get_nats_transport("nats://nats:4222", manage_stream=False)


def test_registered_pipeline_preserves_secret_references_not_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loafer.application.durable import register_pipeline_config

    source = tmp_path / "input.csv"
    source.write_text("id\n1\n", encoding="utf-8")
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        "\n".join(
            [
                "source:",
                "  type: csv",
                f"  path: {source}",
                "target:",
                "  type: json",
                f"  path: {tmp_path / 'output.json'}",
                "transform:",
                "  type: custom",
                f"  path: {tmp_path / 'transform.py'}",
                "llm:",
                "  provider: openai",
                "  api_key: ${JOB_OPENAI_KEY}",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "transform.py").write_text(
        "def transform(data):\n    return data\n", encoding="utf-8"
    )
    monkeypatch.setenv("JOB_OPENAI_KEY", "do-not-store-this-value")
    database_url = f"sqlite:///{tmp_path / 'metadata.db'}"
    metadata = SqlMetadataStore(database_url)
    metadata.migrate()
    metadata.close()

    version = register_pipeline_config(config, metadata_url=database_url)

    rendered = str(version.config)
    assert "do-not-store-this-value" not in rendered
    assert "${JOB_OPENAI_KEY}" in rendered
    assert version.config["secret_references"] == ["JOB_OPENAI_KEY"]


def test_secret_restoration_follows_the_matching_raw_structure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("JOB_SECRET", "prod")
    raw = {
        "credential": "${JOB_SECRET}",
        "description": "production",
        "paths": ["./${JOB_SECRET}/input.csv", "production"],
    }
    resolved = {
        "credential": "prod",
        "description": "production",
        "paths": ["/srv/pipelines/prod/input.csv", "production"],
        "defaulted": "prod",
    }

    restored = durable_application._restore_secret_references(raw, resolved)

    assert restored == {
        "credential": "${JOB_SECRET}",
        "description": "production",
        "paths": ["/srv/pipelines/${JOB_SECRET}/input.csv", "production"],
        "defaulted": "prod",
    }


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


def test_hidden_schedule_id_conflict_is_translated(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, clock = store
    schedule = ScheduleRecord(
        id="shared-schedule",
        workspace_id="workspace-2",
        pipeline_version_id="pipeline-2",
        trigger_kind="cron",
        trigger_spec="0 0 * * *",
        timezone="UTC",
        enabled=True,
        next_run_at=clock(),
        created_at=clock(),
        updated_at=clock(),
    )

    class MissingResult:
        def mappings(self) -> MissingResult:
            return self

        def one_or_none(self) -> None:
            return None

    class PrimaryKeyViolationError(Exception):
        diag = type("Diagnostic", (), {"constraint_name": "loafer_schedules_pkey"})()

    class ScopedConnection:
        def execute(self, statement: object) -> MissingResult:
            if getattr(statement, "is_select", False):
                return MissingResult()
            raise IntegrityError("insert schedule", {}, PrimaryKeyViolationError())

    with pytest.raises(IdempotencyConflictError, match="another workspace"):
        metadata.upsert_schedule(schedule, connection=ScopedConnection())  # type: ignore[arg-type]


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
