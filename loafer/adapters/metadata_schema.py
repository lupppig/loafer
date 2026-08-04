"""Versioned SQLAlchemy Core migrations for Loafer metadata."""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    delete,
    func,
    insert,
    select,
)
from sqlalchemy.engine import Connection, Engine

from loafer.core.run_state import BatchState, RetryCategory, RunState, StageState
from loafer.exceptions import MetadataError

LATEST_SCHEMA_VERSION = 2
metadata = MetaData()


def _values(enum: type[object]) -> str:
    return ", ".join(f"'{item.value}'" for item in enum)  # type: ignore[attr-defined]


schema_migrations = Table(
    "loafer_schema_migrations",
    metadata,
    Column("version", Integer, primary_key=True),
)

pipeline_versions = Table(
    "loafer_pipeline_versions",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("workspace_id", String(64), nullable=False),
    Column("pipeline_key", String(255), nullable=False),
    Column("config_digest", String(64), nullable=False),
    Column("config_json", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "workspace_id",
        "pipeline_key",
        "config_digest",
        name="uq_loafer_pipeline_version_digest",
    ),
)

runs = Table(
    "loafer_runs",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("workspace_id", String(64), nullable=False),
    Column(
        "pipeline_version_id",
        String(64),
        ForeignKey("loafer_pipeline_versions.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("command_key", String(255), nullable=False),
    Column("state", String(32), nullable=False),
    Column("attempt", Integer, nullable=False, default=0),
    Column("retry_category", String(32)),
    Column("retry_at", DateTime(timezone=True)),
    Column("cancel_requested", Boolean, nullable=False, default=False),
    Column("next_event_sequence", BigInteger, nullable=False, default=1),
    Column("lease_owner", String(255)),
    Column("fencing_token", BigInteger, nullable=False, default=0),
    Column("lease_expires_at", DateTime(timezone=True)),
    Column("heartbeat_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("parent_run_id", String(64), ForeignKey("loafer_runs.id", ondelete="SET NULL")),
    Column("error_json", JSON),
    UniqueConstraint("workspace_id", "command_key", name="uq_loafer_run_command"),
    CheckConstraint(f"state IN ({_values(RunState)})", name="ck_loafer_run_state"),
    CheckConstraint(
        f"retry_category IS NULL OR retry_category IN ({_values(RetryCategory)})",
        name="ck_loafer_retry_category",
    ),
    CheckConstraint("attempt >= 0", name="ck_loafer_run_attempt"),
    CheckConstraint("fencing_token >= 0", name="ck_loafer_run_fence"),
)

stages = Table(
    "loafer_stages",
    metadata,
    Column("id", String(96), primary_key=True),
    Column("run_id", String(64), ForeignKey("loafer_runs.id", ondelete="CASCADE"), nullable=False),
    Column("name", String(128), nullable=False),
    Column("state", String(32), nullable=False),
    Column("attempt", Integer, nullable=False, default=0),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    UniqueConstraint("run_id", "name", "attempt", name="uq_loafer_stage_attempt"),
    CheckConstraint(f"state IN ({_values(StageState)})", name="ck_loafer_stage_state"),
)

partitions = Table(
    "loafer_partitions",
    metadata,
    Column("id", String(96), primary_key=True),
    Column("run_id", String(64), ForeignKey("loafer_runs.id", ondelete="CASCADE"), nullable=False),
    Column(
        "stage_id", String(96), ForeignKey("loafer_stages.id", ondelete="CASCADE"), nullable=False
    ),
    Column("partition_key", String(255), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("run_id", "partition_key", name="uq_loafer_partition_key"),
)

artifacts = Table(
    "loafer_artifacts",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("run_id", String(64), ForeignKey("loafer_runs.id", ondelete="CASCADE")),
    Column("kind", String(64), nullable=False),
    Column("uri", Text, nullable=False),
    Column("checksum", String(64), nullable=False),
    Column("size_bytes", BigInteger, nullable=False),
    Column("metadata_json", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("uri", name="uq_loafer_artifact_uri"),
    CheckConstraint("size_bytes >= 0", name="ck_loafer_artifact_size"),
)

batches = Table(
    "loafer_batches",
    metadata,
    Column("id", String(128), primary_key=True),
    Column("run_id", String(64), ForeignKey("loafer_runs.id", ondelete="CASCADE"), nullable=False),
    Column(
        "partition_id",
        String(96),
        ForeignKey("loafer_partitions.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("batch_key", String(128), nullable=False),
    Column("state", String(32), nullable=False),
    Column("attempt", Integer, nullable=False),
    Column("envelope_json", JSON, nullable=False),
    Column("artifact_id", String(64), ForeignKey("loafer_artifacts.id", ondelete="RESTRICT")),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("committed_at", DateTime(timezone=True)),
    UniqueConstraint("run_id", "partition_id", "batch_key", name="uq_loafer_batch_key"),
    CheckConstraint(f"state IN ({_values(BatchState)})", name="ck_loafer_batch_state"),
    CheckConstraint("attempt >= 0", name="ck_loafer_batch_attempt"),
)

checkpoints = Table(
    "loafer_checkpoints",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("run_id", String(64), ForeignKey("loafer_runs.id", ondelete="CASCADE"), nullable=False),
    Column(
        "partition_id",
        String(96),
        ForeignKey("loafer_partitions.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("batch_id", String(128), nullable=False),
    Column("source_position_json", JSON, nullable=False),
    Column("fencing_token", BigInteger, nullable=False),
    Column("committed_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("run_id", "partition_id", "batch_id", name="uq_loafer_checkpoint_batch"),
)

run_events = Table(
    "loafer_run_events",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("run_id", String(64), ForeignKey("loafer_runs.id", ondelete="CASCADE"), nullable=False),
    Column("sequence", BigInteger, nullable=False),
    Column("event_type", String(128), nullable=False),
    Column("stage_id", String(96), ForeignKey("loafer_stages.id", ondelete="SET NULL")),
    Column("batch_id", String(128)),
    Column("payload_json", JSON, nullable=False),
    Column("occurred_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("run_id", "sequence", name="uq_loafer_event_sequence"),
    CheckConstraint("sequence > 0", name="ck_loafer_event_sequence"),
)

schedules = Table(
    "loafer_schedules",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("workspace_id", String(64), nullable=False),
    Column(
        "pipeline_version_id",
        String(64),
        ForeignKey("loafer_pipeline_versions.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("trigger_kind", String(16), nullable=False),
    Column("trigger_spec", String(255), nullable=False),
    Column("timezone", String(64), nullable=False),
    Column("enabled", Boolean, nullable=False),
    Column("next_run_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    CheckConstraint("trigger_kind IN ('cron', 'interval')", name="ck_loafer_schedule_trigger"),
)

outbox = Table(
    "loafer_outbox",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("aggregate_type", String(64), nullable=False),
    Column("aggregate_id", String(64), nullable=False),
    Column("event_type", String(128), nullable=False),
    Column("payload_json", JSON, nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("published_at", DateTime(timezone=True)),
    Column("attempts", Integer, nullable=False, default=0),
    CheckConstraint("attempts >= 0", name="ck_loafer_outbox_attempts"),
)

_RUNNABLE_INDEX = Index(
    "ix_loafer_runs_runnable",
    runs.c.state,
    runs.c.retry_at,
    runs.c.created_at,
)
_CHECKPOINT_INDEX = Index(
    "ix_loafer_checkpoints_latest",
    checkpoints.c.run_id,
    checkpoints.c.partition_id,
    checkpoints.c.committed_at,
)
_OUTBOX_INDEX = Index(
    "ix_loafer_outbox_pending",
    outbox.c.published_at,
    outbox.c.available_at,
)

_V1_TABLES = (
    pipeline_versions,
    runs,
    stages,
    partitions,
    artifacts,
    batches,
    checkpoints,
    run_events,
    schedules,
)


def migrate(engine: Engine, target_version: int | None = None) -> int:
    """Apply or revert checked-in migrations to the requested version."""
    target = LATEST_SCHEMA_VERSION if target_version is None else target_version
    if target < 0 or target > LATEST_SCHEMA_VERSION:
        raise MetadataError(f"unsupported metadata schema version: {target}")

    with engine.begin() as connection:
        schema_migrations.create(connection, checkfirst=True)
        current = (
            connection.execute(select(func.max(schema_migrations.c.version))).scalar_one() or 0
        )
        while current < target:
            current += 1
            _UP[current](connection)
            connection.execute(insert(schema_migrations).values(version=current))
        while current > target:
            _DOWN[current](connection)
            connection.execute(
                delete(schema_migrations).where(schema_migrations.c.version == current)
            )
            current -= 1
        return current


def current_version(engine: Engine) -> int:
    with engine.connect() as connection:
        if not engine.dialect.has_table(connection, schema_migrations.name):
            return 0
        return connection.execute(select(func.max(schema_migrations.c.version))).scalar_one() or 0


def _up_v1(connection: Connection) -> None:
    for table in _V1_TABLES:
        table.create(connection, checkfirst=True)
    _RUNNABLE_INDEX.create(connection, checkfirst=True)
    _CHECKPOINT_INDEX.create(connection, checkfirst=True)


def _down_v1(connection: Connection) -> None:
    _CHECKPOINT_INDEX.drop(connection, checkfirst=True)
    _RUNNABLE_INDEX.drop(connection, checkfirst=True)
    for table in reversed(_V1_TABLES):
        table.drop(connection, checkfirst=True)


def _up_v2(connection: Connection) -> None:
    outbox.create(connection, checkfirst=True)
    _OUTBOX_INDEX.create(connection, checkfirst=True)


def _down_v2(connection: Connection) -> None:
    _OUTBOX_INDEX.drop(connection, checkfirst=True)
    outbox.drop(connection, checkfirst=True)


_UP: dict[int, Callable[[Connection], None]] = {1: _up_v1, 2: _up_v2}
_DOWN: dict[int, Callable[[Connection], None]] = {1: _down_v1, 2: _down_v2}
