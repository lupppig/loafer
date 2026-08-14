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
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    delete,
    func,
    insert,
    inspect,
    select,
    text,
)
from sqlalchemy.engine import Connection, Engine

from loafer.core.roles import WorkerRole
from loafer.core.run_state import BatchState, RetryCategory, RunState, StageState
from loafer.exceptions import MetadataError

LATEST_SCHEMA_VERSION = 4
_POSTGRES_MIGRATION_LOCK_ID = int.from_bytes(b"loafer", byteorder="big")
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
    Column("role", String(32), nullable=False, server_default="etl"),
    Column("environment_id", String(64)),
    Column("quarantined", Boolean, nullable=False, server_default=text("false")),
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
    Column("role", String(32), nullable=False, server_default="etl"),
    Column("claimed_until", DateTime(timezone=True)),
    Column("last_error", Text),
    CheckConstraint("attempts >= 0", name="ck_loafer_outbox_attempts"),
)

workspaces = Table(
    "loafer_workspaces",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("organization_id", String(128), nullable=False),
    Column("slug", String(128), nullable=False),
    Column("name", String(255), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("max_concurrent_runs", Integer),
    UniqueConstraint("organization_id", "slug", name="uq_loafer_workspace_slug"),
)

environments = Table(
    "loafer_environments",
    metadata,
    Column("id", String(64), primary_key=True),
    Column(
        "workspace_id",
        String(64),
        ForeignKey("loafer_workspaces.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("slug", String(128), nullable=False),
    Column("name", String(255), nullable=False),
    Column("is_production", Boolean, nullable=False, default=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("max_concurrent_runs", Integer),
    UniqueConstraint("workspace_id", "slug", name="uq_loafer_environment_slug"),
)

workspace_permissions = Table(
    "loafer_workspace_permissions",
    metadata,
    Column(
        "workspace_id",
        String(64),
        ForeignKey("loafer_workspaces.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("subject_id", String(128), nullable=False),
    Column("role", String(32), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    PrimaryKeyConstraint("workspace_id", "subject_id", name="pk_loafer_workspace_permission"),
    CheckConstraint(
        "role IN ('owner', 'admin', 'operator', 'viewer')",
        name="ck_loafer_workspace_role",
    ),
)

connections = Table(
    "loafer_connections",
    metadata,
    Column("id", String(64), primary_key=True),
    Column(
        "workspace_id",
        String(64),
        ForeignKey("loafer_workspaces.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "environment_id",
        String(64),
        ForeignKey("loafer_environments.id", ondelete="SET NULL"),
    ),
    Column("name", String(255), nullable=False),
    Column("connector_type", String(64), nullable=False),
    Column("secret_reference", String(512), nullable=False),
    Column("metadata_json", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("workspace_id", "name", name="uq_loafer_connection_name"),
)

control_commands = Table(
    "loafer_control_commands",
    metadata,
    Column("id", String(64), primary_key=True),
    Column(
        "workspace_id",
        String(64),
        ForeignKey("loafer_workspaces.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("kind", String(64), nullable=False),
    Column("idempotency_key", String(255), nullable=False),
    Column("resource_id", String(64)),
    Column("payload_json", JSON, nullable=False),
    Column("state", String(32), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "workspace_id",
        "kind",
        "idempotency_key",
        name="uq_loafer_control_command",
    ),
    CheckConstraint(
        "state IN ('queued', 'accepted', 'failed')",
        name="ck_loafer_control_command_state",
    ),
)

audit_events = Table(
    "loafer_audit_events",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("organization_id", String(128), nullable=False),
    Column("workspace_id", String(64)),
    Column("subject_id", String(128), nullable=False),
    Column("action", String(128), nullable=False),
    Column("resource_type", String(64), nullable=False),
    Column("resource_id", String(128)),
    Column("outcome", String(32), nullable=False),
    Column("request_id", String(128), nullable=False),
    Column("metadata_json", JSON, nullable=False),
    Column("occurred_at", DateTime(timezone=True), nullable=False),
)

_CONTROL_PLANE_TABLES = (
    workspaces,
    environments,
    workspace_permissions,
    connections,
    control_commands,
    audit_events,
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
_TENANT_ACTIVE_INDEX = Index(
    "ix_loafer_runs_tenant_active",
    runs.c.workspace_id,
    runs.c.state,
)
_SCHEDULE_DUE_INDEX = Index(
    "ix_loafer_schedules_due",
    schedules.c.enabled,
    schedules.c.next_run_at,
)
_OUTBOX_CLAIM_INDEX = Index(
    "ix_loafer_outbox_claimable",
    outbox.c.published_at,
    outbox.c.claimed_until,
)

_V4_INDEXES = (_TENANT_ACTIVE_INDEX, _SCHEDULE_DUE_INDEX, _OUTBOX_CLAIM_INDEX)

# Columns v4 introduces on tables that earlier versions already created. A
# fresh database gets them from the table definitions above at v1/v2/v3, so
# every ALTER here is applied only when the column is genuinely absent.
_V4_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("loafer_runs", "role", "VARCHAR(32) NOT NULL DEFAULT 'etl'"),
    ("loafer_runs", "environment_id", "VARCHAR(64)"),
    ("loafer_runs", "quarantined", "BOOLEAN NOT NULL DEFAULT false"),
    ("loafer_outbox", "role", "VARCHAR(32) NOT NULL DEFAULT 'etl'"),
    ("loafer_outbox", "claimed_until", "TIMESTAMP WITH TIME ZONE"),
    ("loafer_outbox", "last_error", "TEXT"),
    ("loafer_workspaces", "max_concurrent_runs", "INTEGER"),
    ("loafer_environments", "max_concurrent_runs", "INTEGER"),
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
        if connection.dialect.name == "postgresql":
            connection.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": _POSTGRES_MIGRATION_LOCK_ID},
            )
        schema_migrations.create(connection, checkfirst=True)
        current = (
            connection.execute(select(func.max(schema_migrations.c.version))).scalar_one() or 0
        )
        if current > LATEST_SCHEMA_VERSION:
            raise MetadataError(
                f"metadata schema version {current} is newer than this Loafer build supports "
                f"({LATEST_SCHEMA_VERSION}); deploy a compatible Loafer version"
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


def require_current_version(engine: Engine) -> int:
    """Return the current schema version or fail without modifying the database."""
    current = current_version(engine)
    if current != LATEST_SCHEMA_VERSION:
        raise MetadataError(
            f"metadata schema version {current} is incompatible with this Loafer build; "
            f"expected {LATEST_SCHEMA_VERSION}. Run `loafer metadata migrate` before "
            "starting Loafer services."
        )
    return current


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


def _up_v3(connection: Connection) -> None:
    for table in _CONTROL_PLANE_TABLES:
        table.create(connection, checkfirst=True)
    if connection.dialect.name == "postgresql":
        for table_name in (
            "loafer_pipeline_versions",
            "loafer_runs",
            "loafer_schedules",
            "loafer_environments",
            "loafer_workspace_permissions",
            "loafer_connections",
            "loafer_control_commands",
        ):
            connection.execute(text(f"ALTER TABLE {table_name} ENABLE ROW LEVEL SECURITY"))
        connection.execute(
            text(
                """
                CREATE POLICY loafer_workspace_scope_pipeline_versions
                ON loafer_pipeline_versions USING (
                    current_setting('loafer.workspace_id', true) IS NULL
                    OR workspace_id = current_setting('loafer.workspace_id', true)
                )
                """
            )
        )
        for table_name in (
            "loafer_runs",
            "loafer_schedules",
            "loafer_environments",
            "loafer_workspace_permissions",
            "loafer_connections",
            "loafer_control_commands",
        ):
            connection.execute(
                text(
                    f"""
                    CREATE POLICY loafer_workspace_scope_{table_name.removeprefix("loafer_")}
                    ON {table_name} USING (
                        current_setting('loafer.workspace_id', true) IS NULL
                        OR workspace_id = current_setting('loafer.workspace_id', true)
                    )
                    """
                )
            )


def _down_v3(connection: Connection) -> None:
    if connection.dialect.name == "postgresql":
        connection.execute(
            text(
                "DROP POLICY IF EXISTS loafer_workspace_scope_pipeline_versions "
                "ON loafer_pipeline_versions"
            )
        )
        connection.execute(text("ALTER TABLE loafer_pipeline_versions DISABLE ROW LEVEL SECURITY"))
        for table_name in ("loafer_runs", "loafer_schedules"):
            policy_name = f"loafer_workspace_scope_{table_name.removeprefix('loafer_')}"
            connection.execute(text(f"DROP POLICY IF EXISTS {policy_name} ON {table_name}"))
            connection.execute(text(f"ALTER TABLE {table_name} DISABLE ROW LEVEL SECURITY"))
    for table in reversed(_CONTROL_PLANE_TABLES):
        table.drop(connection, checkfirst=True)


def _column_names(connection: Connection, table_name: str) -> set[str]:
    return {column["name"] for column in inspect(connection).get_columns(table_name)}


def _up_v4(connection: Connection) -> None:
    for table_name, column_name, definition in _V4_COLUMNS:
        if column_name in _column_names(connection, table_name):
            continue
        connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}"))
    for index in _V4_INDEXES:
        index.create(connection, checkfirst=True)
    if connection.dialect.name == "postgresql":
        # SQLite cannot add a constraint to an existing table, so the role
        # domain is enforced in the authoritative store only. The embedded
        # profile relies on WorkerRole at the adapter boundary, the same way
        # its tenant scoping relies on explicit predicates rather than RLS.
        connection.execute(
            text(
                f"ALTER TABLE loafer_runs ADD CONSTRAINT ck_loafer_run_role "
                f"CHECK (role IN ({_values(WorkerRole)}))"
            )
        )


def _down_v4(connection: Connection) -> None:
    if connection.dialect.name == "postgresql":
        connection.execute(
            text("ALTER TABLE loafer_runs DROP CONSTRAINT IF EXISTS ck_loafer_run_role")
        )
    for index in reversed(_V4_INDEXES):
        index.drop(connection, checkfirst=True)
    for table_name, column_name, _definition in reversed(_V4_COLUMNS):
        if column_name not in _column_names(connection, table_name):
            continue
        connection.execute(text(f"ALTER TABLE {table_name} DROP COLUMN {column_name}"))


_UP: dict[int, Callable[[Connection], None]] = {1: _up_v1, 2: _up_v2, 3: _up_v3, 4: _up_v4}
_DOWN: dict[int, Callable[[Connection], None]] = {
    1: _down_v1,
    2: _down_v2,
    3: _down_v3,
    4: _down_v4,
}
