"""Tenant-scoped persistence for the public control-plane interface."""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from sqlalchemy import func, insert, select, text
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import IntegrityError

from loafer.adapters import metadata_schema as schema
from loafer.adapters.metadata import SqlMetadataStore
from loafer.control_plane.domain import WorkspaceRole
from loafer.exceptions import IdempotencyConflictError, MetadataError
from loafer.metadata import RunRecord, ScheduleRecord, StoredEvent, utc_now


class ControlPlaneRepository:
    """Own tenant-safe reads/writes while the durable store owns worker state."""

    def __init__(self, store: SqlMetadataStore, *, clock: Any = utc_now) -> None:
        self.store = store
        self._clock = clock

    def bootstrap_workspace(
        self,
        *,
        organization_id: str,
        subject_id: str,
        slug: str,
        name: str,
    ) -> dict[str, Any]:
        now = self._clock()
        workspace_id = uuid.uuid4().hex
        environment_id = uuid.uuid4().hex
        with self.store.engine.begin() as connection:
            existing = connection.execute(
                select(func.count()).select_from(schema.workspaces)
            ).scalar_one()
            if existing:
                raise IdempotencyConflictError("bootstrap has already been completed")
            connection.execute(
                insert(schema.workspaces).values(
                    id=workspace_id,
                    organization_id=organization_id,
                    slug=slug,
                    name=name,
                    created_at=now,
                )
            )
            connection.execute(
                insert(schema.workspace_permissions).values(
                    workspace_id=workspace_id,
                    subject_id=subject_id,
                    role=WorkspaceRole.OWNER.value,
                    created_at=now,
                )
            )
            connection.execute(
                insert(schema.environments).values(
                    id=environment_id,
                    workspace_id=workspace_id,
                    slug="development",
                    name="Development",
                    is_production=False,
                    created_at=now,
                )
            )
            row = (
                connection.execute(
                    select(schema.workspaces).where(schema.workspaces.c.id == workspace_id)
                )
                .mappings()
                .one()
            )
            return _workspace(row, WorkspaceRole.OWNER)

    def list_workspaces(self, subject_id: str) -> list[dict[str, Any]]:
        with self.store.engine.connect() as connection:
            rows = connection.execute(
                select(schema.workspaces, schema.workspace_permissions.c.role)
                .join(
                    schema.workspace_permissions,
                    schema.workspace_permissions.c.workspace_id == schema.workspaces.c.id,
                )
                .where(schema.workspace_permissions.c.subject_id == subject_id)
                .order_by(schema.workspaces.c.name, schema.workspaces.c.id)
            ).mappings()
            return [_workspace(row, WorkspaceRole(row["role"])) for row in rows]

    def workspace_role(self, workspace_id: str, subject_id: str) -> WorkspaceRole | None:
        with self.store.engine.connect() as connection:
            self._set_workspace_scope(connection, workspace_id)
            value = connection.execute(
                select(schema.workspace_permissions.c.role).where(
                    schema.workspace_permissions.c.workspace_id == workspace_id,
                    schema.workspace_permissions.c.subject_id == subject_id,
                )
            ).scalar_one_or_none()
            return WorkspaceRole(value) if value is not None else None

    def workspace_organization(self, workspace_id: str) -> str | None:
        with self.store.engine.connect() as connection:
            return connection.execute(
                select(schema.workspaces.c.organization_id).where(
                    schema.workspaces.c.id == workspace_id
                )
            ).scalar_one_or_none()

    def list_pipelines(self, workspace_id: str) -> list[dict[str, Any]]:
        with self.store.engine.connect() as connection:
            self._set_workspace_scope(connection, workspace_id)
            rows = connection.execute(
                select(schema.pipeline_versions)
                .where(schema.pipeline_versions.c.workspace_id == workspace_id)
                .order_by(schema.pipeline_versions.c.created_at.desc())
            ).mappings()
            return [_pipeline(row) for row in rows]

    def register_pipeline(
        self,
        *,
        workspace_id: str,
        pipeline_key: str,
        document: dict[str, Any],
    ) -> dict[str, Any]:
        safe_document = _reject_embedded_secrets(document)
        rendered = json.dumps(safe_document, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(rendered.encode()).hexdigest()
        version = self.store.register_pipeline_version(
            workspace_id=workspace_id,
            pipeline_key=pipeline_key,
            config_digest=digest,
            config={"document": safe_document},
        )
        return {
            "id": version.id,
            "workspace_id": version.workspace_id,
            "pipeline_key": version.pipeline_key,
            "config_digest": version.config_digest,
            "created_at": version.created_at,
        }

    def get_pipeline(self, workspace_id: str, version_id: str) -> dict[str, Any] | None:
        with self.store.engine.connect() as connection:
            self._set_workspace_scope(connection, workspace_id)
            row = (
                connection.execute(
                    select(schema.pipeline_versions).where(
                        schema.pipeline_versions.c.id == version_id,
                        schema.pipeline_versions.c.workspace_id == workspace_id,
                    )
                )
                .mappings()
                .one_or_none()
            )
            return _pipeline(row) if row is not None else None

    def list_runs(self, workspace_id: str, *, limit: int = 100) -> list[RunRecord]:
        with self.store.engine.connect() as connection:
            self._set_workspace_scope(connection, workspace_id)
            rows = connection.execute(
                select(schema.runs)
                .where(schema.runs.c.workspace_id == workspace_id)
                .order_by(schema.runs.c.created_at.desc(), schema.runs.c.id)
                .limit(limit)
            ).mappings()
            return [_run(row) for row in rows]

    def get_run(self, workspace_id: str, run_id: str) -> RunRecord | None:
        with self.store.engine.connect() as connection:
            self._set_workspace_scope(connection, workspace_id)
            row = (
                connection.execute(
                    select(schema.runs).where(
                        schema.runs.c.id == run_id,
                        schema.runs.c.workspace_id == workspace_id,
                    )
                )
                .mappings()
                .one_or_none()
            )
            return _run(row) if row is not None else None

    def list_events(
        self, workspace_id: str, run_id: str, after: int = 0
    ) -> list[StoredEvent] | None:
        if self.get_run(workspace_id, run_id) is None:
            return None
        return self.store.list_events(run_id, after, workspace_id=workspace_id)

    def create_connection(
        self,
        *,
        workspace_id: str,
        environment_id: str | None,
        name: str,
        connector_type: str,
        secret_reference: str,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        if not secret_reference or "://" in secret_reference:
            raise ValueError("secret_reference must be an opaque secret-manager identifier")
        safe_metadata = _reject_embedded_secrets(metadata)
        now = self._clock()
        connection_id = uuid.uuid4().hex
        with self.store.engine.begin() as connection:
            self._set_workspace_scope(connection, workspace_id)
            if environment_id is not None:
                environment_workspace = connection.execute(
                    select(schema.environments.c.workspace_id).where(
                        schema.environments.c.id == environment_id
                    )
                ).scalar_one_or_none()
                if environment_workspace != workspace_id:
                    raise MetadataError("environment not found")
            try:
                connection.execute(
                    insert(schema.connections).values(
                        id=connection_id,
                        workspace_id=workspace_id,
                        environment_id=environment_id,
                        name=name,
                        connector_type=connector_type,
                        secret_reference=secret_reference,
                        metadata_json=safe_metadata,
                        created_at=now,
                        updated_at=now,
                    )
                )
            except IntegrityError as exc:
                raise IdempotencyConflictError("connection name already exists") from exc
            row = (
                connection.execute(
                    select(schema.connections).where(schema.connections.c.id == connection_id)
                )
                .mappings()
                .one()
            )
            return _connection(row)

    def list_connections(self, workspace_id: str) -> list[dict[str, Any]]:
        with self.store.engine.connect() as connection:
            self._set_workspace_scope(connection, workspace_id)
            rows = connection.execute(
                select(schema.connections)
                .where(schema.connections.c.workspace_id == workspace_id)
                .order_by(schema.connections.c.name, schema.connections.c.id)
            ).mappings()
            return [_connection(row) for row in rows]

    def get_connection(self, workspace_id: str, connection_id: str) -> dict[str, Any] | None:
        with self.store.engine.connect() as connection:
            self._set_workspace_scope(connection, workspace_id)
            row = (
                connection.execute(
                    select(schema.connections).where(
                        schema.connections.c.id == connection_id,
                        schema.connections.c.workspace_id == workspace_id,
                    )
                )
                .mappings()
                .one_or_none()
            )
            return _connection(row) if row is not None else None

    def create_control_command(
        self,
        *,
        workspace_id: str,
        kind: str,
        idempotency_key: str,
        resource_id: str | None,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        safe_payload = _reject_embedded_secrets(payload)
        now = self._clock()
        with self.store.engine.begin() as connection:
            self._set_workspace_scope(connection, workspace_id)
            existing = (
                connection.execute(
                    select(schema.control_commands).where(
                        schema.control_commands.c.workspace_id == workspace_id,
                        schema.control_commands.c.kind == kind,
                        schema.control_commands.c.idempotency_key == idempotency_key,
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if (
                    existing["payload_json"] != safe_payload
                    or existing["resource_id"] != resource_id
                ):
                    raise IdempotencyConflictError(
                        "idempotency key was reused with a different command"
                    )
                return _command(existing)
            command_id = uuid.uuid4().hex
            connection.execute(
                insert(schema.control_commands).values(
                    id=command_id,
                    workspace_id=workspace_id,
                    kind=kind,
                    idempotency_key=idempotency_key,
                    resource_id=resource_id,
                    payload_json=safe_payload,
                    state="queued",
                    created_at=now,
                )
            )
            row = (
                connection.execute(
                    select(schema.control_commands).where(
                        schema.control_commands.c.id == command_id
                    )
                )
                .mappings()
                .one()
            )
            return _command(row)

    def list_schedules(self, workspace_id: str) -> list[ScheduleRecord]:
        with self.store.engine.connect() as connection:
            self._set_workspace_scope(connection, workspace_id)
            rows = connection.execute(
                select(schema.schedules)
                .where(schema.schedules.c.workspace_id == workspace_id)
                .order_by(schema.schedules.c.created_at.desc())
            ).mappings()
            return [_schedule(row) for row in rows]

    def audit(
        self,
        *,
        organization_id: str,
        workspace_id: str | None,
        subject_id: str,
        action: str,
        resource_type: str,
        resource_id: str | None,
        request_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self.store.engine.begin() as connection:
            if workspace_id is not None:
                self._set_workspace_scope(connection, workspace_id)
            connection.execute(
                insert(schema.audit_events).values(
                    id=uuid.uuid4().hex,
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    subject_id=subject_id,
                    action=action,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    outcome="succeeded",
                    request_id=request_id,
                    metadata_json=_reject_embedded_secrets(metadata or {}),
                    occurred_at=self._clock(),
                )
            )

    def _set_workspace_scope(self, connection: Any, workspace_id: str) -> None:
        if self.store.profile == "postgresql":
            connection.execute(
                text("SELECT set_config('loafer.workspace_id', :workspace_id, true)"),
                {"workspace_id": workspace_id},
            )


_SECRET_KEYS = {
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "credentials",
    "password",
    "private_key",
    "secret",
    "token",
    "url",
}


def _reject_embedded_secrets(value: Any, path: str = "payload") -> Any:
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in _SECRET_KEYS or normalized.endswith(
                ("_password", "_secret", "_token")
            ):
                raise ValueError(f"{path}.{key} must use a server-side secret reference")
            cleaned[str(key)] = _reject_embedded_secrets(item, f"{path}.{key}")
        return cleaned
    if isinstance(value, list):
        return [_reject_embedded_secrets(item, path) for item in value]
    return value


def _workspace(row: RowMapping, role: WorkspaceRole) -> dict[str, Any]:
    return {
        "id": row["id"],
        "organization_id": row["organization_id"],
        "slug": row["slug"],
        "name": row["name"],
        "role": role.value,
        "created_at": row["created_at"],
    }


def _pipeline(row: RowMapping) -> dict[str, Any]:
    return {
        "id": row["id"],
        "workspace_id": row["workspace_id"],
        "pipeline_key": row["pipeline_key"],
        "config_digest": row["config_digest"],
        "created_at": row["created_at"],
    }


def _connection(row: RowMapping) -> dict[str, Any]:
    return {
        "id": row["id"],
        "workspace_id": row["workspace_id"],
        "environment_id": row["environment_id"],
        "name": row["name"],
        "connector_type": row["connector_type"],
        "metadata": row["metadata_json"],
        "has_secret_reference": bool(row["secret_reference"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _command(row: RowMapping) -> dict[str, Any]:
    return {
        "id": row["id"],
        "workspace_id": row["workspace_id"],
        "kind": row["kind"],
        "resource_id": row["resource_id"],
        "state": row["state"],
        "created_at": row["created_at"],
    }


def _run(row: RowMapping) -> RunRecord:
    from loafer.adapters.metadata import _run_record

    return _run_record(row)


def _schedule(row: RowMapping) -> ScheduleRecord:
    from loafer.adapters.metadata import _schedule as schedule_from_row

    return schedule_from_row(row)
