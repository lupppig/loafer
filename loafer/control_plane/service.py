"""Authenticated control-plane use cases shared by HTTP and typed clients."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy.engine import Connection

from loafer.control_plane.domain import AuthContext, Permission, WorkspaceRole, role_allows
from loafer.control_plane.repository import ControlPlaneRepository
from loafer.core.run_state import RetryCategory, RunState
from loafer.exceptions import IdempotencyConflictError, MetadataError
from loafer.metadata import RunRecord, ScheduleRecord, StoredEvent, utc_now


class NotFoundError(LookupError):
    pass


class PermissionDeniedError(PermissionError):
    pass


class ConflictError(ValueError):
    pass


class ControlPlaneService:
    """Small public surface over tenant policy, audit, and durable commands."""

    def __init__(self, repository: ControlPlaneRepository) -> None:
        self.repository = repository

    def bootstrap(
        self,
        auth: AuthContext,
        *,
        organization_id: str,
        slug: str,
        name: str,
        request_id: str,
    ) -> dict[str, Any]:
        if not auth.is_platform_admin:
            raise PermissionDeniedError("platform admin role is required for bootstrap")
        try:
            with self.repository.transaction() as connection:
                workspace = self.repository.bootstrap_workspace(
                    organization_id=organization_id,
                    subject_id=auth.subject_id,
                    slug=slug,
                    name=name,
                    connection=connection,
                )
                self._audit(
                    auth,
                    workspace["id"],
                    "workspace.bootstrap",
                    "workspace",
                    workspace["id"],
                    request_id,
                    connection=connection,
                )
        except IdempotencyConflictError as exc:
            raise ConflictError(str(exc)) from exc
        return workspace

    def list_workspaces(self, auth: AuthContext) -> list[dict[str, Any]]:
        return self.repository.list_workspaces(auth.subject_id)

    def list_pipelines(self, auth: AuthContext, workspace_id: str) -> list[dict[str, Any]]:
        self._require(auth, workspace_id, Permission.READ)
        return self.repository.list_pipelines(workspace_id)

    def register_pipeline(
        self,
        auth: AuthContext,
        workspace_id: str,
        *,
        pipeline_key: str,
        document: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        self._require(auth, workspace_id, Permission.OPERATE)
        with self.repository.transaction() as connection:
            pipeline = self.repository.register_pipeline(
                workspace_id=workspace_id,
                pipeline_key=pipeline_key,
                document=document,
                connection=connection,
            )
            self._audit(
                auth,
                workspace_id,
                "pipeline.register",
                "pipeline_version",
                pipeline["id"],
                request_id,
                connection=connection,
            )
        return pipeline

    def request_validation(
        self,
        auth: AuthContext,
        workspace_id: str,
        *,
        document: dict[str, Any],
        idempotency_key: str,
        request_id: str,
    ) -> dict[str, Any]:
        self._require(auth, workspace_id, Permission.OPERATE)
        with self.repository.transaction() as connection:
            command = self._command(
                workspace_id=workspace_id,
                kind="pipeline.validate",
                idempotency_key=idempotency_key,
                resource_id=None,
                payload={"document": document},
                connection=connection,
            )
            self._audit(
                auth,
                workspace_id,
                "pipeline.validate",
                "command",
                command["id"],
                request_id,
                connection=connection,
            )
        return command

    def list_runs(
        self, auth: AuthContext, workspace_id: str, *, limit: int = 100
    ) -> list[RunRecord]:
        self._require(auth, workspace_id, Permission.READ)
        return self.repository.list_runs(workspace_id, limit=limit)

    def get_run(self, auth: AuthContext, workspace_id: str, run_id: str) -> RunRecord:
        self._require(auth, workspace_id, Permission.READ)
        run = self.repository.get_run(workspace_id, run_id)
        if run is None:
            raise NotFoundError("run not found")
        return run

    def create_run(
        self,
        auth: AuthContext,
        workspace_id: str,
        *,
        pipeline_version_id: str,
        idempotency_key: str,
        request_id: str,
    ) -> RunRecord:
        self._require(auth, workspace_id, Permission.OPERATE)
        if self.repository.get_pipeline(workspace_id, pipeline_version_id) is None:
            raise NotFoundError("pipeline version not found")
        try:
            with self.repository.transaction() as connection:
                run = self.repository.store.create_run(
                    workspace_id=workspace_id,
                    pipeline_version_id=pipeline_version_id,
                    command_key=f"api:{idempotency_key}",
                    run_id=uuid.uuid4().hex,
                    connection=connection,
                )
                self._audit(
                    auth,
                    workspace_id,
                    "run.create",
                    "run",
                    run.id,
                    request_id,
                    connection=connection,
                )
        except IdempotencyConflictError as exc:
            raise ConflictError(str(exc)) from exc
        return run

    def cancel_run(
        self,
        auth: AuthContext,
        workspace_id: str,
        run_id: str,
        *,
        request_id: str,
    ) -> RunRecord:
        self._require(auth, workspace_id, Permission.OPERATE)
        self.get_run(auth, workspace_id, run_id)
        with self.repository.transaction() as connection:
            run = self.repository.store.request_cancel(
                run_id,
                workspace_id=workspace_id,
                connection=connection,
            )
            self._audit(
                auth,
                workspace_id,
                "run.cancel",
                "run",
                run.id,
                request_id,
                connection=connection,
            )
        return run

    def retry_run(
        self,
        auth: AuthContext,
        workspace_id: str,
        run_id: str,
        *,
        idempotency_key: str,
        request_id: str,
    ) -> RunRecord:
        self._require(auth, workspace_id, Permission.OPERATE)
        parent = self.get_run(auth, workspace_id, run_id)
        if parent.state not in {RunState.FAILED, RunState.CANCELLED, RunState.SUCCEEDED}:
            raise ConflictError("only terminal runs can be retried")
        try:
            with self.repository.transaction() as connection:
                run = self.repository.store.create_run(
                    workspace_id=workspace_id,
                    pipeline_version_id=parent.pipeline_version_id,
                    command_key=f"retry:{idempotency_key}",
                    run_id=uuid.uuid4().hex,
                    parent_run_id=parent.id,
                    retry_category=RetryCategory.MANUAL_RERUN,
                    connection=connection,
                )
                self._audit(
                    auth,
                    workspace_id,
                    "run.retry",
                    "run",
                    run.id,
                    request_id,
                    connection=connection,
                )
        except IdempotencyConflictError as exc:
            raise ConflictError(str(exc)) from exc
        return run

    def backfill(
        self,
        auth: AuthContext,
        workspace_id: str,
        *,
        pipeline_version_id: str,
        window_start: datetime,
        window_end: datetime,
        idempotency_key: str,
        request_id: str,
    ) -> dict[str, Any]:
        self._require(auth, workspace_id, Permission.OPERATE)
        if window_end <= window_start:
            raise ValueError("window_end must be after window_start")
        if self.repository.get_pipeline(workspace_id, pipeline_version_id) is None:
            raise NotFoundError("pipeline version not found")
        with self.repository.transaction() as connection:
            command = self._command(
                workspace_id=workspace_id,
                kind="run.backfill",
                idempotency_key=idempotency_key,
                resource_id=pipeline_version_id,
                payload={
                    "pipeline_version_id": pipeline_version_id,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                },
                connection=connection,
            )
            self._audit(
                auth,
                workspace_id,
                "run.backfill",
                "command",
                command["id"],
                request_id,
                connection=connection,
            )
        return command

    def events(
        self, auth: AuthContext, workspace_id: str, run_id: str, *, after: int = 0
    ) -> list[StoredEvent]:
        self._require(auth, workspace_id, Permission.READ)
        events = self.repository.list_events(workspace_id, run_id, after)
        if events is None:
            raise NotFoundError("run not found")
        return events

    def list_connections(self, auth: AuthContext, workspace_id: str) -> list[dict[str, Any]]:
        self._require(auth, workspace_id, Permission.READ)
        return self.repository.list_connections(workspace_id)

    def create_connection(
        self,
        auth: AuthContext,
        workspace_id: str,
        *,
        environment_id: str | None,
        name: str,
        connector_type: str,
        secret_reference: str,
        metadata: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        self._require(auth, workspace_id, Permission.ADMIN)
        with self.repository.transaction() as transaction:
            connection = self.repository.create_connection(
                workspace_id=workspace_id,
                environment_id=environment_id,
                name=name,
                connector_type=connector_type,
                secret_reference=secret_reference,
                metadata=metadata,
                connection=transaction,
            )
            self._audit(
                auth,
                workspace_id,
                "connection.create",
                "connection",
                connection["id"],
                request_id,
                connection=transaction,
            )
            return connection

    def test_connection(
        self,
        auth: AuthContext,
        workspace_id: str,
        connection_id: str,
        *,
        idempotency_key: str,
        request_id: str,
    ) -> dict[str, Any]:
        self._require(auth, workspace_id, Permission.OPERATE)
        if self.repository.get_connection(workspace_id, connection_id) is None:
            raise NotFoundError("connection not found")
        with self.repository.transaction() as connection:
            command = self._command(
                workspace_id=workspace_id,
                kind="connection.test",
                idempotency_key=idempotency_key,
                resource_id=connection_id,
                payload={"connection_id": connection_id},
                connection=connection,
            )
            self._audit(
                auth,
                workspace_id,
                "connection.test",
                "command",
                command["id"],
                request_id,
                connection=connection,
            )
        return command

    def list_schedules(self, auth: AuthContext, workspace_id: str) -> list[ScheduleRecord]:
        self._require(auth, workspace_id, Permission.READ)
        return self.repository.list_schedules(workspace_id)

    def upsert_schedule(
        self,
        auth: AuthContext,
        workspace_id: str,
        *,
        schedule_id: str,
        pipeline_version_id: str,
        trigger_kind: str,
        trigger_spec: str,
        timezone: str,
        enabled: bool,
        next_run_at: datetime,
        request_id: str,
    ) -> ScheduleRecord:
        self._require(auth, workspace_id, Permission.OPERATE)
        if self.repository.get_pipeline(workspace_id, pipeline_version_id) is None:
            raise NotFoundError("pipeline version not found")
        now = utc_now()
        with self.repository.transaction() as connection:
            schedule = self.repository.store.upsert_schedule(
                ScheduleRecord(
                    id=schedule_id,
                    workspace_id=workspace_id,
                    pipeline_version_id=pipeline_version_id,
                    trigger_kind=trigger_kind,
                    trigger_spec=trigger_spec,
                    timezone=timezone,
                    enabled=enabled,
                    next_run_at=next_run_at,
                    created_at=now,
                    updated_at=now,
                ),
                connection=connection,
            )
            self._audit(
                auth,
                workspace_id,
                "schedule.upsert",
                "schedule",
                schedule.id,
                request_id,
                connection=connection,
            )
        return schedule

    def _require(
        self, auth: AuthContext, workspace_id: str, permission: Permission
    ) -> WorkspaceRole:
        role = self.repository.workspace_role(workspace_id, auth.subject_id)
        if role is None:
            raise NotFoundError("workspace not found")
        if not role_allows(role, permission):
            raise PermissionDeniedError("workspace role does not allow this operation")
        return role

    def _command(self, *, connection: Connection, **kwargs: Any) -> dict[str, Any]:
        try:
            return self.repository.create_control_command(connection=connection, **kwargs)
        except IdempotencyConflictError as exc:
            raise ConflictError(str(exc)) from exc

    def _audit(
        self,
        auth: AuthContext,
        workspace_id: str,
        action: str,
        resource_type: str,
        resource_id: str | None,
        request_id: str,
        *,
        connection: Connection,
    ) -> None:
        organization_id = self.repository.workspace_organization(
            workspace_id,
            connection=connection,
        )
        if organization_id is None:
            raise MetadataError("workspace organization is missing")
        self.repository.audit(
            organization_id=organization_id,
            workspace_id=workspace_id,
            subject_id=auth.subject_id,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            request_id=request_id,
            connection=connection,
        )
