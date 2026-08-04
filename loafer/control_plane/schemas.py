"""Credential-free OpenAPI contracts for `/api/v1`."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BootstrapRequest(StrictModel):
    organization_id: str = Field(min_length=1, max_length=128)
    workspace_slug: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    workspace_name: str = Field(min_length=1, max_length=255)


class WorkspaceResponse(StrictModel):
    id: str
    organization_id: str
    slug: str
    name: str
    role: Literal["owner", "admin", "operator", "viewer"]
    created_at: datetime


class PipelineCreateRequest(StrictModel):
    pipeline_key: str = Field(min_length=1, max_length=255)
    document: dict[str, Any]


class PipelineValidationRequest(StrictModel):
    document: dict[str, Any]


class PipelineResponse(StrictModel):
    id: str
    workspace_id: str
    pipeline_key: str
    config_digest: str
    created_at: datetime


class RunCreateRequest(StrictModel):
    pipeline_version_id: str = Field(min_length=1, max_length=64)


class RunResponse(StrictModel):
    id: str
    workspace_id: str
    pipeline_version_id: str
    state: str
    attempt: int
    retry_category: str | None
    cancel_requested: bool
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    parent_run_id: str | None
    error: dict[str, Any] | None


class BackfillRequest(StrictModel):
    pipeline_version_id: str
    window_start: datetime
    window_end: datetime


class StoredEventResponse(StrictModel):
    run_id: str
    sequence: int
    event_type: str
    payload: dict[str, Any]
    occurred_at: datetime


class ConnectionCreateRequest(StrictModel):
    environment_id: str | None = None
    name: str = Field(min_length=1, max_length=255)
    connector_type: str = Field(min_length=1, max_length=64)
    secret_reference: str = Field(min_length=1, max_length=512)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ConnectionResponse(StrictModel):
    id: str
    workspace_id: str
    environment_id: str | None
    name: str
    connector_type: str
    metadata: dict[str, Any]
    has_secret_reference: bool
    created_at: datetime
    updated_at: datetime


class ScheduleUpsertRequest(StrictModel):
    id: str = Field(min_length=1, max_length=64)
    pipeline_version_id: str
    trigger_kind: Literal["cron", "interval"]
    trigger_spec: str = Field(min_length=1, max_length=255)
    timezone: str = Field(min_length=1, max_length=64)
    enabled: bool = True
    next_run_at: datetime


class ScheduleResponse(StrictModel):
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


class CommandResponse(StrictModel):
    id: str
    workspace_id: str
    kind: str
    resource_id: str | None
    state: str
    created_at: datetime


class ProblemResponse(BaseModel):
    type: str
    title: str
    status: int
    detail: str
    instance: str
    request_id: str
