"""FastAPI composition root for the HTTPS-only `loaferd` service."""

import asyncio
import json
import math
import os
import time
import uuid
from collections import defaultdict, deque
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import ValidationError

from loafer.adapters.metadata import SqlMetadataStore
from loafer.application.durable import get_metadata_store
from loafer.control_plane.auth import (
    AuthenticationError,
    BetterAuthJWTVerifier,
    TokenVerifier,
)
from loafer.control_plane.domain import AuthContext
from loafer.control_plane.repository import ControlPlaneRepository
from loafer.control_plane.schemas import (
    BackfillRequest,
    BootstrapRequest,
    CommandResponse,
    ConnectionCreateRequest,
    ConnectionResponse,
    PipelineCreateRequest,
    PipelineResponse,
    PipelineValidationRequest,
    ProblemResponse,
    RunCreateRequest,
    RunResponse,
    ScheduleResponse,
    ScheduleUpsertRequest,
    StoredEventResponse,
    WorkspaceResponse,
)
from loafer.control_plane.service import (
    ConflictError,
    ControlPlaneService,
    NotFoundError,
    PermissionDeniedError,
)
from loafer.exceptions import IdempotencyConflictError, MetadataError
from loafer.metadata import RunRecord, ScheduleRecord, StoredEvent


@dataclass(frozen=True, slots=True)
class ControlPlaneSettings:
    issuer: str
    audience: str
    jwks_url: str
    allowed_origins: tuple[str, ...]
    jwks_timeout_seconds: float = 5.0
    enforce_https: bool = True
    trust_proxy_headers: bool = False
    rate_limit_requests: int = 120
    rate_limit_window_seconds: int = 60
    sse_poll_seconds: float = 1.0
    sse_heartbeat_seconds: float = 15.0

    @classmethod
    def from_environment(cls) -> "ControlPlaneSettings":
        issuer = os.environ.get("LOAFER_AUTH_ISSUER", "")
        audience = os.environ.get("LOAFER_AUTH_AUDIENCE", "")
        jwks_url = os.environ.get("LOAFER_AUTH_JWKS_URL", "")
        origins = tuple(
            item.strip()
            for item in os.environ.get("LOAFER_ALLOWED_ORIGINS", "").split(",")
            if item.strip()
        )
        if not issuer or not audience or not jwks_url or not origins:
            raise RuntimeError(
                "LOAFER_AUTH_ISSUER, LOAFER_AUTH_AUDIENCE, LOAFER_AUTH_JWKS_URL, "
                "and LOAFER_ALLOWED_ORIGINS are required"
            )
        return cls(
            issuer=issuer,
            audience=audience,
            jwks_url=jwks_url,
            allowed_origins=origins,
            jwks_timeout_seconds=float(os.environ.get("LOAFER_AUTH_JWKS_TIMEOUT_SECONDS", "5")),
            trust_proxy_headers=os.environ.get("LOAFER_TRUST_PROXY_HEADERS") == "1",
        )


def create_app(
    *,
    settings: ControlPlaneSettings | None = None,
    store: SqlMetadataStore | None = None,
    verifier: TokenVerifier | None = None,
) -> FastAPI:
    """Create one stateless control-plane instance over authoritative metadata."""
    selected = settings or ControlPlaneSettings.from_environment()
    _validate_settings(selected)
    owned_store = store is None
    metadata = store or get_metadata_store()
    try:
        metadata.verify_schema()
    except Exception:
        if owned_store:
            metadata.close()
        raise
    token_verifier = verifier or BetterAuthJWTVerifier(
        jwks_url=selected.jwks_url,
        issuer=selected.issuer,
        audience=selected.audience,
        jwks_timeout_seconds=selected.jwks_timeout_seconds,
    )
    service = ControlPlaneService(ControlPlaneRepository(metadata))
    bearer = HTTPBearer(auto_error=False)
    limiter = _RateLimiter(selected.rate_limit_requests, selected.rate_limit_window_seconds)

    app = FastAPI(
        title="Loafer Control Plane",
        version="1.0.0",
        description=(
            "HTTPS command and resource API. HTTP processes never execute pipeline data work."
        ),
        openapi_url="/api/v1/openapi.json",
        docs_url=None,
        redoc_url=None,
    )
    app.state.metadata_store = metadata
    app.state.control_plane = service

    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(selected.allowed_origins),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "Idempotency-Key", "Last-Event-ID"],
    )

    @app.middleware("http")
    async def security_boundary(request: Request, call_next: Callable[..., Any]) -> Any:
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = request_id
        scheme = request.url.scheme
        client_identity = request.client.host if request.client else "unknown"
        if selected.trust_proxy_headers:
            forwarded = request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip()
            if forwarded:
                scheme = forwarded
            forwarded_for = request.headers.get("x-forwarded-for", "").split(",", 1)[0].strip()
            if forwarded_for:
                client_identity = forwarded_for
        if selected.enforce_https and scheme != "https":
            response = _problem(request, 400, "HTTPS required", "loaferd accepts HTTPS only")
        elif (
            request.headers.get("origin")
            and request.headers["origin"] not in selected.allowed_origins
        ):
            response = _problem(request, 403, "Origin rejected", "request origin is not trusted")
        elif (
            request.method in {"POST", "PUT", "PATCH", "DELETE"}
            and request.headers.get("cookie")
            and not request.headers.get("authorization")
        ):
            response = _problem(
                request,
                403,
                "Bearer token required",
                "cookie-authenticated mutations must pass through the CSRF-protected web BFF",
            )
        elif not limiter.allow(client_identity):
            response = _problem(
                request, 429, "Rate limit exceeded", "retry after the rate-limit window"
            )
            response.headers["Retry-After"] = str(selected.rate_limit_window_seconds)
        else:
            response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = "default-src 'none'; frame-ancestors 'none'"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["Cache-Control"] = "no-store"
        return response

    async def authenticate(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> AuthContext:
        if credentials is None or credentials.scheme.lower() != "bearer":
            raise AuthenticationError("a bearer token is required")
        return await asyncio.to_thread(token_verifier.verify, credentials.credentials)

    auth_dependency = Annotated[AuthContext, Depends(authenticate)]

    @app.exception_handler(AuthenticationError)
    async def authentication_error(request: Request, exc: AuthenticationError) -> JSONResponse:
        response = _problem(request, 401, "Authentication required", str(exc))
        response.headers["WWW-Authenticate"] = "Bearer"
        return response

    @app.exception_handler(NotFoundError)
    async def not_found(request: Request, exc: NotFoundError) -> JSONResponse:
        return _problem(request, 404, "Resource not found", str(exc))

    @app.exception_handler(PermissionDeniedError)
    async def forbidden(request: Request, exc: PermissionDeniedError) -> JSONResponse:
        return _problem(request, 403, "Permission denied", str(exc))

    @app.exception_handler(ConflictError)
    @app.exception_handler(IdempotencyConflictError)
    async def conflict(request: Request, exc: Exception) -> JSONResponse:
        return _problem(request, 409, "Command conflict", str(exc))

    @app.exception_handler(ValueError)
    @app.exception_handler(ValidationError)
    async def invalid(request: Request, exc: Exception) -> JSONResponse:
        return _problem(request, 422, "Invalid request", str(exc))

    @app.exception_handler(RequestValidationError)
    async def request_invalid(request: Request, _exc: RequestValidationError) -> JSONResponse:
        return _problem(
            request,
            422,
            "Invalid request",
            "request body, headers, or parameters failed validation",
        )

    @app.exception_handler(MetadataError)
    async def persistence_error(request: Request, exc: MetadataError) -> JSONResponse:
        return _problem(request, 503, "Control plane unavailable", "metadata operation failed")

    @app.get("/healthz", include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/api/v1/bootstrap",
        response_model=WorkspaceResponse,
        status_code=status.HTTP_201_CREATED,
        responses={409: {"model": ProblemResponse}},
    )
    async def bootstrap(payload: BootstrapRequest, request: Request, auth: auth_dependency) -> Any:
        return service.bootstrap(
            auth,
            organization_id=payload.organization_id,
            slug=payload.workspace_slug,
            name=payload.workspace_name,
            request_id=request.state.request_id,
        )

    @app.get("/api/v1/workspaces", response_model=list[WorkspaceResponse])
    async def workspaces(auth: auth_dependency) -> Any:
        return service.list_workspaces(auth)

    @app.get(
        "/api/v1/workspaces/{workspace_id}/pipelines",
        response_model=list[PipelineResponse],
    )
    async def pipelines(workspace_id: str, auth: auth_dependency) -> Any:
        return service.list_pipelines(auth, workspace_id)

    @app.post(
        "/api/v1/workspaces/{workspace_id}/pipelines",
        response_model=PipelineResponse,
        status_code=status.HTTP_201_CREATED,
    )
    async def register_pipeline(
        workspace_id: str,
        payload: PipelineCreateRequest,
        request: Request,
        auth: auth_dependency,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> Any:
        return service.register_pipeline(
            auth,
            workspace_id,
            pipeline_key=payload.pipeline_key,
            document=payload.document,
            request_id=request.state.request_id,
        )

    @app.post(
        "/api/v1/workspaces/{workspace_id}/pipelines/validate",
        response_model=CommandResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def validate_pipeline(
        workspace_id: str,
        payload: PipelineValidationRequest,
        request: Request,
        auth: auth_dependency,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> Any:
        return service.request_validation(
            auth,
            workspace_id,
            document=payload.document,
            idempotency_key=idempotency_key,
            request_id=request.state.request_id,
        )

    @app.get("/api/v1/workspaces/{workspace_id}/runs", response_model=list[RunResponse])
    async def runs(
        workspace_id: str,
        auth: auth_dependency,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> Any:
        return [_run_response(run) for run in service.list_runs(auth, workspace_id, limit=limit)]

    @app.post(
        "/api/v1/workspaces/{workspace_id}/runs",
        response_model=RunResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def create_run(
        workspace_id: str,
        payload: RunCreateRequest,
        request: Request,
        auth: auth_dependency,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> Any:
        return _run_response(
            service.create_run(
                auth,
                workspace_id,
                pipeline_version_id=payload.pipeline_version_id,
                idempotency_key=idempotency_key,
                request_id=request.state.request_id,
            )
        )

    @app.get("/api/v1/workspaces/{workspace_id}/runs/{run_id}", response_model=RunResponse)
    async def run_detail(workspace_id: str, run_id: str, auth: auth_dependency) -> Any:
        return _run_response(service.get_run(auth, workspace_id, run_id))

    @app.post(
        "/api/v1/workspaces/{workspace_id}/runs/{run_id}/cancel",
        response_model=RunResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def cancel_run(
        workspace_id: str, run_id: str, request: Request, auth: auth_dependency
    ) -> Any:
        return _run_response(
            service.cancel_run(auth, workspace_id, run_id, request_id=request.state.request_id)
        )

    @app.post(
        "/api/v1/workspaces/{workspace_id}/runs/{run_id}/retry",
        response_model=RunResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def retry_run(
        workspace_id: str,
        run_id: str,
        request: Request,
        auth: auth_dependency,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> Any:
        return _run_response(
            service.retry_run(
                auth,
                workspace_id,
                run_id,
                idempotency_key=idempotency_key,
                request_id=request.state.request_id,
            )
        )

    @app.post(
        "/api/v1/workspaces/{workspace_id}/backfills",
        response_model=CommandResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def backfill(
        workspace_id: str,
        payload: BackfillRequest,
        request: Request,
        auth: auth_dependency,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> Any:
        return service.backfill(
            auth,
            workspace_id,
            pipeline_version_id=payload.pipeline_version_id,
            window_start=payload.window_start,
            window_end=payload.window_end,
            idempotency_key=idempotency_key,
            request_id=request.state.request_id,
        )

    @app.get(
        "/api/v1/workspaces/{workspace_id}/runs/{run_id}/events",
        response_model=list[StoredEventResponse],
    )
    async def events(
        workspace_id: str,
        run_id: str,
        auth: auth_dependency,
        after: Annotated[int, Query(ge=0)] = 0,
    ) -> Any:
        return [
            _event_response(event)
            for event in service.events(auth, workspace_id, run_id, after=after)
        ]

    @app.get(
        "/api/v1/workspaces/{workspace_id}/runs/{run_id}/logs",
        response_model=list[StoredEventResponse],
    )
    async def logs(
        workspace_id: str,
        run_id: str,
        auth: auth_dependency,
        after: Annotated[int, Query(ge=0)] = 0,
    ) -> Any:
        return [
            _event_response(event)
            for event in service.events(auth, workspace_id, run_id, after=after)
        ]

    @app.get("/api/v1/workspaces/{workspace_id}/runs/{run_id}/stream")
    async def event_stream(
        workspace_id: str,
        run_id: str,
        request: Request,
        auth: auth_dependency,
        last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    ) -> StreamingResponse:
        try:
            after = int(last_event_id or "0")
        except ValueError as exc:
            raise ValueError("Last-Event-ID must be an integer sequence") from exc
        service.get_run(auth, workspace_id, run_id)
        return StreamingResponse(
            _stream_events(
                request,
                service,
                auth,
                workspace_id,
                run_id,
                after,
                poll_seconds=selected.sse_poll_seconds,
                heartbeat_seconds=selected.sse_heartbeat_seconds,
            ),
            media_type="text/event-stream",
            headers={"X-Accel-Buffering": "no", "Connection": "keep-alive"},
        )

    @app.get(
        "/api/v1/workspaces/{workspace_id}/connections",
        response_model=list[ConnectionResponse],
    )
    async def connections(workspace_id: str, auth: auth_dependency) -> Any:
        return service.list_connections(auth, workspace_id)

    @app.post(
        "/api/v1/workspaces/{workspace_id}/connections",
        response_model=ConnectionResponse,
        status_code=status.HTTP_201_CREATED,
    )
    async def create_connection(
        workspace_id: str,
        payload: ConnectionCreateRequest,
        request: Request,
        auth: auth_dependency,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> Any:
        return service.create_connection(
            auth,
            workspace_id,
            environment_id=payload.environment_id,
            name=payload.name,
            connector_type=payload.connector_type,
            secret_reference=payload.secret_reference,
            metadata=payload.metadata,
            request_id=request.state.request_id,
        )

    @app.post(
        "/api/v1/workspaces/{workspace_id}/connections/{connection_id}/test",
        response_model=CommandResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def test_connection(
        workspace_id: str,
        connection_id: str,
        request: Request,
        auth: auth_dependency,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> Any:
        return service.test_connection(
            auth,
            workspace_id,
            connection_id,
            idempotency_key=idempotency_key,
            request_id=request.state.request_id,
        )

    @app.get(
        "/api/v1/workspaces/{workspace_id}/schedules",
        response_model=list[ScheduleResponse],
    )
    async def schedules(workspace_id: str, auth: auth_dependency) -> Any:
        return [_schedule_response(item) for item in service.list_schedules(auth, workspace_id)]

    @app.put(
        "/api/v1/workspaces/{workspace_id}/schedules/{schedule_id}",
        response_model=ScheduleResponse,
    )
    async def upsert_schedule(
        workspace_id: str,
        schedule_id: str,
        payload: ScheduleUpsertRequest,
        request: Request,
        auth: auth_dependency,
    ) -> Any:
        if payload.id != schedule_id:
            raise ValueError("schedule id in the path and body must match")
        return _schedule_response(
            service.upsert_schedule(
                auth,
                workspace_id,
                schedule_id=schedule_id,
                pipeline_version_id=payload.pipeline_version_id,
                trigger_kind=payload.trigger_kind,
                trigger_spec=payload.trigger_spec,
                timezone=payload.timezone,
                enabled=payload.enabled,
                next_run_at=payload.next_run_at,
                request_id=request.state.request_id,
            )
        )

    if owned_store:

        @app.on_event("shutdown")
        def close_metadata() -> None:
            metadata.close()

    return app


class _RateLimiter:
    def __init__(self, maximum: int, window_seconds: int) -> None:
        self.maximum = maximum
        self.window_seconds = window_seconds
        self._requests: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        requests = self._requests[key]
        while requests and requests[0] <= now - self.window_seconds:
            requests.popleft()
        if len(requests) >= self.maximum:
            return False
        requests.append(now)
        return True


def _validate_settings(settings: ControlPlaneSettings) -> None:
    if settings.enforce_https:
        for name, value in {
            "issuer": settings.issuer,
            "audience": settings.audience,
            "jwks_url": settings.jwks_url,
            **{f"origin[{index}]": origin for index, origin in enumerate(settings.allowed_origins)},
        }.items():
            if not value.startswith("https://"):
                raise ValueError(f"{name} must use HTTPS")
    if not settings.allowed_origins:
        raise ValueError("at least one trusted origin is required")
    if not math.isfinite(settings.jwks_timeout_seconds) or settings.jwks_timeout_seconds <= 0:
        raise ValueError("JWKS timeout must be positive and finite")


def _problem(request: Request, status_code: int, title: str, detail: str) -> JSONResponse:
    request_id = getattr(request.state, "request_id", uuid.uuid4().hex)
    body = ProblemResponse(
        type=f"https://loafer.dev/problems/{title.lower().replace(' ', '-')}",
        title=title,
        status=status_code,
        detail=detail,
        instance=request.url.path,
        request_id=request_id,
    )
    return JSONResponse(
        status_code=status_code,
        content=body.model_dump(mode="json"),
        media_type="application/problem+json",
    )


def _run_response(run: RunRecord) -> dict[str, Any]:
    return {
        "id": run.id,
        "workspace_id": run.workspace_id,
        "pipeline_version_id": run.pipeline_version_id,
        "state": run.state.value,
        "attempt": run.attempt,
        "retry_category": run.retry_category.value if run.retry_category else None,
        "cancel_requested": run.cancel_requested,
        "created_at": run.created_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "parent_run_id": run.parent_run_id,
        "error": run.error,
    }


def _event_response(event: StoredEvent) -> dict[str, Any]:
    return {
        "run_id": event.run_id,
        "sequence": event.sequence,
        "event_type": event.event_type,
        "payload": event.payload,
        "occurred_at": event.occurred_at,
    }


def _schedule_response(schedule: ScheduleRecord) -> dict[str, Any]:
    return {
        "id": schedule.id,
        "workspace_id": schedule.workspace_id,
        "pipeline_version_id": schedule.pipeline_version_id,
        "trigger_kind": schedule.trigger_kind,
        "trigger_spec": schedule.trigger_spec,
        "timezone": schedule.timezone,
        "enabled": schedule.enabled,
        "next_run_at": schedule.next_run_at,
        "created_at": schedule.created_at,
        "updated_at": schedule.updated_at,
    }


async def _stream_events(
    request: Request,
    service: ControlPlaneService,
    auth: AuthContext,
    workspace_id: str,
    run_id: str,
    after: int,
    *,
    poll_seconds: float,
    heartbeat_seconds: float,
) -> AsyncIterator[str]:
    last_sequence = after
    last_write = time.monotonic()
    while not await request.is_disconnected():
        events = service.events(auth, workspace_id, run_id, after=last_sequence)
        if events and events[0].sequence > last_sequence + 1:
            yield f"event: gap\ndata: {json.dumps({'after': last_sequence, 'next': events[0].sequence})}\n\n"
        for event in events:
            payload = json.dumps(_event_response(event), default=str, separators=(",", ":"))
            yield f"id: {event.sequence}\nevent: {event.event_type}\ndata: {payload}\n\n"
            last_sequence = event.sequence
            last_write = time.monotonic()
        if time.monotonic() - last_write >= heartbeat_seconds:
            yield ": heartbeat\n\n"
            last_write = time.monotonic()
        await asyncio.sleep(poll_seconds)
