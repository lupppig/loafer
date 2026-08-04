"""Security and contract tests for the HTTPS-only control plane."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import insert

from loafer.adapters import metadata_schema as schema
from loafer.adapters.metadata import SqlMetadataStore
from loafer.control_plane.app import ControlPlaneSettings, create_app
from loafer.control_plane.auth import (
    AuthenticationError,
    BetterAuthJWTVerifier,
    StaticTokenVerifier,
)
from loafer.control_plane.client import HTTPSControlPlaneClient
from loafer.control_plane.domain import AuthContext
from loafer.metadata import StoredEvent


class ASGIClient:
    """Small synchronous facade over HTTPX's async ASGI transport."""

    def __init__(self, app: object, *, base_url: str) -> None:
        self.app = app
        self.base_url = base_url

    def request(self, method: str, url: str, **kwargs: object) -> httpx.Response:
        async def send() -> httpx.Response:
            transport = httpx.ASGITransport(app=self.app)  # type: ignore[arg-type]
            async with httpx.AsyncClient(transport=transport, base_url=self.base_url) as client:
                return await client.request(method, url, **kwargs)

        return asyncio.run(send())

    def get(self, url: str, **kwargs: object) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: object) -> httpx.Response:
        return self.request("POST", url, **kwargs)


@pytest.fixture()
def api(tmp_path: Path) -> tuple[ASGIClient, SqlMetadataStore]:
    store = SqlMetadataStore(f"sqlite:///{tmp_path / 'control.db'}")
    app = create_app(
        settings=ControlPlaneSettings(
            issuer="https://auth.test",
            audience="https://api.test",
            jwks_url="https://auth.test/api/auth/jwks",
            allowed_origins=("https://app.test",),
            rate_limit_requests=1_000,
        ),
        store=store,
        verifier=StaticTokenVerifier(
            {
                "owner-token": AuthContext("owner", global_roles=frozenset({"admin"})),
                "viewer-token": AuthContext("viewer"),
                "outsider-token": AuthContext("outsider"),
            }
        ),
    )
    client = ASGIClient(app, base_url="https://api.test")
    yield client, store
    store.close()


def _headers(token: str = "owner-token", **extra: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", **extra}


def _bootstrap(client: ASGIClient) -> str:
    response = client.post(
        "/api/v1/bootstrap",
        headers=_headers(),
        json={
            "organization_id": "org-a",
            "workspace_slug": "primary",
            "workspace_name": "Primary",
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


def test_bootstrap_requires_platform_admin(api: tuple[ASGIClient, SqlMetadataStore]) -> None:
    client, _store = api
    response = client.post(
        "/api/v1/bootstrap",
        headers=_headers("viewer-token"),
        json={
            "organization_id": "org-a",
            "workspace_slug": "primary",
            "workspace_name": "Primary",
        },
    )
    assert response.status_code == 403


def _pipeline(client: ASGIClient, workspace_id: str) -> str:
    response = client.post(
        f"/api/v1/workspaces/{workspace_id}/pipelines",
        headers=_headers(),
        json={"pipeline_key": "orders", "document": {"name": "orders", "mode": "etl"}},
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


def test_https_auth_origin_and_cookie_boundaries(api: tuple[ASGIClient, SqlMetadataStore]) -> None:
    client, _store = api

    missing = client.get("/api/v1/workspaces")
    assert missing.status_code == 401
    assert missing.headers["www-authenticate"] == "Bearer"
    assert missing.headers["strict-transport-security"]

    invalid = client.get("/api/v1/workspaces", headers=_headers("not-valid"))
    assert invalid.status_code == 401

    untrusted = client.get(
        "/api/v1/workspaces",
        headers=_headers(Origin="https://evil.example"),
    )
    assert untrusted.status_code == 403
    assert untrusted.headers.get("access-control-allow-origin") != "https://evil.example"

    cookie_mutation = client.post(
        "/api/v1/bootstrap",
        headers={"Cookie": "session=forged"},
        json={"organization_id": "org", "workspace_slug": "x", "workspace_name": "X"},
    )
    assert cookie_mutation.status_code == 403

    insecure = ASGIClient(client.app, base_url="http://api.test")
    assert insecure.get("/healthz").status_code == 400


def test_cross_tenant_guessed_ids_and_role_matrix(api: tuple[ASGIClient, SqlMetadataStore]) -> None:
    client, store = api
    workspace_id = _bootstrap(client)
    version_id = _pipeline(client, workspace_id)
    now = datetime.now(UTC)
    with store.engine.begin() as connection:
        connection.execute(
            insert(schema.workspaces).values(
                id="workspace-b",
                organization_id="org-b",
                slug="secondary",
                name="Secondary",
                created_at=now,
            )
        )
        connection.execute(
            insert(schema.workspace_permissions),
            [
                {
                    "workspace_id": workspace_id,
                    "subject_id": "viewer",
                    "role": "viewer",
                    "created_at": now,
                },
                {
                    "workspace_id": "workspace-b",
                    "subject_id": "outsider",
                    "role": "owner",
                    "created_at": now,
                },
            ],
        )

    created = client.post(
        f"/api/v1/workspaces/{workspace_id}/runs",
        headers=_headers(**{"Idempotency-Key": "create-1"}),
        json={"pipeline_version_id": version_id},
    )
    assert created.status_code == 202, created.text
    run_id = created.json()["id"]

    guessed = client.get(
        f"/api/v1/workspaces/{workspace_id}/runs/{run_id}",
        headers=_headers("outsider-token"),
    )
    assert guessed.status_code == 404

    viewer_read = client.get(
        f"/api/v1/workspaces/{workspace_id}/runs/{run_id}",
        headers=_headers("viewer-token"),
    )
    assert viewer_read.status_code == 200
    viewer_mutation = client.post(
        f"/api/v1/workspaces/{workspace_id}/runs/{run_id}/cancel",
        headers=_headers("viewer-token"),
    )
    assert viewer_mutation.status_code == 403


def test_run_commands_are_idempotent_and_only_enqueue(
    api: tuple[ASGIClient, SqlMetadataStore],
) -> None:
    client, store = api
    workspace_id = _bootstrap(client)
    version_id = _pipeline(client, workspace_id)
    path = f"/api/v1/workspaces/{workspace_id}/runs"
    headers = _headers(**{"Idempotency-Key": "same-command"})

    first = client.post(path, headers=headers, json={"pipeline_version_id": version_id})
    repeated = client.post(path, headers=headers, json={"pipeline_version_id": version_id})

    assert first.status_code == repeated.status_code == 202
    assert first.json()["id"] == repeated.json()["id"]
    assert first.json()["state"] == "queued"
    assert store.claim_run("test-worker", timedelta(seconds=30)) is not None


def test_secret_values_are_rejected_and_never_serialized(
    api: tuple[ASGIClient, SqlMetadataStore],
) -> None:
    client, _store = api
    workspace_id = _bootstrap(client)

    embedded = client.post(
        f"/api/v1/workspaces/{workspace_id}/pipelines",
        headers=_headers(),
        json={
            "pipeline_key": "unsafe",
            "document": {"source": {"password": "do-not-store"}},
        },
    )
    assert embedded.status_code == 422
    assert "do-not-store" not in embedded.text

    connection = client.post(
        f"/api/v1/workspaces/{workspace_id}/connections",
        headers=_headers(),
        json={
            "name": "warehouse",
            "connector_type": "postgres",
            "secret_reference": "vault:loafer/workspaces/primary/warehouse",
            "metadata": {"host_label": "primary"},
        },
    )
    assert connection.status_code == 201, connection.text
    body = connection.json()
    assert body["has_secret_reference"] is True
    assert "secret_reference" not in body
    assert "vault:loafer" not in connection.text

    schema_document = client.get("/api/v1/openapi.json").text.lower()
    assert "password" not in schema_document
    assert "api_key" not in schema_document


def test_remote_client_rejects_non_https_urls() -> None:
    with pytest.raises(ValueError, match="HTTPS"):
        HTTPSControlPlaneClient("http://localhost:9443", "token")


def test_better_auth_jwt_verifier_rejects_expiry_and_wrong_audience() -> None:
    private_key = Ed25519PrivateKey.generate()
    verifier = BetterAuthJWTVerifier(
        jwks_url="https://auth.test/api/auth/jwks",
        issuer="https://auth.test",
        audience="https://api.test",
    )
    verifier._jwks = SimpleNamespace(  # type: ignore[attr-defined]
        get_signing_key_from_jwt=lambda _token: SimpleNamespace(key=private_key.public_key())
    )
    now = int(time.time())

    def token(*, expiry: int, audience: str = "https://api.test", role: str = "admin") -> str:
        return jwt.encode(
            {
                "sub": "user-1",
                "iss": "https://auth.test",
                "aud": audience,
                "iat": now - 1,
                "exp": expiry,
                "role": role,
            },
            private_key,
            algorithm="EdDSA",
        )

    context = verifier.verify(token(expiry=now + 60))
    assert context.subject_id == "user-1"
    assert context.is_platform_admin
    with pytest.raises(AuthenticationError):
        verifier.verify(token(expiry=now - 60))
    with pytest.raises(AuthenticationError):
        verifier.verify(token(expiry=now + 60, audience="https://other.test"))


def test_sse_formats_sequence_reconnect_gap_and_heartbeat() -> None:
    from loafer.control_plane.app import _stream_events

    event = StoredEvent(
        run_id="run-1",
        sequence=3,
        event_type="run.running",
        payload={"state": "running"},
        occurred_at=datetime.now(UTC),
    )

    class Request:
        async def is_disconnected(self) -> bool:
            return False

    class Service:
        def __init__(self, events: list[StoredEvent]) -> None:
            self.pending = events

        def events(self, *_args: object, **_kwargs: object) -> list[StoredEvent]:
            current, self.pending = self.pending, []
            return current

    async def first(stream: object) -> str:
        return await anext(stream)  # type: ignore[arg-type]

    gap = asyncio.run(
        first(
            _stream_events(
                Request(),
                Service([event]),  # type: ignore[arg-type]
                AuthContext("user-1"),
                "workspace-1",
                "run-1",
                1,
                poll_seconds=0,
                heartbeat_seconds=15,
            )
        )
    )
    assert gap.startswith("event: gap")
    assert '"after": 1' in gap

    heartbeat = asyncio.run(
        first(
            _stream_events(
                Request(),
                Service([]),  # type: ignore[arg-type]
                AuthContext("user-1"),
                "workspace-1",
                "run-1",
                0,
                poll_seconds=0,
                heartbeat_seconds=0,
            )
        )
    )
    assert heartbeat == ": heartbeat\n\n"


def test_control_plane_rate_limit_returns_retry_after(tmp_path: Path) -> None:
    store = SqlMetadataStore(f"sqlite:///{tmp_path / 'limited.db'}")
    app = create_app(
        settings=ControlPlaneSettings(
            issuer="https://auth.test",
            audience="https://api.test",
            jwks_url="https://auth.test/api/auth/jwks",
            allowed_origins=("https://app.test",),
            rate_limit_requests=1,
            rate_limit_window_seconds=30,
        ),
        store=store,
        verifier=StaticTokenVerifier({}),
    )
    client = ASGIClient(app, base_url="https://api.test")
    assert client.get("/healthz").status_code == 200
    limited = client.get("/healthz")
    assert limited.status_code == 429
    assert limited.headers["retry-after"] == "30"
    store.close()
