"""Typed HTTPS client used by CLI and automation; no embedded fallback."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any, Protocol
from urllib.parse import urlparse

import httpx


class ControlPlaneClient(Protocol):
    def create_run(
        self, workspace_id: str, pipeline_version_id: str, *, idempotency_key: str | None = None
    ) -> dict[str, Any]: ...

    def get_run(self, workspace_id: str, run_id: str) -> dict[str, Any]: ...

    def cancel_run(self, workspace_id: str, run_id: str) -> dict[str, Any]: ...


class ControlPlaneClientError(RuntimeError):
    pass


class HTTPSControlPlaneClient:
    """Credential-bearing client for the versioned `loaferd` interface."""

    def __init__(
        self,
        base_url: str,
        access_token: str,
        *,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        parsed = urlparse(base_url)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("Loafer API URL must be an absolute HTTPS URL")
        if not access_token:
            raise ValueError("a Better Auth access token is required")
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
                "User-Agent": "loafer-cli",
            },
            timeout=timeout,
            transport=transport,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> HTTPSControlPlaneClient:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def list_workspaces(self) -> list[dict[str, Any]]:
        return self._request("GET", "/api/v1/workspaces")

    def list_pipelines(self, workspace_id: str) -> list[dict[str, Any]]:
        return self._request("GET", f"/api/v1/workspaces/{workspace_id}/pipelines")

    def register_pipeline(
        self, workspace_id: str, pipeline_key: str, document: dict[str, Any]
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/workspaces/{workspace_id}/pipelines",
            json={"pipeline_key": pipeline_key, "document": document},
        )

    def create_run(
        self,
        workspace_id: str,
        pipeline_version_id: str,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/workspaces/{workspace_id}/runs",
            headers={"Idempotency-Key": idempotency_key or uuid.uuid4().hex},
            json={"pipeline_version_id": pipeline_version_id},
        )

    def get_run(self, workspace_id: str, run_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/v1/workspaces/{workspace_id}/runs/{run_id}")

    def cancel_run(self, workspace_id: str, run_id: str) -> dict[str, Any]:
        return self._request("POST", f"/api/v1/workspaces/{workspace_id}/runs/{run_id}/cancel")

    def iter_events(self, workspace_id: str, run_id: str, *, after: int = 0) -> Iterator[str]:
        headers = {"Accept": "text/event-stream"}
        if after:
            headers["Last-Event-ID"] = str(after)
        with self._client.stream(
            "GET",
            f"/api/v1/workspaces/{workspace_id}/runs/{run_id}/stream",
            headers=headers,
        ) as response:
            self._raise_for_status(response)
            yield from response.iter_lines()

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self._client.request(method, path, **kwargs)
        self._raise_for_status(response)
        return response.json()

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        if response.is_success:
            return
        try:
            body = response.json()
            detail = body.get("detail", response.reason_phrase)
            request_id = body.get("request_id")
        except ValueError:
            detail = response.reason_phrase
            request_id = None
        suffix = f" (request_id={request_id})" if request_id else ""
        raise ControlPlaneClientError(f"Loafer API {response.status_code}: {detail}{suffix}")
