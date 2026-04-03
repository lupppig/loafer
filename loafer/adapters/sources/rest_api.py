"""REST API source connector adapter."""

from __future__ import annotations

import time
import urllib.parse
from typing import Any

from loafer.ports.connector import SourceConnector


class RestApiSourceConnector(SourceConnector):
    def __init__(
        self,
        url: str,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
        response_key: str | None = None,
        pagination: dict[str, Any] | None = None,
        auth_token: str | None = None,
        verify_ssl: bool = True,
        timeout: int = 30,
    ) -> None:
        self._url = url
        self._method = method
        self._headers = headers or {}
        self._params = params or {}
        self._body = body
        self._response_key = response_key
        self._pagination = pagination or {}
        self._auth_token = auth_token
        self._verify_ssl = verify_ssl
        self._timeout = timeout
        self._client: Any = None
        self._row_count: int | None = None

    def connect(self) -> None:
        try:
            import httpx
        except ImportError as exc:
            from loafer.exceptions import ExtractionError

            raise ExtractionError("REST API connector requires 'httpx'") from exc

        self._client = httpx.Client(verify=self._verify_ssl, timeout=self._timeout)

    def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    def stream(self, chunk_size: int) -> Any:
        if self._client is None:
            from loafer.exceptions import ExtractionError

            raise ExtractionError("connect() must be called before stream()")

        import httpx

        headers = dict(self._headers)
        if self._auth_token:
            headers["Authorization"] = f"Bearer {self._auth_token}"

        url: str | None = self._url
        total_rows = 0
        chunk: list[dict[str, Any]] = []

        while url:
            try:
                response = self._client.request(
                    self._method,
                    url,
                    headers=headers,
                    params=self._params if url == self._url else None,
                    json=self._body if self._method == "POST" else None,
                )
            except httpx.TimeoutException as exc:
                from loafer.exceptions import ExtractionError

                raise ExtractionError(f"request timed out after {self._timeout}s: {exc}") from exc
            except httpx.HTTPError as exc:
                from loafer.exceptions import ExtractionError

                raise ExtractionError(f"request failed: {exc}") from exc

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "5"))
                time.sleep(retry_after)
                continue

            if not response.is_success:
                from loafer.exceptions import ExtractionError

                raise ExtractionError(
                    f"unexpected status {response.status_code}: {response.text[:200]}"
                )

            try:
                data = response.json()
            except Exception as exc:
                from loafer.exceptions import ExtractionError

                raise ExtractionError(
                    f"response is not JSON: {exc}. Content-Type: {response.headers.get('Content-Type')}"
                ) from exc

            if self._response_key:
                if not isinstance(data, dict) or self._response_key not in data:
                    from loafer.exceptions import ExtractionError

                    raise ExtractionError(f"response has no key '{self._response_key}'")
                data = data[self._response_key]

            if not isinstance(data, list):
                from loafer.exceptions import ExtractionError

                raise ExtractionError(f"response is not a list (got {type(data).__name__})")

            for item in data:
                total_rows += 1
                chunk.append(item)

                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []

            url = self._next_page(response, data)

        if chunk:
            yield chunk

        self._row_count = total_rows

    def _next_page(self, response: Any, data: list[dict[str, Any]]) -> str | None:
        if not self._pagination or not data:
            return None

        key = self._pagination.get("key")
        if key and key in response.json():
            url: str | None = response.json()[key]
            return url

        next_field = self._pagination.get("next", "next")
        if next_field in response.links:
            next_url: str | None = response.links[next_field]["url"]
            return next_url

        if isinstance(data[-1], dict) and "next_cursor" in data[-1]:
            cursor = data[-1]["next_cursor"]
            parsed = urllib.parse.urlparse(self._url)
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{parsed.query}&cursor={cursor}"

        return None

    def count(self) -> int | None:
        return self._row_count
