"""Tests for RestApiSourceConnector (via registry)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestRestApiSourceConnector:
    def test_stream_basic(self) -> None:
        from loafer.connectors.registry import RestApiSourceConnector

        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        mock_response.headers = {}
        mock_response.links = {}
        mock_client = MagicMock()
        mock_client.request.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            conn = RestApiSourceConnector("https://api.example.com/users")
            conn.connect()
            rows = list(conn.stream(chunk_size=100))

        assert len(rows) == 1
        assert rows[0][0]["id"] == 1
        assert rows[0][0]["name"] == "Alice"

    def test_non_200_response_raises(self) -> None:
        from loafer.connectors.registry import RestApiSourceConnector
        from loafer.exceptions import ExtractionError

        mock_response = MagicMock()
        mock_response.is_success = False
        mock_response.status_code = 404
        mock_response.text = "Not found"
        mock_client = MagicMock()
        mock_client.request.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            conn = RestApiSourceConnector("https://api.example.com/users")
            conn.connect()
            with pytest.raises(ExtractionError, match="404"):
                list(conn.stream(100))

    def test_non_json_response_raises(self) -> None:
        from loafer.connectors.registry import RestApiSourceConnector
        from loafer.exceptions import ExtractionError

        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.side_effect = ValueError("not JSON")
        mock_response.headers = {"Content-Type": "text/html"}
        mock_response.links = {}
        mock_client = MagicMock()
        mock_client.request.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            conn = RestApiSourceConnector("https://api.example.com/users")
            conn.connect()
            with pytest.raises(ExtractionError, match="not JSON"):
                list(conn.stream(100))

    def test_response_key_extraction(self) -> None:
        from loafer.connectors.registry import RestApiSourceConnector

        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {
            "data": [{"id": 1}, {"id": 2}],
            "meta": {"total": 2},
        }
        mock_response.headers = {}
        mock_response.links = {}
        mock_client = MagicMock()
        mock_client.request.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            conn = RestApiSourceConnector("https://api.example.com/users", response_key="data")
            conn.connect()
            rows = list(conn.stream(100))

        assert rows[0][0]["id"] == 1
        assert rows[0][1]["id"] == 2

    def test_response_key_missing_raises(self) -> None:
        from loafer.connectors.registry import RestApiSourceConnector
        from loafer.exceptions import ExtractionError

        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {"users": []}
        mock_response.headers = {}
        mock_response.links = {}
        mock_client = MagicMock()
        mock_client.request.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            conn = RestApiSourceConnector("https://api.example.com/users", response_key="data")
            conn.connect()
            with pytest.raises(ExtractionError, match="no key 'data'"):
                list(conn.stream(100))

    def test_response_not_a_list_raises(self) -> None:
        from loafer.connectors.registry import RestApiSourceConnector
        from loafer.exceptions import ExtractionError

        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {"count": 42}
        mock_response.headers = {}
        mock_response.links = {}
        mock_client = MagicMock()
        mock_client.request.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            conn = RestApiSourceConnector("https://api.example.com/users")
            conn.connect()
            with pytest.raises(ExtractionError, match="not a list"):
                list(conn.stream(100))

    def test_bearer_auth_header(self) -> None:
        from loafer.connectors.registry import RestApiSourceConnector

        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [{"id": 1}]
        mock_response.headers = {}
        mock_response.links = {}
        mock_client = MagicMock()
        mock_client.request.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            conn = RestApiSourceConnector(
                "https://api.example.com/users", auth_token="secret-token"
            )
            conn.connect()
            list(conn.stream(100))

        mock_client.request.assert_called_once()
        headers = mock_client.request.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer secret-token"

    def test_rate_limit_retry(self) -> None:
        from loafer.connectors.registry import RestApiSourceConnector

        rate_limited = MagicMock()
        rate_limited.is_success = False
        rate_limited.status_code = 429
        rate_limited.headers = {"Retry-After": "1"}

        success = MagicMock()
        success.is_success = True
        success.json.return_value = [{"id": 1}]
        success.headers = {}
        success.links = {}

        mock_client = MagicMock()
        mock_client.request.side_effect = [rate_limited, success]

        with patch("httpx.Client", return_value=mock_client), patch("time.sleep"):
            conn = RestApiSourceConnector("https://api.example.com/users")
            conn.connect()
            rows = list(conn.stream(100))

        assert len(rows) == 1
        assert mock_client.request.call_count == 2

    def test_timeout_raises(self) -> None:
        from loafer.connectors.registry import RestApiSourceConnector
        from loafer.exceptions import ExtractionError

        mock_response = MagicMock()
        mock_response.is_success = False
        mock_response.status_code = 408
        mock_response.text = "Request timeout"
        mock_client = MagicMock()
        mock_client.request.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            conn = RestApiSourceConnector("https://api.example.com/users", timeout=5)
            conn.connect()
            with pytest.raises(ExtractionError, match="408"):
                list(conn.stream(100))

    def test_stream_before_connect_raises(self) -> None:
        from loafer.connectors.registry import RestApiSourceConnector
        from loafer.exceptions import ExtractionError

        conn = RestApiSourceConnector("https://api.example.com/users")
        with pytest.raises(ExtractionError, match="connect"):
            list(conn.stream(100))

    def test_chunking(self) -> None:
        from loafer.connectors.registry import RestApiSourceConnector

        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [{"id": i} for i in range(10)]
        mock_response.headers = {}
        mock_response.links = {}
        mock_client = MagicMock()
        mock_client.request.return_value = mock_response

        with patch("httpx.Client", return_value=mock_client):
            conn = RestApiSourceConnector("https://api.example.com/users")
            conn.connect()
            chunks = list(conn.stream(chunk_size=3))

        assert len(chunks) == 4
        assert sum(len(c) for c in chunks) == 10
