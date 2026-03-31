"""Tests for the connector registry."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestRegistrySourceResolution:
    def test_resolve_csv_source(self) -> None:
        from loafer.connectors.registry import get_source_connector

        with patch("loafer.connectors.sources.csv_source.CsvSourceConnector") as mock_cls:
            mock_cls.return_value = MagicMock()
            config = MagicMock()
            config.type = "csv"
            config.path = "/tmp/data.csv"
            config.has_header = True
            config.encoding = "utf-8"
            config.column_names = None

            conn = get_source_connector(config)
            assert conn.__class__.__name__ == "CsvSourceConnector"
            mock_cls.assert_called_once()

    def test_resolve_excel_source(self) -> None:
        from loafer.connectors.registry import get_source_connector

        with patch("loafer.connectors.sources.excel_source.ExcelSourceConnector") as mock_cls:
            mock_cls.return_value = MagicMock()
            config = MagicMock()
            config.type = "excel"
            config.path = "/tmp/data.xlsx"
            config.sheet = None

            conn = get_source_connector(config)
            assert conn.__class__.__name__ == "ExcelSourceConnector"
            mock_cls.assert_called_once()

    def test_resolve_postgres_source(self) -> None:
        from loafer.connectors.registry import get_source_connector

        with patch("psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            config = MagicMock()
            config.type = "postgres"
            config.url = "postgresql://localhost/db"
            config.query = "SELECT 1"
            config.timeout = 30

            conn = get_source_connector(config)
            assert conn.__class__.__name__ == "PostgresSourceConnector"

    def test_resolve_mongo_source(self) -> None:
        from loafer.connectors.registry import get_source_connector

        with patch("pymongo.MongoClient") as mock_client:
            mock_client.return_value = MagicMock()
            config = MagicMock()
            config.type = "mongo"
            config.url = "mongodb://localhost:27017"
            config.database = "mydb"
            config.collection = "users"
            config.filter = {}

            conn = get_source_connector(config)
            assert conn.__class__.__name__ == "MongoSourceConnector"

    def test_resolve_rest_api_source(self) -> None:
        from loafer.connectors.registry import get_source_connector

        with patch("httpx.Client") as mock_client_cls:
            mock_client_cls.return_value = MagicMock()
            config = MagicMock()
            config.type = "rest_api"
            config.url = "https://api.example.com/data"
            config.method = "GET"
            config.headers = {}
            config.params = {}
            config.body = None
            config.response_key = None
            config.pagination = None
            config.auth_token = None
            config.verify_ssl = True
            config.timeout = 30

            conn = get_source_connector(config)
            assert conn.__class__.__name__ == "RestApiSourceConnector"

    def test_resolve_unknown_source_raises(self) -> None:
        from loafer.connectors.registry import RegistryError, get_source_connector

        config = MagicMock()
        config.type = "unknown_type"

        with pytest.raises(RegistryError, match="unknown_type"):
            get_source_connector(config)


class TestRegistryTargetResolution:
    def test_resolve_csv_target(self) -> None:
        from loafer.connectors.registry import get_target_connector

        with patch("loafer.connectors.targets.csv_target.CsvTargetConnector") as mock_cls:
            mock_cls.return_value = MagicMock()
            config = MagicMock()
            config.type = "csv"
            config.path = "/tmp/out.csv"
            config.write_mode = "overwrite"

            conn = get_target_connector(config)
            assert conn.__class__.__name__ == "CsvTargetConnector"
            mock_cls.assert_called_once()

    def test_resolve_json_target(self) -> None:
        from loafer.connectors.registry import get_target_connector

        with patch("loafer.connectors.targets.json_target.JsonTargetConnector") as mock_cls:
            mock_cls.return_value = MagicMock()
            config = MagicMock()
            config.type = "json"
            config.path = "/tmp/out.json"
            config.write_mode = "overwrite"

            conn = get_target_connector(config)
            assert conn.__class__.__name__ == "JsonTargetConnector"
            mock_cls.assert_called_once()

    def test_resolve_postgres_target(self) -> None:
        from loafer.connectors.registry import get_target_connector

        with patch("psycopg2.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            config = MagicMock()
            config.type = "postgres"
            config.url = "postgresql://localhost/db"
            config.table = "users"
            config.write_mode = "append"

            conn = get_target_connector(config)
            assert conn.__class__.__name__ == "PostgresTargetConnector"

    def test_resolve_unknown_target_raises(self) -> None:
        from loafer.connectors.registry import RegistryError, get_target_connector

        config = MagicMock()
        config.type = "unknown_target"

        with pytest.raises(RegistryError, match="unknown_target"):
            get_target_connector(config)


class TestResolveUrl:
    def test_valid_postgres_url(self) -> None:
        from loafer.connectors.registry import _resolve_url

        result = _resolve_url("postgresql://user:pass@localhost:5432/mydb")
        assert result["scheme"] == "postgresql"
        assert result["host"] == "localhost"
        assert result["port"] == 5432
        assert result["database"] == "mydb"
        assert result["username"] == "user"

    def test_valid_mysql_url(self) -> None:
        from loafer.connectors.registry import _resolve_url

        result = _resolve_url("mysql://root:secret@db.example.com:3306/app")
        assert result["scheme"] == "mysql"
        assert result["host"] == "db.example.com"
        assert result["port"] == 3306
        assert result["database"] == "app"
        assert result["username"] == "root"
        assert result["password"] == "secret"

    def test_non_string_url_raises(self) -> None:
        from loafer.connectors.registry import RegistryError, _resolve_url

        with pytest.raises(RegistryError, match="malformed"):
            _resolve_url(123)  # type: ignore
