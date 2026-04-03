"""Connector registry — single source of truth for type→connector resolution.

All connector instantiation goes through here. No ``if config.type == ...``
logic should exist outside this module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loafer.adapters.sources.csv_source import CsvSourceConnector as _Csv
from loafer.adapters.sources.excel_source import ExcelSourceConnector as _Excel
from loafer.adapters.sources.mongo import MongoSourceConnector as _Mongo
from loafer.adapters.sources.mysql import MySQLSourceConnector as _MySQL
from loafer.adapters.sources.pdf import PdfSourceConnector as _Pdf
from loafer.adapters.sources.postgres import PostgresSourceConnector as _Postgres
from loafer.adapters.sources.rest_api import RestApiSourceConnector as _Rest
from loafer.adapters.sources.sqlite import SqliteSourceConnector as _Sqlite
from loafer.adapters.targets.csv_target import CsvTargetConnector as _CsvTarget
from loafer.adapters.targets.json_target import JsonTargetConnector as _JsonTarget
from loafer.adapters.targets.mongo import MongoTargetConnector as _MongoTarget
from loafer.adapters.targets.postgres import PostgresTargetConnector as _PgTarget
from loafer.connectors.base import SourceConnector, TargetConnector
from loafer.exceptions import ConnectorError

if TYPE_CHECKING:
    from loafer.config import (
        SourceConfig,
        TargetConfig,
    )


class RegistryError(ConnectorError):
    """No connector registered for the given config type."""


def _import_error(name: str, package: str) -> type[ConnectorError]:
    class _MissingError(ConnectorError):
        def __init__(self) -> None:
            super().__init__(f"{name} requires the '{package}' package")

    return _MissingError


_SOURCE_REGISTRY: dict[str, type[SourceConnector]] = {}
_TARGET_REGISTRY: dict[str, type[TargetConnector]] = {}


def _register_source(type_name: str, cls: type[SourceConnector]) -> None:
    _SOURCE_REGISTRY[type_name] = cls


def _register_target(type_name: str, cls: type[TargetConnector]) -> None:
    _TARGET_REGISTRY[type_name] = cls


_register_source("csv", _Csv)
_register_source("excel", _Excel)
_register_source("postgres", _Postgres)
_register_source("mysql", _MySQL)
_register_source("mongo", _Mongo)
_register_source("rest_api", _Rest)
_register_source("sqlite", _Sqlite)
_register_source("pdf", _Pdf)

_register_target("csv", _CsvTarget)
_register_target("json", _JsonTarget)
_register_target("postgres", _PgTarget)
_register_target("mongo", _MongoTarget)


def get_source_connector(config: SourceConfig) -> SourceConnector:
    """Instantiate the source connector for the given config."""
    connector_cls = _SOURCE_REGISTRY.get(config.type)
    if connector_cls is None:
        raise RegistryError(f"no source connector registered for type '{config.type}'")
    return _build_source(connector_cls, config)


def get_target_connector(config: TargetConfig) -> TargetConnector:
    """Instantiate the target connector for the given config."""
    connector_cls = _TARGET_REGISTRY.get(config.type)
    if connector_cls is None:
        raise RegistryError(f"no target connector registered for type '{config.type}'")
    return _build_target(connector_cls, config)


def _build_source(cls: type[SourceConnector], config: SourceConfig) -> SourceConnector:
    match config.type:
        case "csv":
            return cls(config.path, config.has_header, config.encoding, config.column_names)  # type: ignore[call-arg]
        case "excel":
            return cls(config.path, config.sheet)  # type: ignore[call-arg]
        case "postgres" | "mysql":
            return cls(config.url, config.query, config.timeout)  # type: ignore[call-arg]
        case "mongo":
            return cls(config.url, config.database, config.collection, config.filter)  # type: ignore[call-arg]
        case "rest_api":
            return cls(
                config.url,
                config.method,
                config.headers,
                config.params,
                config.body,
                config.response_key,
                config.pagination,
                config.auth_token,
                config.verify_ssl,
                config.timeout,
            )  # type: ignore[call-arg]
        case "sqlite":
            return cls(config.path, config.query)  # type: ignore[call-arg]
        case "pdf":
            return cls(config.path, config.extract_tables)  # type: ignore[call-arg]
    msg = f"source connector '{config.type}' not implemented"
    raise RegistryError(msg)


def _build_target(cls: type[TargetConnector], config: TargetConfig) -> TargetConnector:
    match config.type:
        case "csv":
            return cls(config.path, config.write_mode)  # type: ignore[call-arg]
        case "json":
            return cls(config.path, config.write_mode)  # type: ignore[call-arg]
        case "postgres":
            return cls(config.url, config.table, config.write_mode)  # type: ignore[call-arg]
        case "mongo":
            return cls(config.url, config.database, config.collection, config.write_mode)  # type: ignore[call-arg]
    msg = f"target connector '{config.type}' not implemented"
    raise RegistryError(msg)


def _resolve_url(url: str) -> dict[str, Any]:
    try:
        from urllib.parse import urlparse
    except ImportError:
        msg = "url parsing requires urllib (stdlib)"
        raise RegistryError(msg) from None

    try:
        parsed = urlparse(url)
    except Exception as exc:
        raise RegistryError(f"malformed connection URL: {url}") from exc

    return {
        "scheme": parsed.scheme,
        "host": parsed.hostname,
        "port": parsed.port,
        "database": parsed.path.lstrip("/") if parsed.path else None,
        "username": parsed.username,
        "password": parsed.password,
    }


# -- backward compatibility: re-export connector classes ----------------------

CsvSourceConnector = _Csv
ExcelSourceConnector = _Excel
PostgresSourceConnector = _Postgres
MySQLSourceConnector = _MySQL
MongoSourceConnector = _Mongo
RestApiSourceConnector = _Rest
SqliteSourceConnector = _Sqlite
PdfSourceConnector = _Pdf

CsvTargetConnector = _CsvTarget
JsonTargetConnector = _JsonTarget
PostgresTargetConnector = _PgTarget
MongoTargetConnector = _MongoTarget
