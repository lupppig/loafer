"""Connector registry — single source of truth for type→connector resolution.

All connector instantiation goes through here. No ``if config.type == ...``
logic should exist outside this module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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


# -- source connector implementations -----------------------------------------


class CsvSourceConnector(SourceConnector):
    def __init__(
        self,
        path: str,
        has_header: bool = True,
        encoding: str = "utf-8",
        column_names: list[str] | None = None,
    ) -> None:
        from loafer.connectors.sources.csv_source import CsvSourceConnector as Impl

        self._impl = Impl(path, has_header, encoding, column_names)

    def connect(self) -> None:
        self._impl.connect()

    def disconnect(self) -> None:
        self._impl.disconnect()

    def stream(self, chunk_size: int) -> Any:
        return self._impl.stream(chunk_size)

    def count(self) -> int | None:
        return self._impl.count()

    def read_all(self) -> list[dict[str, Any]]:
        return self._impl.read_all()


_register_source("csv", CsvSourceConnector)


class ExcelSourceConnector(SourceConnector):
    def __init__(self, path: str, sheet: str | None = None) -> None:
        from loafer.connectors.sources.excel_source import ExcelSourceConnector as Impl

        self._impl = Impl(path, sheet)

    def connect(self) -> None:
        self._impl.connect()

    def disconnect(self) -> None:
        self._impl.disconnect()

    def stream(self, chunk_size: int) -> Any:
        return self._impl.stream(chunk_size)

    def count(self) -> int | None:
        return self._impl.count()

    def read_all(self) -> list[dict[str, Any]]:
        return self._impl.read_all()


_register_source("excel", ExcelSourceConnector)


class PostgresSourceConnector(SourceConnector):
    def __init__(self, url: str, query: str, timeout: int = 30) -> None:
        self._url = url
        self._query = query
        self._timeout = timeout
        self._conn: Any = None
        self._cursor: Any = None
        self._row_count: int | None = None

    def connect(self) -> None:
        try:
            import psycopg2
        except ImportError as exc:
            raise _import_error("PostgresSourceConnector", "psycopg2-binary")() from exc

        try:
            self._conn = psycopg2.connect(self._url, connect_timeout=self._timeout)
        except psycopg2.Error as exc:
            from loafer.exceptions import ExtractionError

            raise ExtractionError(f"failed to connect to PostgreSQL: {exc}") from exc

        try:
            self._conn.set_client_encoding("UTF8")
            tmp_cursor = self._conn.cursor()
            tmp_cursor.execute(f"SET statement_timeout = '{self._timeout * 1000}'")
            tmp_cursor.close()
        except psycopg2.Error as exc:
            self._conn.close()
            from loafer.exceptions import ExtractionError

            raise ExtractionError(
                f"failed to set timeout (timeout={self._timeout}s): {exc}"
            ) from exc

        try:
            cursor = self._conn.cursor(name="loafer_stream")
            cursor.itersize = 500
            cursor.execute(self._query)
            self._cursor = cursor
            self._row_count = cursor.rowcount if cursor.rowcount >= 0 else None
        except psycopg2.Error as exc:
            self._conn.close()
            from loafer.exceptions import ExtractionError

            raise ExtractionError(f"query failed (timeout={self._timeout}s): {exc}") from exc

    def disconnect(self) -> None:
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._conn:
            self._conn.close()
            self._conn = None

    def stream(self, chunk_size: int) -> Any:
        if self._cursor is None:
            from loafer.exceptions import ExtractionError

            raise ExtractionError("connect() must be called before stream()")

        chunk: list[dict[str, Any]] = []
        total_rows = 0

        for row in self._cursor:
            total_rows += 1
            chunk.append(self._row_to_dict(row))

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        if chunk:
            yield chunk

        self._row_count = total_rows

    def _row_to_dict(self, row: tuple[Any, ...]) -> dict[str, Any]:
        if self._cursor is None:
            return {}
        return {
            col.name: self._convert_value(val, col.type_code)
            for col, val in zip(self._cursor.description, row, strict=False)
        }

    def _convert_value(self, value: Any, type_code: int) -> Any:
        if value is None:
            return None
        if isinstance(value, (int, float, str, bool)):
            return value
        try:
            from decimal import Decimal

            if isinstance(value, Decimal):
                return float(value)
        except ImportError:
            pass
        try:
            import uuid

            if isinstance(value, uuid.UUID):
                return str(value)
        except ImportError:
            pass
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    def count(self) -> int | None:
        return self._row_count


_register_source("postgres", PostgresSourceConnector)


class MySQLSourceConnector(SourceConnector):
    def __init__(self, url: str, query: str, timeout: int = 30) -> None:
        self._url = url
        self._query = query
        self._timeout = timeout
        self._conn: Any = None
        self._cursor: Any = None
        self._row_count: int | None = None
        self._description: tuple[Any, ...] | None = None

    def connect(self) -> None:
        try:
            import pymysql
        except ImportError as exc:
            raise _import_error("MySQLSourceConnector", "pymysql")() from exc

        parsed = _resolve_url(self._url)
        try:
            self._conn = pymysql.connect(
                host=parsed["host"],
                port=parsed["port"] or 3306,
                user=parsed["username"],
                password=parsed["password"],
                database=parsed["database"],
                connect_timeout=self._timeout,
            )
        except pymysql.Error as exc:
            from loafer.exceptions import ExtractionError

            raise ExtractionError(f"failed to connect to MySQL: {exc}") from exc

        try:
            self._cursor = self._conn.cursor()
            self._cursor.execute(self._query)
            self._description = self._cursor.description
            self._row_count = self._cursor.rowcount if self._cursor.rowcount >= 0 else None
        except pymysql.Error as exc:
            self._conn.close()
            from loafer.exceptions import ExtractionError

            raise ExtractionError(f"query failed (timeout={self._timeout}s): {exc}") from exc

    def disconnect(self) -> None:
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._conn:
            self._conn.close()
            self._conn = None

    def stream(self, chunk_size: int) -> Any:
        if self._cursor is None:
            from loafer.exceptions import ExtractionError

            raise ExtractionError("connect() must be called before stream()")

        chunk: list[dict[str, Any]] = []
        total_rows = 0

        while True:
            rows = self._cursor.fetchmany(chunk_size)
            if not rows:
                break
            for row in rows:
                total_rows += 1
                chunk.append(self._row_to_dict(row))

            if chunk:
                yield chunk
                chunk = []

        self._row_count = total_rows

    def _row_to_dict(self, row: tuple[Any, ...]) -> dict[str, Any]:
        if self._description is None:
            return {}
        return {
            col[0]: self._convert_value(val)
            for col, val in zip(self._description, row, strict=False)
        }

    def _convert_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (int, float, str, bool)):
            return value
        try:
            from decimal import Decimal

            if isinstance(value, Decimal):
                return float(value)
        except ImportError:
            pass
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    def count(self) -> int | None:
        return self._row_count


_register_source("mysql", MySQLSourceConnector)


class MongoSourceConnector(SourceConnector):
    def __init__(
        self,
        url: str,
        database: str,
        collection: str,
        filter_doc: dict[str, Any] | None = None,
    ) -> None:
        self._url = url
        self._database = database
        self._collection = collection
        self._filter = filter_doc or {}
        self._client: Any = None
        self._coll: Any = None
        self._row_count: int | None = None

    def connect(self) -> None:
        try:
            import pymongo
        except ImportError as exc:
            raise _import_error("MongoSourceConnector", "pymongo")() from exc

        try:
            self._client = pymongo.MongoClient(self._url, serverSelectionTimeoutMS=30000)
            self._client.admin.command("ping")
        except pymongo.errors.ConnectionFailure as exc:
            from loafer.exceptions import ExtractionError

            raise ExtractionError(f"failed to connect to MongoDB: {exc}") from exc

        db = self._client[self._database]
        if self._collection not in db.list_collection_names():
            from loafer.exceptions import ExtractionError

            raise ExtractionError(
                f"collection '{self._collection}' not found in database '{self._database}'"
            )
        self._coll = db[self._collection]

        try:
            self._row_count = self._coll.count_documents(self._filter)
        except pymongo.errors.PyMongoError:
            self._row_count = None

    def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
        self._coll = None

    def stream(self, chunk_size: int) -> Any:
        if self._coll is None:
            from loafer.exceptions import ExtractionError

            raise ExtractionError("connect() must be called before stream()")

        cursor = self._coll.find(self._filter, batch_size=chunk_size)
        chunk: list[dict[str, Any]] = []

        for doc in cursor:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
            chunk.append(doc)

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        if chunk:
            yield chunk

    def count(self) -> int | None:
        return self._row_count


_register_source("mongo", MongoSourceConnector)


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
            raise _import_error("RestApiSourceConnector", "httpx")() from exc

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
                import time

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
            import urllib.parse

            parsed = urllib.parse.urlparse(self._url)
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{parsed.query}&cursor={cursor}"

        return None

    def count(self) -> int | None:
        return self._row_count


_register_source("rest_api", RestApiSourceConnector)


# -- target connector implementations ------------------------------------------


class CsvTargetConnector(TargetConnector):
    def __init__(self, path: str, write_mode: str = "overwrite") -> None:
        from loafer.connectors.targets.csv_target import CsvTargetConnector as Impl

        self._impl = Impl(path, write_mode)

    def connect(self) -> None:
        self._impl.connect()

    def disconnect(self) -> None:
        self._impl.disconnect()

    def write_chunk(self, chunk: list[dict[str, Any]]) -> int:
        return self._impl.write_chunk(chunk)

    def finalize(self) -> None:
        self._impl.finalize()


_register_target("csv", CsvTargetConnector)


class JsonTargetConnector(TargetConnector):
    def __init__(self, path: str, write_mode: str = "overwrite") -> None:
        from loafer.connectors.targets.json_target import JsonTargetConnector as Impl

        self._impl = Impl(path, write_mode)

    def connect(self) -> None:
        self._impl.connect()

    def disconnect(self) -> None:
        self._impl.disconnect()

    def write_chunk(self, chunk: list[dict[str, Any]]) -> int:
        return self._impl.write_chunk(chunk)

    def finalize(self) -> None:
        self._impl.finalize()


_register_target("json", JsonTargetConnector)


class PostgresTargetConnector(TargetConnector):
    def __init__(self, url: str, table: str, write_mode: str = "append") -> None:
        self._url = url
        self._table = table
        self._write_mode = write_mode
        self._conn: Any = None
        self._cursor: Any = None
        self._rows_written = 0
        self._columns: list[str] = []

    def connect(self) -> None:
        try:
            import psycopg2
        except ImportError as exc:
            raise _import_error("PostgresTargetConnector", "psycopg2-binary")() from exc

        try:
            self._conn = psycopg2.connect(self._url)
            self._conn.autocommit = False
        except psycopg2.Error as exc:
            from loafer.exceptions import LoadError

            raise LoadError(f"failed to connect to PostgreSQL: {exc}") from exc

        self._cursor = self._conn.cursor()

    def disconnect(self) -> None:
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._conn:
            self._conn.close()
            self._conn = None

    def write_chunk(self, chunk: list[dict[str, Any]]) -> int:
        if self._conn is None or self._cursor is None:
            from loafer.exceptions import LoadError

            raise LoadError("connect() must be called before write_chunk()")

        if not chunk:
            return 0

        import psycopg2
        import psycopg2.extras

        table_exists = self._table_exists()
        if not table_exists:
            self._columns = list(chunk[0].keys())
            self._create_table(chunk[0])
        elif not self._columns:
            self._columns = list(chunk[0].keys())

        batch_size = min(len(chunk), 100)
        rows_in_batch = 0

        for i in range(0, len(chunk), batch_size):
            batch = chunk[i : i + batch_size]
            cols = list(batch[0].keys()) if not self._columns else self._columns
            col_names = ", ".join(cols)

            query = f"INSERT INTO {self._table} ({col_names}) VALUES %s"
            values = [self._serialize_value(row, cols) for row in batch]

            try:
                psycopg2.extras.execute_values(
                    self._cursor, query, values, template=None, page_size=batch_size
                )
                self._conn.commit()
                rows_in_batch += len(batch)
            except psycopg2.Error as exc:
                self._conn.rollback()
                from loafer.exceptions import LoadError

                raise LoadError(f"batch insert failed ({len(batch)} rows): {exc}") from exc

        self._rows_written += rows_in_batch
        return rows_in_batch

    def _table_exists(self) -> bool:
        import psycopg2

        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            )
        """
        try:
            self._cursor.execute(query, (self._table,))
            return bool(self._cursor.fetchone()[0])
        except psycopg2.Error:
            return False

    def _create_table(self, sample_row: dict[str, Any]) -> None:
        import psycopg2

        col_defs: list[str] = []
        for col, val in sample_row.items():
            pg_type = self._infer_pg_type(val)
            col_defs.append(f'"{col}" {pg_type}')

        query = f"CREATE TABLE {self._table} ({', '.join(col_defs)})"
        try:
            self._cursor.execute(query)
            self._conn.commit()
        except psycopg2.Error as exc:
            self._conn.rollback()
            from loafer.exceptions import LoadError

            raise LoadError(f"failed to create table {self._table}: {exc}") from exc

    def _infer_pg_type(self, value: Any) -> str:
        if value is None:
            return "TEXT"
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int):
            return "BIGINT"
        if isinstance(value, float):
            return "DOUBLE PRECISION"
        if isinstance(value, (dict, list)):
            return "JSONB"
        return "TEXT"

    def _serialize_value(self, row: dict[str, Any], cols: list[str]) -> tuple[Any, ...]:
        result: list[Any] = []
        for c in cols:
            v: Any | None = row.get(c)
            if isinstance(v, (dict, list)):
                import json

                result.append(json.dumps(v))
            else:
                result.append(v)
        return tuple(result)

    def finalize(self) -> None:
        if self._conn:
            try:
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise


_register_target("postgres", PostgresTargetConnector)
