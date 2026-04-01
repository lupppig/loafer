"""Integration test fixtures — real database connections.

Tests are marked with ``pytest.mark.integration`` and require running services.
PostgreSQL is expected to be available; MySQL and MongoDB are optional.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

_POSTGRES_URL = os.environ.get(
    "TEST_POSTGRES_URL",
    "postgresql://loafer:loafer@localhost:5432/loafer_dev",
)


def _pg_available() -> bool:
    try:
        import psycopg2

        conn = psycopg2.connect(_POSTGRES_URL, connect_timeout=5)
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def postgres_url() -> str:
    """PostgreSQL connection URL."""
    if not _pg_available():
        pytest.skip(f"PostgreSQL not available at {_POSTGRES_URL}")
    return _POSTGRES_URL


@pytest.fixture()
def pg_conn(postgres_url: str) -> Generator[Any, None, None]:
    """Yield a psycopg2 connection, clean up after."""
    import psycopg2

    conn = psycopg2.connect(postgres_url)
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def pg_test_table(pg_conn: Any) -> Generator[str, None, None]:
    """Create a temporary table, drop it after the test."""
    import psycopg2

    table = "test_loafer_integration"
    cur = pg_conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.execute(
        f"""
        CREATE TABLE {table} (
            id SERIAL PRIMARY KEY,
            name TEXT,
            email TEXT,
            score NUMERIC(10, 2),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """
    )
    try:
        yield table
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        pg_conn.commit()


@pytest.fixture()
def pg_test_tables(pg_conn: Any) -> Generator[dict[str, str], None, None]:
    """Create users + orders test tables, drop after."""
    cur = pg_conn.cursor()
    tables = {
        "users": "test_loafer_users",
        "orders": "test_loafer_orders",
    }
    for t in tables.values():
        cur.execute(f"DROP TABLE IF EXISTS {t}")

    cur.execute(
        f"""
        CREATE TABLE {tables["users"]} (
            id SERIAL PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            country TEXT,
            age INT
        )
    """
    )
    cur.execute(
        f"""
        CREATE TABLE {tables["orders"]} (
            id SERIAL PRIMARY KEY,
            user_id INT REFERENCES {tables["users"]}(id),
            product TEXT,
            quantity INT,
            unit_price NUMERIC(10, 2),
            total_price NUMERIC(10, 2),
            status TEXT DEFAULT 'pending',
            region TEXT
        )
    """
    )
    pg_conn.commit()

    try:
        yield tables
    finally:
        for t in tables.values():
            cur.execute(f"DROP TABLE IF EXISTS {t}")
        pg_conn.commit()


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------

_MYSQL_URL = os.environ.get(
    "TEST_MYSQL_URL",
    "mysql://loafer:loafer@localhost:3306/loafer_source",
)


def _mysql_available() -> bool:
    try:
        import pymysql

        from urllib.parse import urlparse

        parsed = urlparse(_MYSQL_URL)
        conn = pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or 3306,
            user=parsed.username,
            password=parsed.password,
            database=parsed.path.lstrip("/"),
            connect_timeout=5,
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def mysql_url() -> str:
    """MySQL connection URL."""
    if not _mysql_available():
        pytest.skip(f"MySQL not available at {_MYSQL_URL}")
    return _MYSQL_URL


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------

_MONGO_URL = os.environ.get(
    "TEST_MONGO_URL",
    "mongodb://loafer:loafer@localhost:27017",
)
_MONGO_DB = "test_loafer"


def _mongo_available() -> bool:
    try:
        import pymongo

        client = pymongo.MongoClient(_MONGO_URL, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        client.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def mongo_url() -> str:
    """MongoDB connection URL."""
    if not _mongo_available():
        pytest.skip(f"MongoDB not available at {_MONGO_URL}")
    return _MONGO_URL


@pytest.fixture()
def mongo_client(mongo_url: str) -> Generator[Any, None, None]:
    """Yield a pymongo client, clean test DB after."""
    import pymongo

    client = pymongo.MongoClient(mongo_url)
    db = client[_MONGO_DB]
    try:
        yield db
    finally:
        client.drop_database(_MONGO_DB)
        client.close()
