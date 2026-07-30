"""Integration coverage for atomic PostgreSQL run publication."""

from __future__ import annotations

from typing import Any

import pytest

from loafer.adapters.targets.postgres_staging import PostgresStagingTargetConnector

pytestmark = pytest.mark.integration

_TABLE = "public.test_loafer_staged_publication"


def _target(
    postgres_url: str,
    *,
    run_id: str,
    write_mode: str,
    key: list[str] | None = None,
) -> PostgresStagingTargetConnector:
    return PostgresStagingTargetConnector(
        postgres_url,
        _TABLE,
        write_mode,
        key,
        run_id,
    )


def _seed_old_target(pg_conn: Any) -> None:
    cursor = pg_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {_TABLE}")
    cursor.execute(f"CREATE TABLE {_TABLE} (id BIGINT, name TEXT)")
    cursor.execute(f"INSERT INTO {_TABLE} (id, name) VALUES (1, 'old')")


def _rows(pg_conn: Any) -> list[tuple[int, str]]:
    cursor = pg_conn.cursor()
    cursor.execute(f"SELECT id, name FROM {_TABLE} ORDER BY id")
    return cursor.fetchall()


def test_replace_is_invisible_until_atomic_publication(
    postgres_url: str,
    pg_conn: Any,
) -> None:
    _seed_old_target(pg_conn)
    target = _target(postgres_url, run_id="replace-success", write_mode="replace")
    try:
        target.connect()
        target.write_chunk([{"id": 2, "name": "new"}])
        target.write_chunk([{"id": 3, "name": "newer"}])

        assert _rows(pg_conn) == [(1, "old")]

        target.finalize()
        assert _rows(pg_conn) == [(2, "new"), (3, "newer")]
    finally:
        target.disconnect()
        pg_conn.cursor().execute(f"DROP TABLE IF EXISTS {_TABLE}")


def test_disconnect_before_finalize_preserves_target_and_discards_stage(
    postgres_url: str,
    pg_conn: Any,
) -> None:
    _seed_old_target(pg_conn)
    target = _target(postgres_url, run_id="replace-cancelled", write_mode="replace")
    stage = target._staging_table
    try:
        target.connect()
        target.write_chunk([{"id": 2, "name": "unpublished"}])
        target.disconnect()

        assert _rows(pg_conn) == [(1, "old")]
        cursor = pg_conn.cursor()
        cursor.execute("SELECT to_regclass(%s)", (stage,))
        assert cursor.fetchone()[0] is None
    finally:
        target.disconnect()
        pg_conn.cursor().execute(f"DROP TABLE IF EXISTS {_TABLE}")


def test_append_merges_all_staged_rows_in_one_transaction(
    postgres_url: str,
    pg_conn: Any,
) -> None:
    _seed_old_target(pg_conn)
    target = _target(postgres_url, run_id="append-success", write_mode="append")
    try:
        target.connect()
        target.write_chunk([{"id": 2, "name": "new"}])

        assert _rows(pg_conn) == [(1, "old")]

        target.finalize()
        assert _rows(pg_conn) == [(1, "old"), (2, "new")]
    finally:
        target.disconnect()
        pg_conn.cursor().execute(f"DROP TABLE IF EXISTS {_TABLE}")


def test_upsert_is_atomic_and_idempotent_by_key(
    postgres_url: str,
    pg_conn: Any,
) -> None:
    _seed_old_target(pg_conn)
    try:
        for run_id in ("upsert-one", "upsert-two"):
            target = _target(
                postgres_url,
                run_id=run_id,
                write_mode="upsert",
                key=["id"],
            )
            try:
                target.connect()
                target.write_chunk([{"id": 1, "name": "updated"}, {"id": 2, "name": "new"}])
                target.finalize()
            finally:
                target.disconnect()

        assert _rows(pg_conn) == [(1, "updated"), (2, "new")]
    finally:
        pg_conn.cursor().execute(f"DROP TABLE IF EXISTS {_TABLE}")
