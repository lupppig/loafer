"""Tests for run-scoped PostgreSQL staging publication."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from loafer.adapters.targets.postgres_staging import PostgresStagingTargetConnector
from loafer.config import PostgresTargetConfig
from loafer.connectors.registry import get_staged_target_connector
from loafer.exceptions import LoadError
from tests.postgres_sql import render_sql


def _connector(
    *,
    table: str = "analytics.events",
    write_mode: str = "replace",
    key: list[str] | None = None,
) -> PostgresStagingTargetConnector:
    connector = PostgresStagingTargetConnector(
        "postgresql://user:pass@localhost/db",
        table,
        write_mode,
        key,
        "run/with unsafe text",
    )
    connector._conn = MagicMock()
    connector._cursor = MagicMock()
    return connector


def test_stage_name_is_safe_and_keeps_target_schema() -> None:
    connector = _connector(table='analytics.events"; DROP TABLE users; --')

    assert connector._staging_table.startswith("analytics._loafer_stage_")
    assert '"' not in connector._staging_table
    assert ";" not in connector._staging_table


def test_create_staging_quotes_adversarial_columns() -> None:
    connector = _connector()

    connector._create_staging({'name"; DROP TABLE users; --': "safe"})

    query = render_sql(connector._cursor.execute.call_args.args[0])
    stage_name = connector._staging_table.split(".", 1)[1]
    assert query == (
        f'CREATE TABLE "analytics"."{stage_name}" ("name""; DROP TABLE users; --" TEXT)'
    )


def test_write_chunk_inserts_only_into_stage_and_commits() -> None:
    connector = _connector()

    with patch("psycopg2.extras.execute_values") as execute_values:
        assert connector.write_chunk([{"id": 1, "name": "Ada"}]) == 1

    query = render_sql(execute_values.call_args.args[1])
    assert query.startswith('INSERT INTO "analytics"."_loafer_stage_')
    assert '"analytics"."events"' not in query
    connector._conn.commit.assert_called_once()


def test_write_chunk_rolls_back_and_wraps_stage_creation_failure() -> None:
    connector = _connector()
    connector._cursor.execute.side_effect = RuntimeError("database unavailable")

    with pytest.raises(LoadError, match="staging batch insert failed"):
        connector.write_chunk([{"id": 1}])

    connector._conn.rollback.assert_called_once()


def test_replace_drops_final_and_renames_stage_in_one_finalize_commit() -> None:
    connector = _connector()
    connector._staging_created = True
    connector._columns = ["id"]

    connector.finalize()

    queries = [render_sql(call.args[0]) for call in connector._cursor.execute.call_args_list]
    assert queries[0] == 'DROP TABLE IF EXISTS "analytics"."events"'
    assert queries[1].startswith('ALTER TABLE "analytics"."_loafer_stage_')
    assert queries[1].endswith(' RENAME TO "events"')
    connector._conn.commit.assert_called_once()
    assert connector._published is True


def test_append_merges_stage_then_drops_it() -> None:
    connector = _connector(write_mode="append")
    connector._staging_created = True
    connector._columns = ["id", "name"]
    connector._table_exists = MagicMock(return_value=True)

    connector.finalize()

    queries = [render_sql(call.args[0]) for call in connector._cursor.execute.call_args_list]
    assert queries[0].startswith('INSERT INTO "analytics"."events" ("id", "name") SELECT ')
    assert queries[1].startswith('DROP TABLE IF EXISTS "analytics"."_loafer_stage_')
    connector._conn.commit.assert_called_once()


def test_upsert_quotes_keys_and_columns() -> None:
    connector = _connector(
        write_mode="upsert",
        key=['id"; DROP TABLE users; --'],
    )
    connector._staging_created = True
    connector._columns = ['id"; DROP TABLE users; --', "name"]
    connector._table_exists = MagicMock(return_value=True)

    connector.finalize()

    queries = [render_sql(call.args[0]) for call in connector._cursor.execute.call_args_list]
    merge = next(query for query in queries if query.startswith("INSERT INTO"))
    assert 'ON CONFLICT ("id""; DROP TABLE users; --")' in merge
    assert '"name" = EXCLUDED."name"' in merge


def test_disconnect_discards_unpublished_stage() -> None:
    connector = _connector()
    connector._staging_created = True
    mock_conn = connector._conn
    mock_cursor = connector._cursor

    connector.disconnect()

    queries = [render_sql(call.args[0]) for call in mock_cursor.execute.call_args_list]
    assert any(query.startswith("DROP TABLE IF EXISTS") for query in queries)
    mock_conn.rollback.assert_called_once()
    mock_conn.close.assert_called_once()


def test_empty_append_requires_an_existing_target_schema() -> None:
    connector = _connector(write_mode="append")
    connector._table_exists = MagicMock(return_value=False)

    with pytest.raises(LoadError, match="no output schema"):
        connector.finalize()


def test_empty_replace_truncates_existing_target() -> None:
    connector = _connector(write_mode="replace")
    connector._table_exists = MagicMock(return_value=True)

    connector.finalize()

    query = render_sql(connector._cursor.execute.call_args.args[0])
    assert query == 'TRUNCATE TABLE "analytics"."events"'
    assert connector._published is True


def test_registry_selects_staging_adapter_for_bounded_postgres() -> None:
    config = PostgresTargetConfig(
        type="postgres",
        url="postgresql://localhost/db",
        table="public.output",
        write_mode="upsert",
        key="id",
    )

    connector = get_staged_target_connector(config, run_id="run-123")

    assert isinstance(connector, PostgresStagingTargetConnector)
    assert connector._key == ["id"]
