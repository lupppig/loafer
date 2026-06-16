"""Tests for upsert write mode: config validation and SQL generation (no DB)."""

from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

import pytest

from loafer.adapters.targets.postgres import _build_upsert_sql
from loafer.config import load_config
from loafer.exceptions import ConfigError

if TYPE_CHECKING:
    from pathlib import Path


def _write(tmp_path: Path, target_block: str) -> Path:
    p = tmp_path / "pipeline.yaml"
    p.write_text(
        textwrap.dedent(
            f"""
            source:
              type: postgres
              url: postgresql://u:p@localhost:5432/db
              query: SELECT * FROM orders
            target:
            {textwrap.indent(textwrap.dedent(target_block), "              ")}
            transform:
              type: ai
              instruction: noop
            """
        ),
        encoding="utf-8",
    )
    return p


class TestUpsertConfig:
    def test_key_string_is_normalised_to_list(self, tmp_path: Path) -> None:
        cfg = load_config(
            _write(
                tmp_path,
                """
                type: postgres
                url: postgresql://u:p@localhost:5432/db
                table: orders
                write_mode: upsert
                key: order_id
                """,
            )
        )
        assert cfg.target.key == ["order_id"]

    def test_multi_column_key(self, tmp_path: Path) -> None:
        cfg = load_config(
            _write(
                tmp_path,
                """
                type: postgres
                url: postgresql://u:p@localhost:5432/db
                table: orders
                write_mode: upsert
                key: [tenant_id, order_id]
                """,
            )
        )
        assert cfg.target.key == ["tenant_id", "order_id"]

    def test_upsert_without_key_errors(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="requires a non-empty 'key'"):
            load_config(
                _write(
                    tmp_path,
                    """
                    type: postgres
                    url: postgresql://u:p@localhost:5432/db
                    table: orders
                    write_mode: upsert
                    """,
                )
            )

    def test_mongo_upsert_requires_key(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="requires a non-empty 'key'"):
            load_config(
                _write(
                    tmp_path,
                    """
                    type: mongo
                    url: mongodb://localhost:27017
                    database: db
                    collection: orders
                    write_mode: upsert
                    """,
                )
            )

    def test_append_does_not_require_key(self, tmp_path: Path) -> None:
        cfg = load_config(
            _write(
                tmp_path,
                """
                type: postgres
                url: postgresql://u:p@localhost:5432/db
                table: orders
                write_mode: append
                """,
            )
        )
        assert cfg.target.write_mode == "append"
        assert cfg.target.key is None


class TestBuildUpsertSql:
    def test_updates_non_key_columns_from_excluded(self) -> None:
        sql = _build_upsert_sql("orders", ["id", "name", "amount"], ["id"])
        assert 'ON CONFLICT ("id")' in sql
        assert '"name" = EXCLUDED."name"' in sql
        assert '"amount" = EXCLUDED."amount"' in sql
        # The key column itself is never in the SET clause.
        assert '"id" = EXCLUDED."id"' not in sql

    def test_composite_key(self) -> None:
        sql = _build_upsert_sql("t", ["a", "b", "v"], ["a", "b"])
        assert 'ON CONFLICT ("a", "b")' in sql
        assert '"v" = EXCLUDED."v"' in sql

    def test_all_columns_are_keys_do_nothing(self) -> None:
        sql = _build_upsert_sql("t", ["a", "b"], ["a", "b"])
        assert sql.endswith("DO NOTHING")
