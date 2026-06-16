"""Tests for incremental loading: state store, query wrapping, cursor math, config."""

from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

from loafer.config import load_config
from loafer.core.incremental import (
    StateStore,
    max_cursor,
    state_path_for,
    wrap_incremental_query,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestStateStore:
    def test_missing_file_returns_none(self, tmp_path: Path) -> None:
        store = StateStore(tmp_path / "nope.json")
        assert store.get_cursor("orders") is None

    def test_roundtrip(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        store = StateStore(path)
        store.set_cursor("orders", "2024-01-01T00:00:00")
        assert store.get_cursor("orders") == "2024-01-01T00:00:00"
        # A fresh instance reads the same persisted value.
        assert StateStore(path).get_cursor("orders") == "2024-01-01T00:00:00"

    def test_keys_are_independent(self, tmp_path: Path) -> None:
        store = StateStore(tmp_path / "state.json")
        store.set_cursor("a", 5)
        store.set_cursor("b", 9)
        assert store.get_cursor("a") == 5
        assert store.get_cursor("b") == 9

    def test_corrupt_file_treated_as_empty(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        path.write_text("not json{{{", encoding="utf-8")
        assert StateStore(path).get_cursor("orders") is None

    def test_state_path_sits_next_to_config(self, tmp_path: Path) -> None:
        cfg = tmp_path / "daily.yaml"
        assert state_path_for(cfg) == tmp_path / "daily.loafer-state.json"


class TestWrapIncrementalQuery:
    def test_postgres_placeholder_and_quote(self) -> None:
        q = wrap_incremental_query("SELECT * FROM orders", "updated_at", "%s")
        assert "%s" in q
        assert '"updated_at"' in q
        assert q.lower().startswith("select * from (select * from orders)")
        assert "order by" in q.lower()

    def test_sqlite_placeholder(self) -> None:
        q = wrap_incremental_query("SELECT * FROM t", "id", "?")
        assert "> ?" in q

    def test_mysql_backtick_quote(self) -> None:
        q = wrap_incremental_query("SELECT * FROM t", "ts", "%s", quote="`")
        assert "`ts`" in q

    def test_trailing_semicolon_stripped(self) -> None:
        q = wrap_incremental_query("SELECT * FROM t;", "id", "?")
        assert "t;" not in q


class TestMaxCursor:
    def test_returns_current_when_no_rows(self) -> None:
        assert max_cursor([], "id", current=7) == 7

    def test_ignores_nulls(self) -> None:
        rows = [{"id": None}, {"id": 3}, {"id": None}]
        assert max_cursor(rows, "id") == 3

    def test_never_downgrades_below_current(self) -> None:
        rows = [{"id": 1}, {"id": 2}]
        assert max_cursor(rows, "id", current=10) == 10

    def test_iso_string_ordering(self) -> None:
        rows = [
            {"ts": "2024-01-01T00:00:00"},
            {"ts": "2024-03-01T00:00:00"},
            {"ts": "2024-02-01T00:00:00"},
        ]
        assert max_cursor(rows, "ts") == "2024-03-01T00:00:00"

    def test_missing_column_returns_current(self) -> None:
        assert max_cursor([{"other": 1}], "id", current=None) is None


class TestIncrementalConfig:
    def _write(self, tmp_path: Path, body: str) -> Path:
        p = tmp_path / "pipeline.yaml"
        p.write_text(textwrap.dedent(body), encoding="utf-8")
        return p

    def test_parses_incremental_block(self, tmp_path: Path) -> None:
        path = self._write(
            tmp_path,
            """
            source:
              type: postgres
              url: postgresql://u:p@localhost:5432/db
              query: SELECT * FROM orders
            target:
              type: csv
              path: /tmp/out.csv
            transform:
              type: ai
              instruction: noop
            incremental:
              column: updated_at
              initial: "1970-01-01"
            """,
        )
        cfg = load_config(path)
        assert cfg.incremental is not None
        assert cfg.incremental.column == "updated_at"
        assert cfg.incremental.initial == "1970-01-01"
        assert cfg.incremental.param is None

    def test_incremental_defaults_to_none(self, tmp_path: Path) -> None:
        path = self._write(
            tmp_path,
            """
            source:
              type: postgres
              url: postgresql://u:p@localhost:5432/db
              query: SELECT * FROM orders
            target:
              type: csv
              path: /tmp/out.csv
            transform:
              type: ai
              instruction: noop
            """,
        )
        assert load_config(path).incremental is None
