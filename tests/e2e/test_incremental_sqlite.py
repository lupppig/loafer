"""End-to-end incremental loading against a real SQLite source (no Docker).

Validates that a second run only extracts rows past the saved watermark, that
the state file advances, and that --full-refresh re-pulls everything.
"""

from __future__ import annotations

import json
import sqlite3
import textwrap
from typing import TYPE_CHECKING

from loafer.core.incremental import state_path_for
from loafer.runner import run_pipeline

if TYPE_CHECKING:
    from pathlib import Path

_PASSTHROUGH = "def transform(data):\n    return data\n"


def _make_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, seq INTEGER)")
    conn.executemany(
        "INSERT INTO events (name, seq) VALUES (?, ?)",
        [("a", 1), ("b", 2), ("c", 3)],
    )
    conn.commit()
    conn.close()


def _insert(path: Path, rows: list[tuple[str, int]]) -> None:
    conn = sqlite3.connect(path)
    conn.executemany("INSERT INTO events (name, seq) VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


def _write_config(tmp_path: Path, db: Path, out: Path) -> Path:
    transform = tmp_path / "passthrough.py"
    transform.write_text(_PASSTHROUGH, encoding="utf-8")
    cfg = tmp_path / "incremental.yaml"
    cfg.write_text(
        textwrap.dedent(
            f"""
            name: incremental_test
            source:
              type: sqlite
              path: {db}
              query: SELECT * FROM events
            target:
              type: json
              path: {out}
            transform:
              type: custom
              path: {transform}
            incremental:
              column: seq
              initial: 0
            """
        ),
        encoding="utf-8",
    )
    return cfg


def _load_rows(out: Path) -> list[dict]:
    return json.loads(out.read_text(encoding="utf-8"))


def test_second_run_only_pulls_new_rows(tmp_path: Path) -> None:
    db = tmp_path / "events.db"
    out = tmp_path / "out.json"
    _make_db(db)
    cfg = _write_config(tmp_path, db, out)

    # First run: all 3 existing rows.
    state1 = run_pipeline(cfg)
    assert state1["rows_extracted"] == 3
    assert state1["rows_loaded"] == 3

    # Watermark persisted next to the config.
    state_file = state_path_for(cfg)
    assert state_file.exists()
    saved = json.loads(state_file.read_text())
    assert saved["incremental_test"]["cursor"] == 3

    # Add two newer rows, then run again.
    _insert(db, [("d", 4), ("e", 5)])
    state2 = run_pipeline(cfg)
    assert state2["rows_extracted"] == 2
    assert {r["name"] for r in _load_rows(out)} == {"d", "e"}

    # Watermark advanced.
    saved2 = json.loads(state_file.read_text())
    assert saved2["incremental_test"]["cursor"] == 5


def test_no_new_rows_extracts_zero(tmp_path: Path) -> None:
    db = tmp_path / "events.db"
    out = tmp_path / "out.json"
    _make_db(db)
    cfg = _write_config(tmp_path, db, out)

    run_pipeline(cfg)
    state2 = run_pipeline(cfg)
    assert state2["rows_extracted"] == 0


def test_full_refresh_repulls_everything(tmp_path: Path) -> None:
    db = tmp_path / "events.db"
    out = tmp_path / "out.json"
    _make_db(db)
    cfg = _write_config(tmp_path, db, out)

    run_pipeline(cfg)
    _insert(db, [("d", 4)])
    state = run_pipeline(cfg, full_refresh=True)
    assert state["rows_extracted"] == 4
