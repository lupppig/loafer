"""Incremental loading — cursor/watermark state and query helpers.

Loafer remembers the highest cursor value (e.g. ``updated_at``) it has seen for
a pipeline and, on the next run, extracts only rows greater than that watermark.
State is persisted in a JSON file next to the config so it survives restarts and
the scheduler.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def state_path_for(config_path: str | Path) -> Path:
    """Return the state-file path for a given config: ``<stem>.loafer-state.json``."""
    p = Path(config_path)
    return p.parent / f"{p.stem}.loafer-state.json"


class StateStore:
    """Persisted cursor watermarks, keyed by pipeline, in a JSON file.

    Layout: ``{"<key>": {"cursor": <value>, "updated_at": "<iso>"}}``.
    A missing or unreadable file is treated as empty.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def _load(self) -> dict[str, Any]:
        if not self._path.exists():
            return {}
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
        return data if isinstance(data, dict) else {}

    def get_cursor(self, key: str) -> Any | None:
        """Return the stored cursor for *key*, or ``None`` if absent."""
        entry = self._load().get(key)
        if isinstance(entry, dict):
            return entry.get("cursor")
        return None

    def set_cursor(self, key: str, value: Any) -> None:
        """Persist *value* as the cursor for *key*."""
        data = self._load()
        data[key] = {
            "cursor": value,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def wrap_incremental_query(base_query: str, column: str, placeholder: str, quote: str = '"') -> str:
    """Wrap *base_query* so it only returns rows where *column* exceeds the cursor.

    The cursor value is passed as a bound parameter (the *placeholder*); it is
    never interpolated into the SQL string. *quote* is the engine's identifier
    quote char (``"`` for postgres/sqlite, `` ` `` for mysql).
    """
    inner = base_query.strip().rstrip(";")
    escaped_column = column.replace(quote, quote * 2)
    col = f"{quote}{escaped_column}{quote}"
    return (
        f"SELECT * FROM ({inner}) AS _loafer_src "
        f"WHERE _loafer_src.{col} > {placeholder} "
        f"ORDER BY _loafer_src.{col}"
    )


def _sort_key(value: Any) -> tuple[int, Any]:
    """Total-ordering key tolerant of mixed numeric / string cursor values."""
    if isinstance(value, bool):
        return (0, int(value))
    if isinstance(value, (int, float)):
        return (0, float(value))
    return (1, str(value))


def max_cursor(rows: list[dict[str, Any]], column: str, current: Any = None) -> Any:
    """Return the max non-null *column* value across *rows*, never below *current*.

    Returns *current* unchanged when no row carries a usable cursor value.
    """
    best = current
    for row in rows:
        val = row.get(column)
        if val is None:
            continue
        if best is None or _sort_key(val) > _sort_key(best):
            best = val
    return best


def filter_rows_after_cursor(
    rows: list[dict[str, Any]],
    column: str,
    cursor: Any,
) -> list[dict[str, Any]]:
    """Return rows whose non-null cursor value is strictly after *cursor*."""
    if cursor is None:
        return list(rows)
    cursor_key = _sort_key(cursor)
    return [
        row for row in rows if row.get(column) is not None and _sort_key(row[column]) > cursor_key
    ]
