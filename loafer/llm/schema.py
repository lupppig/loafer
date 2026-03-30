"""Token-efficient schema sampler.

Converts raw data rows into a compact schema representation that is safe
(and cheap) to include in an LLM prompt.  The full dataset is *never*
sent — only column metadata and a handful of sample values.
"""

from __future__ import annotations

import re
from typing import Any

# Common ISO-8601 datetime patterns.
_DATETIME_PATTERNS: list[re.Pattern[str]] = [
    re.compile(
        r"^\d{4}-\d{2}-\d{2}"  # date
        r"[T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?"  # time
        r"(Z|[+-]\d{2}:?\d{2})?$"  # optional tz
    ),
    re.compile(r"^\d{4}-\d{2}-\d{2}$"),  # date only
]


def _looks_like_datetime(value: str) -> bool:
    """Heuristic check for ISO-8601-ish datetime strings."""
    return any(p.match(value) for p in _DATETIME_PATTERNS)


def _infer_type(value: Any) -> str:
    """Return a human-friendly type label for a single value."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if isinstance(value, str):
        if _looks_like_datetime(value):
            return "datetime"
        return "string"
    return "string"


def _resolve_column_type(types: set[str]) -> str:
    """Resolve a set of per-value types into a single column type."""
    types_without_null = types - {"null"}
    if not types_without_null:
        return "null"
    if len(types_without_null) == 1:
        return next(iter(types_without_null))
    return "mixed"


def build_schema_sample(
    data: list[dict[str, Any]],
    max_sample_rows: int = 5,
    max_string_length: int = 100,
) -> dict[str, dict[str, Any]]:
    """Build a compact schema representation from raw data rows.

    Parameters
    ----------
    data:
        List of row dicts.  May be empty.
    max_sample_rows:
        Maximum unique sample values to keep per column.
    max_string_length:
        Long strings are truncated in sample_values to save tokens.

    Returns
    -------
    dict mapping column name → column metadata::

        {
            "column_name": {
                "inferred_type": "string",
                "nullable": true,
                "sample_values": ["v1", "v2"],
                "null_count": 3,
                "total_count": 100,
            }
        }
    """
    if not data:
        return {}

    # Discover all columns across all rows.
    all_columns: list[str] = []
    seen: set[str] = set()
    for row in data:
        for col in row:
            if col not in seen:
                seen.add(col)
                all_columns.append(col)

    total_count = len(data)
    schema: dict[str, dict[str, Any]] = {}

    for col in all_columns:
        null_count = 0
        types_seen: set[str] = set()
        sample_values: list[Any] = []

        for row in data:
            value = row.get(col)  # missing key treated as None
            if value is None or col not in row:
                null_count += 1
                types_seen.add("null")
                continue

            vtype = _infer_type(value)
            types_seen.add(vtype)

            if len(sample_values) < max_sample_rows:
                display_value = value
                if isinstance(value, str) and len(value) > max_string_length:
                    display_value = value[:max_string_length] + "…"
                sample_values.append(display_value)

        inferred_type = _resolve_column_type(types_seen)

        schema[col] = {
            "inferred_type": inferred_type,
            "nullable": null_count > 0,
            "sample_values": sample_values,
            "null_count": null_count,
            "total_count": total_count,
        }

    return schema
