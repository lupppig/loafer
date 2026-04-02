"""Destructive operation detector for ETL/ELT pipelines.

Detects irreversible data modifications during transforms:
- Row drops exceeding threshold
- Column removal
- Type changes
- SQL-level filtering (ELT mode via AST analysis)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import sqlglot.expressions as exp

from loafer.exceptions import TransformError


class DestructiveReason(StrEnum):
    ROWS_DROPPED = "rows_dropped"
    COLUMNS_REMOVED = "columns_removed"
    COLUMN_TYPES_CHANGED = "column_types_changed"
    SQL_FILTERS_ROWS = "sql_filters_rows"
    SQL_DROPS_COLUMNS = "sql_drops_columns"


@dataclass
class DestructiveWarning:
    reason: DestructiveReason
    message: str
    severity: str
    details: dict[str, Any]


def detect_destructive_operations(
    before_state: dict[str, Any],
    after_state: dict[str, Any],
    threshold: float = 0.3,
) -> list[DestructiveWarning]:
    """Compare pre/post transform state to detect destructive operations.

    For ETL mode: compares raw_data vs transformed_data in memory.
    For ELT mode: analyzes the generated SQL via AST.
    """
    warnings: list[DestructiveWarning] = []

    # ETL mode: compare in-memory data
    raw_data: list[dict[str, Any]] | None = before_state.get("raw_data")
    transformed_data: list[dict[str, Any]] | None = after_state.get("transformed_data")

    if raw_data is not None and transformed_data is not None:
        warnings.extend(_check_rows_dropped(raw_data, transformed_data, threshold))
        warnings.extend(_check_columns_removed(raw_data, transformed_data))
        warnings.extend(_check_type_changes(raw_data, transformed_data))

    # ELT mode: analyze SQL
    generated_sql: str | None = after_state.get("generated_sql")
    if generated_sql:
        warnings.extend(_analyze_sql_destructive(generated_sql, raw_data))

    return warnings


def format_destructive_warnings(warnings: list[DestructiveWarning]) -> str:
    """Format destructive warnings for error display."""
    if not warnings:
        return ""

    lines = ["Destructive operations detected:"]
    for w in warnings:
        icon = "✗" if w.severity == "error" else "⚠"
        lines.append(f"  {icon} {w.message}")
    lines.append("\nIf this is intentional, re-run with --yes to confirm.")
    return "\n".join(lines)


def raise_if_destructive(
    warnings: list[DestructiveWarning],
    auto_confirmed: bool = False,
) -> None:
    """Raise TransformError if destructive warnings found and not auto-confirmed."""
    if warnings and not auto_confirmed:
        msg = format_destructive_warnings(warnings)
        raise TransformError(msg)


# ---------------------------------------------------------------------------
# ETL detection functions
# ---------------------------------------------------------------------------


def _check_rows_dropped(
    raw_data: list[dict[str, Any]],
    transformed_data: list[dict[str, Any]],
    threshold: float,
) -> list[DestructiveWarning]:
    """Check if transform dropped more than threshold fraction of rows."""
    before_count = len(raw_data)
    after_count = len(transformed_data)

    if before_count == 0:
        return []

    dropped = before_count - after_count
    drop_rate = dropped / before_count if before_count > 0 else 0

    if drop_rate >= 1.0:
        return [
            DestructiveWarning(
                reason=DestructiveReason.ROWS_DROPPED,
                message=f"Transform dropped all {before_count} rows",
                severity="error",
                details={"before": before_count, "after": after_count, "drop_rate": 1.0},
            )
        ]

    if drop_rate > threshold:
        return [
            DestructiveWarning(
                reason=DestructiveReason.ROWS_DROPPED,
                message=f"Transform dropped {drop_rate:.0%} of rows ({before_count} → {after_count})",
                severity="warning",
                details={"before": before_count, "after": after_count, "drop_rate": drop_rate},
            )
        ]

    return []


def _check_columns_removed(
    raw_data: list[dict[str, Any]],
    transformed_data: list[dict[str, Any]],
) -> list[DestructiveWarning]:
    """Check if transform removed columns from the schema."""
    if not raw_data or not transformed_data:
        return []

    before_cols = {k for row in raw_data for k in row}
    after_cols = {k for row in transformed_data for k in row}
    removed = before_cols - after_cols

    if removed:
        return [
            DestructiveWarning(
                reason=DestructiveReason.COLUMNS_REMOVED,
                message=f"Transform removed columns: {', '.join(sorted(removed))}",
                severity="warning",
                details={
                    "removed": sorted(removed),
                    "before": sorted(before_cols),
                    "after": sorted(after_cols),
                },
            )
        ]

    return []


def _check_type_changes(
    raw_data: list[dict[str, Any]],
    transformed_data: list[dict[str, Any]],
) -> list[DestructiveWarning]:
    """Check if column types changed between raw and transformed data."""
    if not raw_data or not transformed_data:
        return []

    before_cols = {k for row in raw_data for k in row}
    after_cols = {k for row in transformed_data for k in row}
    common_cols = before_cols & after_cols

    changes: list[str] = []
    for col in sorted(common_cols):
        before_type = _infer_type([row.get(col) for row in raw_data if col in row])
        after_type = _infer_type([row.get(col) for row in transformed_data if col in row])
        if before_type != after_type and before_type != "null" and after_type != "null":
            changes.append(f"{col}: {before_type} → {after_type}")

    if changes:
        return [
            DestructiveWarning(
                reason=DestructiveReason.COLUMN_TYPES_CHANGED,
                message=f"Column types changed: {'; '.join(changes)}",
                severity="warning",
                details={"changes": changes},
            )
        ]

    return []


def _infer_type(values: list[Any]) -> str:
    """Infer the dominant type from a list of values."""
    non_null = [v for v in values if v is not None]
    if not non_null:
        return "null"

    types = set()
    for v in non_null:
        if isinstance(v, bool):
            types.add("boolean")
        elif isinstance(v, int):
            types.add("integer")
        elif isinstance(v, float):
            types.add("float")
        elif isinstance(v, str):
            types.add("string")
        elif isinstance(v, (list, dict)):
            types.add("object")
        else:
            types.add("unknown")

    if len(types) > 1:
        return "mixed"
    return types.pop()


# ---------------------------------------------------------------------------
# ELT detection functions (SQL AST analysis)
# ---------------------------------------------------------------------------


def _analyze_sql_destructive(
    sql: str,
    raw_data: list[dict[str, Any]] | None,
) -> list[DestructiveWarning]:
    """Analyze SQL via AST to detect potentially destructive operations."""
    try:
        import sqlglot
    except ImportError:
        return []

    try:
        statements = sqlglot.parse(sql)
    except Exception:
        return [
            DestructiveWarning(
                reason=DestructiveReason.SQL_FILTERS_ROWS,
                message="SQL parse error",
                severity="error",
                details={"error": "failed to parse SQL"},
            )
        ]

    if not statements:
        return []

    stmt = statements[0]
    if not isinstance(stmt, exp.Select):
        return []

    warnings: list[DestructiveWarning] = []

    if stmt.find(exp.Where):
        warnings.append(
            DestructiveWarning(
                reason=DestructiveReason.SQL_FILTERS_ROWS,
                message="SQL contains WHERE clause — may filter rows",
                severity="warning",
                details={"operation": "WHERE"},
            )
        )

    if stmt.find(exp.Group):
        warnings.append(
            DestructiveWarning(
                reason=DestructiveReason.SQL_FILTERS_ROWS,
                message="SQL contains GROUP BY — aggregates rows",
                severity="warning",
                details={"operation": "GROUP BY"},
            )
        )

    if stmt.find(exp.Having):
        warnings.append(
            DestructiveWarning(
                reason=DestructiveReason.SQL_FILTERS_ROWS,
                message="SQL contains HAVING clause — may filter aggregated rows",
                severity="warning",
                details={"operation": "HAVING"},
            )
        )

    if stmt.find(exp.Distinct):
        warnings.append(
            DestructiveWarning(
                reason=DestructiveReason.SQL_FILTERS_ROWS,
                message="SQL contains DISTINCT — may deduplicate rows",
                severity="warning",
                details={"operation": "DISTINCT"},
            )
        )

    if stmt.find(exp.Limit):
        warnings.append(
            DestructiveWarning(
                reason=DestructiveReason.SQL_FILTERS_ROWS,
                message="SQL contains LIMIT — truncates result set",
                severity="warning",
                details={"operation": "LIMIT"},
            )
        )

    if stmt.find(exp.Offset):
        warnings.append(
            DestructiveWarning(
                reason=DestructiveReason.SQL_FILTERS_ROWS,
                message="SQL contains OFFSET — skips rows in result set",
                severity="warning",
                details={"operation": "OFFSET"},
            )
        )

    for join in stmt.find_all(exp.Join):
        join_kind = (join.kind or "").lower()
        if join_kind == "inner":
            warnings.append(
                DestructiveWarning(
                    reason=DestructiveReason.SQL_FILTERS_ROWS,
                    message="SQL contains INNER JOIN — may drop unmatched rows",
                    severity="warning",
                    details={"operation": "INNER JOIN"},
                )
            )

    if raw_data:
        source_cols = {k for row in raw_data for k in row}
        if source_cols:
            selected_cols = _extract_selected_columns(stmt)
            if selected_cols:
                dropped = source_cols - selected_cols
                if dropped:
                    warnings.append(
                        DestructiveWarning(
                            reason=DestructiveReason.SQL_DROPS_COLUMNS,
                            message=f"SQL drops columns: {', '.join(sorted(dropped))}",
                            severity="warning",
                            details={"dropped": sorted(dropped)},
                        )
                    )

    return warnings


def _extract_selected_columns(stmt: Any) -> set[str]:
    """Extract column names from a SELECT statement AST.

    Handles aliased columns (SELECT foo AS bar) by extracting the alias name.
    Returns empty set only when a bare SELECT * is used (no aliases).
    """
    cols: set[str] = set()

    for col_expr in stmt.find_all(exp.Alias):
        alias_name = col_expr.alias
        if alias_name:
            cols.add(alias_name)
        else:
            inner_col = col_expr.find(exp.Column)
            if inner_col and inner_col.name:
                cols.add(inner_col.name)

    for col in stmt.find_all(exp.Column):
        if col.name and col.name != "*":
            parent = col.parent
            if not isinstance(parent, exp.Alias):
                cols.add(col.name)

    if not cols and stmt.find(exp.Star):
        return set()

    return cols
