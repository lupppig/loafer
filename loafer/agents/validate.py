"""Validate Agent — pure function over PipelineState.

Checks null rates, schema consistency, and data quality.  Produces a
validation report with per-column stats and sets validation_passed.
"""

from __future__ import annotations

import time
from typing import Any

from loafer.graph.state import PipelineState

# Threshold for flagging schema inconsistency (fraction of rows missing a column).
_SCHEMA_INCONSISTENCY_THRESHOLD = 0.10

# Null rate that triggers a soft warning when strict=False.
_SOFT_NULL_THRESHOLD = 0.50


def validate_agent(state: PipelineState) -> PipelineState:
    """Validate extracted data quality.

    Returns the updated PipelineState with validation_report and
    validation_passed set.
    """
    start = time.monotonic()

    raw_data: list[dict[str, Any]] = state.get("raw_data", [])
    schema_sample: dict[str, Any] = state.get("schema_sample", {})

    is_streaming: bool = state.get("is_streaming", False)
    strict: bool = state.get("strict_validation", False)
    max_null_rate: float = state.get("max_null_rate", 0.5)

    report: dict[str, Any] = {}
    hard_failures: list[str] = []
    soft_warnings: list[str] = []

    if not raw_data and not is_streaming:
        hard_failures.append("Source returned 0 rows — nothing to validate")
        state["validation_passed"] = False
        state["validation_report"] = {"hard_failures": hard_failures}
        state["duration_ms"]["validate"] = (time.monotonic() - start) * 1000
        return state

    if not schema_sample:
        hard_failures.append("Schema sample is empty — cannot validate")
        state["validation_passed"] = False
        state["validation_report"] = {"hard_failures": hard_failures}
        state["duration_ms"]["validate"] = (time.monotonic() - start) * 1000
        return state

    if is_streaming and not raw_data:
        total = len(state.get("_first_chunk", []))
    else:
        total = len(raw_data) if raw_data else 0

    for col_name, col_meta in schema_sample.items():
        null_count: int = col_meta.get("null_count", 0)
        col_total: int = col_meta.get("total_count", total)
        null_rate = null_count / col_total if col_total > 0 else 0.0
        inferred_type: str = col_meta.get("inferred_type", "unknown")

        col_report: dict[str, Any] = {
            "null_rate": round(null_rate, 4),
            "null_count": null_count,
            "total_count": col_total,
            "inferred_type": inferred_type,
        }

        if inferred_type == "mixed":
            soft_warnings.append(f"Column '{col_name}' has mixed types")

        if null_rate > max_null_rate and strict:
            hard_failures.append(
                f"Column '{col_name}' null rate {null_rate:.2%} "
                f"exceeds max_null_rate {max_null_rate:.2%} (strict mode)"
            )
        elif null_rate > _SOFT_NULL_THRESHOLD and not strict:
            soft_warnings.append(
                f"Column '{col_name}' null rate {null_rate:.2%} is above {_SOFT_NULL_THRESHOLD:.0%}"
            )

        missing_count = col_total - (col_total - null_count)
        missing_rate = missing_count / col_total if col_total > 0 else 0.0
        if missing_rate > _SCHEMA_INCONSISTENCY_THRESHOLD:
            soft_warnings.append(f"Column '{col_name}' missing in {missing_rate:.0%} of rows")

        report[col_name] = col_report

    state["validation_report"] = {
        "columns": report,
        "hard_failures": hard_failures,
        "soft_warnings": soft_warnings,
    }
    state["validation_passed"] = len(hard_failures) == 0
    state["duration_ms"]["validate"] = (time.monotonic() - start) * 1000

    for warning in soft_warnings:
        state.setdefault("warnings", []).append(warning)

    return state
