"""Custom Python transform runner.

Loads a user-supplied .py file, validates it for safety, and executes
the transform function.  No LLM call.  No retry loop — one attempt.
"""

from __future__ import annotations

import copy
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loafer.config import CustomTransformConfig
from loafer.core.destructive import detect_destructive_operations, raise_if_destructive
from loafer.core.sandbox import run_sandboxed
from loafer.exceptions import TransformError
from loafer.graph.state import PipelineState
from loafer.transform import TransformRunner
from loafer.transform.code_validator import validate_transform_function

if TYPE_CHECKING:
    from collections.abc import Iterator


def _sandbox_limits(state: PipelineState) -> tuple[int, int]:
    """Read (timeout, max_memory_mb) from state, falling back to defaults."""
    cfg = state.get("sandbox_config")
    timeout = getattr(cfg, "timeout", 60)
    max_memory_mb = getattr(cfg, "max_memory_mb", 512)
    return timeout, max_memory_mb


class CustomTransformRunner(TransformRunner):
    """Execute a user-supplied Python transform file."""

    def run(self, state: PipelineState) -> PipelineState:
        transform_config = state.get("transform_config")
        if not isinstance(transform_config, CustomTransformConfig):
            raise TransformError("custom transform requires a CustomTransformConfig")
        path_str: str = transform_config.path
        if not path_str:
            raise TransformError("custom transform requires a 'path' in transform_config")

        path = Path(path_str)
        if not path.exists():
            raise TransformError(f"transform file not found: {path}")

        code = path.read_text(encoding="utf-8")

        is_valid, reason = validate_transform_function(code)
        if not is_valid:
            raise TransformError(f"Custom transform validation failed: {reason}")

        start = time.monotonic()

        # Snapshot raw data for destructive detection
        raw_data_snapshot = copy.deepcopy(state.get("raw_data", []))

        try:
            transformed = _execute_transform(code, state)
        except TransformError:
            raise
        except Exception as exc:
            import traceback

            raise TransformError(
                f"Custom transform execution failed: {exc}\n{traceback.format_exc()}"
            ) from exc

        state["transformed_data"] = transformed
        state["duration_ms"]["transform"] = (time.monotonic() - start) * 1000

        if len(transformed) == 0:
            state.setdefault("warnings", []).append("Transform returned 0 rows")

        # Destructive operation detection
        before_state = {"raw_data": raw_data_snapshot}
        after_state = {"transformed_data": transformed}
        threshold = state.get("destructive_filter_threshold", 0.3)
        warnings = detect_destructive_operations(before_state, after_state, threshold)
        raise_if_destructive(warnings, state.get("auto_confirmed", False))
        if warnings:
            state.setdefault("destructive_warnings", []).extend(warnings)

        return state


def _execute_transform(code: str, state: PipelineState) -> list[dict[str, Any]]:
    data = _materialize_data(state)
    timeout, max_memory_mb = _sandbox_limits(state)
    return run_sandboxed(code, data, timeout=timeout, max_memory_mb=max_memory_mb)


def _materialize_data(state: PipelineState) -> list[dict[str, Any]]:
    """Return the rows to transform, collecting the stream when in streaming mode."""
    if not state.get("is_streaming", False):
        return list(state.get("raw_data", []))

    stream_iter: Iterator[list[dict[str, Any]]] | None = state.get("stream_iterator")
    if stream_iter is None:
        raise TransformError("stream_iterator is None in streaming mode")

    rows: list[dict[str, Any]] = []
    for chunk in stream_iter:
        rows.extend(chunk)
    state["rows_extracted"] = len(rows)
    return rows
