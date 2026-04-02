"""Custom Python transform runner.

Loads a user-supplied .py file, validates it for safety, and executes
the transform function.  No LLM call.  No retry loop — one attempt.
"""

from __future__ import annotations

import copy
import time
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any

from loafer.core.destructive import detect_destructive_operations, raise_if_destructive
from loafer.exceptions import TransformError
from loafer.graph.state import PipelineState
from loafer.transform import TransformRunner
from loafer.transform.code_validator import validate_transform_function

if TYPE_CHECKING:
    from collections.abc import Iterator

_MAX_EXECUTION_TIME = 60

_SAFE_BUILTINS: dict[str, Any] = {
    "__import__": __import__,
    "len": len,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "list": list,
    "dict": dict,
    "set": set,
    "tuple": tuple,
    "range": range,
    "enumerate": enumerate,
    "zip": zip,
    "map": map,
    "filter": filter,
    "sorted": sorted,
    "reversed": reversed,
    "sum": sum,
    "min": min,
    "max": max,
    "abs": abs,
    "round": round,
    "isinstance": isinstance,
    "type": type,
    "None": None,
    "True": True,
    "False": False,
    "print": print,
}


def _build_safe_globals() -> dict[str, Any]:
    safe_globals: dict[str, Any] = {"__builtins__": _SAFE_BUILTINS}
    for mod_name in ("re", "json", "datetime", "math", "decimal", "uuid", "itertools"):
        try:
            mod: ModuleType = __import__(mod_name)
            safe_globals[mod_name] = mod
        except ImportError:
            pass
    return safe_globals


class CustomTransformRunner(TransformRunner):
    """Execute a user-supplied Python transform file."""

    def run(self, state: PipelineState) -> PipelineState:
        transform_config = state.get("transform_config")
        path_str: str | None = transform_config.path if transform_config else None
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
    safe_globals = _build_safe_globals()
    exec(code, safe_globals)

    if "transform" not in safe_globals:
        raise TransformError("Custom file does not define a `transform` function")

    transform_fn = safe_globals["transform"]

    is_streaming: bool = state.get("is_streaming", False)

    if is_streaming:
        return _apply_streaming(transform_fn, state)

    raw_data: list[dict[str, Any]] = state.get("raw_data", [])
    result = transform_fn(raw_data)

    if not isinstance(result, list):
        raise TransformError(f"Transform must return list[dict], got {type(result).__name__}")

    return result


def _apply_streaming(transform_fn: Any, state: PipelineState) -> list[dict[str, Any]]:
    stream_iter: Iterator[list[dict[str, Any]]] | None = state.get("stream_iterator")
    if stream_iter is None:
        raise TransformError("stream_iterator is None in streaming mode")

    all_transformed: list[dict[str, Any]] = []
    start = time.monotonic()
    total_rows = 0

    for chunk in stream_iter:
        if (time.monotonic() - start) > _MAX_EXECUTION_TIME:
            raise TransformError(f"Transform exceeded {_MAX_EXECUTION_TIME}s timeout")

        total_rows += len(chunk)
        result = transform_fn(chunk)
        if not isinstance(result, list):
            raise TransformError(f"Transform must return list[dict], got {type(result).__name__}")
        all_transformed.extend(result)

    state["rows_extracted"] = total_rows
    return all_transformed
