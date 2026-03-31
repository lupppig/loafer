"""Custom Python transform runner.

Loads a user-supplied .py file, validates it for safety, and executes
the transform function.  No LLM call.  No retry loop — one attempt.
"""

from __future__ import annotations

import time
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any

from loafer.exceptions import TransformError
from loafer.graph.state import PipelineState
from loafer.transform import TransformRunner
from loafer.transform.code_validator import validate_transform_function

if TYPE_CHECKING:
    from collections.abc import Iterator

_MAX_EXECUTION_TIME = 60

_SAFE_BUILTINS: dict[str, Any] = {
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
        path_str: str | None = state.get("transform_config", {}).get("path")
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

    for chunk in stream_iter:
        if (time.monotonic() - start) > _MAX_EXECUTION_TIME:
            raise TransformError(f"Transform exceeded {_MAX_EXECUTION_TIME}s timeout")

        result = transform_fn(chunk)
        if not isinstance(result, list):
            raise TransformError(f"Transform must return list[dict], got {type(result).__name__}")
        all_transformed.extend(result)

    return all_transformed
