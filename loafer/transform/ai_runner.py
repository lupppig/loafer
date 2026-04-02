"""AI-powered transform runner.

Generates Python transform code via an LLM provider, validates it for
safety, executes it in a restricted context, and supports automatic
retry on failure (max 3 attempts).
"""

from __future__ import annotations

import copy
import time
import traceback
from types import ModuleType
from typing import TYPE_CHECKING, Any

from loafer.core.destructive import detect_destructive_operations, raise_if_destructive
from loafer.exceptions import TransformError
from loafer.graph.state import PipelineState
from loafer.llm.base import LLMProvider, TransformPromptResult
from loafer.llm.prompt_builder import build_etl_transform_prompt
from loafer.transform import TransformRunner
from loafer.transform.code_validator import validate_transform_function

if TYPE_CHECKING:
    from collections.abc import Iterator

# Maximum number of retry attempts for AI-generated code.
_MAX_RETRIES = 3

# Maximum total execution time for transform (seconds).
_MAX_EXECUTION_TIME = 60

# Safe builtins allowed in generated transform code.
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
    """Build a restricted globals dict for safe exec of transform code."""
    safe_globals: dict[str, Any] = {"__builtins__": _SAFE_BUILTINS}
    for mod_name in ("re", "json", "datetime", "math", "decimal", "uuid", "itertools"):
        try:
            mod: ModuleType = __import__(mod_name)
            safe_globals[mod_name] = mod
        except ImportError:
            pass
    return safe_globals


class AiTransformRunner(TransformRunner):
    """Generate, validate, and execute transform code via an LLM."""

    def run(self, state: PipelineState) -> PipelineState:
        llm_provider: LLMProvider = state["llm_provider"]
        schema_sample: dict[str, Any] = state["schema_sample"]
        instruction: str = state["transform_instruction"]

        previous_error: str | None = state.get("last_error")
        previous_code: str | None = state.get("generated_code")
        retry_count: int = state.get("retry_count", 0)
        total_tokens: dict[str, int] = state.get("token_usage", {})

        start = time.monotonic()

        # Snapshot raw data for destructive detection
        raw_data_snapshot = copy.deepcopy(state.get("raw_data", []))

        while retry_count < _MAX_RETRIES:
            build_etl_transform_prompt(schema_sample, instruction, previous_error, previous_code)

            try:
                result: TransformPromptResult = llm_provider.generate_transform_function(
                    schema_sample,
                    instruction,
                    previous_error=previous_error,
                    previous_code=previous_code,
                )
            except Exception as exc:
                retry_count += 1
                previous_error = f"LLM call failed: {exc}"
                previous_code = None
                state["last_error"] = previous_error
                state["retry_count"] = retry_count
                state["generated_code"] = ""
                continue

            total_tokens["prompt_tokens"] = total_tokens.get(
                "prompt_tokens", 0
            ) + result.token_usage.get("prompt_tokens", 0)
            total_tokens["completion_tokens"] = total_tokens.get(
                "completion_tokens", 0
            ) + result.token_usage.get("completion_tokens", 0)
            total_tokens["total_tokens"] = total_tokens.get(
                "total_tokens", 0
            ) + result.token_usage.get("total_tokens", 0)

            code = result.code
            state["generated_code"] = code

            is_valid, reason = validate_transform_function(code)
            if not is_valid:
                retry_count += 1
                previous_error = f"Code validation failed: {reason}"
                previous_code = code
                state["last_error"] = previous_error
                state["retry_count"] = retry_count
                continue

            try:
                transformed = _execute_transform(code, state)
            except Exception:
                retry_count += 1
                previous_error = f"Execution error: {traceback.format_exc()}"
                previous_code = code
                state["last_error"] = previous_error
                state["retry_count"] = retry_count
                continue

            state["transformed_data"] = transformed
            state["last_error"] = None
            state["retry_count"] = retry_count
            state["token_usage"] = total_tokens
            state["duration_ms"]["transform"] = (time.monotonic() - start) * 1000

            if len(transformed) == 0:
                state.setdefault("warnings", []).append(
                    "Transform returned 0 rows (filtering may have removed all data)"
                )

            # Destructive operation detection
            before_state = {"raw_data": raw_data_snapshot}
            after_state = {"transformed_data": transformed}
            threshold = state.get("destructive_filter_threshold", 0.3)
            warnings = detect_destructive_operations(before_state, after_state, threshold)
            raise_if_destructive(warnings, state.get("auto_confirmed", False))
            if warnings:
                state.setdefault("destructive_warnings", []).extend(warnings)

            return state

        raise TransformError(
            f"Transform failed after {_MAX_RETRIES} attempts. Last error: {previous_error}"
        )


def _execute_transform(code: str, state: PipelineState) -> list[dict[str, Any]]:
    """Execute transform code against the data in state.

    Handles both streaming and batch modes.
    """
    safe_globals = _build_safe_globals()
    exec(code, safe_globals)

    if "transform" not in safe_globals:
        raise TransformError("Generated code does not define a `transform` function")

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
    """Apply transform chunk-by-chunk on a stream iterator."""
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
