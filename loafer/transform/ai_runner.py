"""AI-powered transform runner.

Generates Python transform code via an LLM provider, validates it for
safety, executes it in a restricted context, and supports automatic
retry on failure (max 3 attempts).

When a custom transform path is provided alongside AI, both are executed
in the configured order (custom_first or ai_first). The AI is shown the
custom code so it does not duplicate or override it.
"""

from __future__ import annotations

import copy
import time
import traceback
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any

from loafer.config import AITransformConfig
from loafer.core.destructive import detect_destructive_operations, raise_if_destructive
from loafer.exceptions import LLMError, LLMRateLimitError, TransformError
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


def _human_readable_llm_error(exc: Exception) -> str:
    """Convert raw LLM exceptions into user-friendly messages."""
    msg = str(exc)

    if isinstance(exc, LLMRateLimitError):
        return (
            "Rate limited by the AI provider — you've sent too many requests.\n"
            "  • Wait a moment and try again\n"
            "  • Check your provider dashboard for quota usage"
        )

    if isinstance(exc, LLMError):
        return msg

    # 404 model not found
    if "404" in msg or "not_found" in msg.lower() or "not found" in msg.lower():
        if "model" in msg.lower() or "gemini" in msg.lower() or "claude" in msg.lower():
            model = "unknown"
            for part in msg.split():
                if part.startswith(("gemini", "claude", "gpt", "qwen")):
                    model = part.strip("',.")
                    break
            if "gemini" in model.lower():
                return (
                    f"Model '{model}' was not found. Google has renamed or deprecated it.\n"
                    f"  Try: gemini-2.0-flash, gemini-2.5-flash, or gemini-2.0-flash-lite"
                )
            return (
                f"Model '{model}' was not found. Check your provider's docs for available models."
            )
        return "The API endpoint could not be reached. Check your internet connection."

    # 401 auth errors
    if "401" in msg or "unauthorized" in msg.lower():
        return (
            "Authentication failed — your API key is invalid or expired.\n"
            "  • Check that your API key is correct\n"
            "  • Make sure it hasn't expired or been revoked"
        )

    # 429 rate limit in raw string
    if "429" in msg or "rate" in msg.lower() or "quota" in msg.lower():
        return (
            "Rate limited or quota exhausted.\n"
            "  • Wait a moment and try again\n"
            "  • Check your provider dashboard for quota usage"
        )

    return msg


def _load_custom_code(path: str) -> str:
    """Load custom transform code from a file path."""
    p = Path(path)
    if not p.exists():
        raise TransformError(f"Custom transform file not found: {path}")
    return p.read_text(encoding="utf-8")


def _execute_code(code: str, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Execute transform code against data and return the result."""
    safe_globals = _build_safe_globals()
    exec(code, safe_globals)

    if "transform" not in safe_globals:
        raise TransformError("Code does not define a `transform` function")

    transform_fn = safe_globals["transform"]
    result = transform_fn(data)

    if not isinstance(result, list):
        raise TransformError(f"Transform must return list[dict], got {type(result).__name__}")

    return result


def _ask_user_confirmation(generated_code: str) -> bool:
    """Show generated code and ask user to confirm execution."""
    from rich.console import Console
    from rich.panel import Panel
    from rich.syntax import Syntax

    console = Console()

    console.print()
    console.print(
        Panel(
            "[yellow]AI-generated transform code is ready for review.[/yellow]\n"
            "Review the code below. If it looks correct, type 'y' to execute.\n"
            "Type 'n' to skip AI transform (custom transform will still run if configured).",
            title="[bold yellow]⚠ Human Review Required[/bold yellow]",
        )
    )
    console.print()

    # Show the code with syntax highlighting
    syntax = Syntax(generated_code, "python", theme="monokai", line_numbers=True)
    console.print(syntax)
    console.print()

    try:
        answer = input("Execute this code? [y/N]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        console.print("\n[dim]No input received. Skipping AI transform.[/dim]")
        return False

    return answer in ("y", "yes")


class AiTransformRunner(TransformRunner):
    """Generate, validate, and execute transform code via an LLM.

    Supports optional custom transform execution before or after AI,
    and human-in-the-loop review before AI code execution.
    """

    def run(self, state: PipelineState) -> PipelineState:
        transform_config = state.get("transform_config")

        # If no AITransformConfig, fall back to simple AI-only behavior
        if not isinstance(transform_config, AITransformConfig):
            return self._run_simple_ai(state)

        # If bypass_ai is set and there's a custom path, just run custom
        if transform_config.bypass_ai:
            if transform_config.custom_path:
                return self._run_custom_only(state, transform_config.custom_path)
            raise TransformError(
                "bypass_ai is set but no custom_path is configured.\n"
                "  Either provide a custom_path or remove bypass_ai to use AI."
            )

        llm_provider: LLMProvider = state["llm_provider"]
        schema_sample: dict[str, Any] = state["schema_sample"]
        instruction: str = state["transform_instruction"]

        custom_code: str | None = None
        if transform_config.custom_path:
            custom_code = _load_custom_code(transform_config.custom_path)

        start = time.monotonic()
        raw_data_snapshot = copy.deepcopy(state.get("raw_data", []))

        # Determine data flow: custom_first or ai_first
        order = transform_config.custom_order
        data = list(state.get("raw_data", []))

        # Step 1: Run custom transform first if order is custom_first
        if custom_code and order == "custom_first":
            data = self._run_custom_code(custom_code, data, state)
            state["transformed_data"] = data

        # Step 2: Run AI transform
        ai_code = self._generate_ai_code(
            llm_provider, schema_sample, instruction, custom_code, state
        )

        if ai_code:
            # Human review if requested
            if transform_config.review:
                if not _ask_user_confirmation(ai_code):
                    # User rejected — skip AI, keep custom result if any
                    state["transformed_data"] = data
                    state["duration_ms"]["transform"] = (time.monotonic() - start) * 1000
                    return state

            data = self._run_ai_code(ai_code, data, state)

        # Step 3: Run custom transform after AI if order is ai_first
        if custom_code and order == "ai_first":
            data = self._run_custom_code(custom_code, data, state)

        state["transformed_data"] = data
        state["last_error"] = None
        state["retry_count"] = 0
        state["duration_ms"]["transform"] = (time.monotonic() - start) * 1000

        if len(data) == 0:
            state.setdefault("warnings", []).append(
                "Transform returned 0 rows (filtering may have removed all data)"
            )

        # Destructive operation detection
        before_state = {"raw_data": raw_data_snapshot}
        after_state = {"transformed_data": data}
        threshold = state.get("destructive_filter_threshold", 0.3)
        warnings = detect_destructive_operations(before_state, after_state, threshold)
        raise_if_destructive(warnings, state.get("auto_confirmed", False))
        if warnings:
            state.setdefault("destructive_warnings", []).extend(warnings)

        return state

    def _run_simple_ai(self, state: PipelineState) -> PipelineState:
        """Legacy path: AI-only transform without custom code support."""
        llm_provider: LLMProvider = state["llm_provider"]
        schema_sample: dict[str, Any] = state["schema_sample"]
        instruction: str = state["transform_instruction"]

        previous_error: str | None = state.get("last_error")
        previous_code: str | None = state.get("generated_code")
        retry_count: int = state.get("retry_count", 0)
        total_tokens: dict[str, int] = state.get("token_usage", {})

        start = time.monotonic()
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
                user_msg = _human_readable_llm_error(exc)
                previous_error = f"LLM call failed: {user_msg}"
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
                transformed = _execute_code(code, list(state.get("raw_data", [])))
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

    def _run_custom_only(self, state: PipelineState, custom_path: str) -> PipelineState:
        """Run only the custom transform, no AI."""
        start = time.monotonic()
        custom_code = _load_custom_code(custom_path)
        raw_data_snapshot = copy.deepcopy(state.get("raw_data", []))
        data = list(state.get("raw_data", []))
        data = self._run_custom_code(custom_code, data, state)

        state["transformed_data"] = data
        state["duration_ms"]["transform"] = (time.monotonic() - start) * 1000

        if len(data) == 0:
            state.setdefault("warnings", []).append("Transform returned 0 rows")

        before_state = {"raw_data": raw_data_snapshot}
        after_state = {"transformed_data": data}
        threshold = state.get("destructive_filter_threshold", 0.3)
        warnings = detect_destructive_operations(before_state, after_state, threshold)
        raise_if_destructive(warnings, state.get("auto_confirmed", False))
        if warnings:
            state.setdefault("destructive_warnings", []).extend(warnings)

        return state

    def _run_custom_code(
        self, code: str, data: list[dict[str, Any]], state: PipelineState
    ) -> list[dict[str, Any]]:
        """Execute custom transform code against data."""
        try:
            return _execute_code(code, data)
        except TransformError:
            raise
        except Exception as exc:
            raise TransformError(
                f"Custom transform failed: {exc}\n{traceback.format_exc()}"
            ) from exc

    def _generate_ai_code(
        self,
        llm_provider: LLMProvider,
        schema_sample: dict[str, Any],
        instruction: str,
        custom_code: str | None,
        state: PipelineState,
    ) -> str | None:
        """Generate transform code via LLM with retry logic."""
        previous_error: str | None = state.get("last_error")
        previous_code: str | None = state.get("generated_code")
        retry_count: int = state.get("retry_count", 0)
        total_tokens: dict[str, int] = state.get("token_usage", {})

        while retry_count < _MAX_RETRIES:
            build_etl_transform_prompt(
                schema_sample, instruction, previous_error, previous_code, custom_code
            )

            try:
                result: TransformPromptResult = llm_provider.generate_transform_function(
                    schema_sample,
                    instruction,
                    previous_error=previous_error,
                    previous_code=previous_code,
                )
            except Exception as exc:
                retry_count += 1
                user_msg = _human_readable_llm_error(exc)
                previous_error = f"LLM call failed: {user_msg}"
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

            state["token_usage"] = total_tokens
            return code

        raise TransformError(
            f"AI transform failed after {_MAX_RETRIES} attempts. Last error: {previous_error}"
        )

    def _run_ai_code(
        self,
        code: str,
        data: list[dict[str, Any]],
        state: PipelineState,
    ) -> list[dict[str, Any]]:
        """Execute AI-generated transform code against data."""
        try:
            return _execute_code(code, data)
        except Exception:
            raise TransformError(f"AI transform execution failed: {traceback.format_exc()}")
