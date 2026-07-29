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
import re
import time
import traceback
from pathlib import Path
from typing import Any

from loafer.config import AITransformConfig
from loafer.core.destructive import detect_destructive_operations, raise_if_destructive
from loafer.core.sandbox import run_sandboxed
from loafer.exceptions import LLMAuthError, LLMError, LLMRateLimitError, TransformError
from loafer.graph.state import PipelineState
from loafer.llm.base import LLMProvider, TransformPromptResult
from loafer.llm.models import default_model_for, provider_for_model
from loafer.llm.schema import build_schema_sample
from loafer.transform import TransformRunner, materialize_input_rows
from loafer.transform.code_validator import validate_transform_function

# Maximum number of retry attempts for AI-generated code.
_MAX_RETRIES = 3
_TRANSIENT_HTTP_STATUS_CODES = {408, 409, 425, 429}


def _llm_status_code(exc: Exception) -> int | None:
    """Extract an HTTP status from provider exceptions or their messages."""
    for candidate in (exc, getattr(exc, "response", None)):
        value = getattr(candidate, "status_code", None)
        if isinstance(value, int):
            return value
        value = getattr(candidate, "code", None)
        if isinstance(value, int):
            return value

    match = re.search(r"\b(?:error\s+code\s*:\s*)?([1-5]\d{2})\b", str(exc), re.IGNORECASE)
    return int(match.group(1)) if match else None


def _is_retryable_llm_error(exc: Exception) -> bool:
    """Return whether repeating the same provider request may succeed."""
    if isinstance(exc, LLMAuthError):
        return False
    if isinstance(exc, LLMRateLimitError):
        return True

    status = _llm_status_code(exc)
    if status is None:
        return True
    if status in _TRANSIENT_HTTP_STATUS_CODES or status >= 500:
        return True
    return not 400 <= status < 500


def _human_readable_llm_error(exc: Exception) -> str:
    """Convert raw LLM exceptions into user-friendly messages."""
    msg = str(exc)

    if isinstance(exc, LLMAuthError):
        return (
            "Authentication failed — your API key is invalid or expired.\n"
            "  • Check that your API key is correct\n"
            "  • Make sure it hasn't expired or been revoked\n"
            "  • Generate a new key at https://aistudio.google.com/apikey"
        )

    if isinstance(exc, LLMRateLimitError):
        return (
            "Rate limited by Gemini — free tier allows 15 requests per minute.\n"
            "  • Retrying with exponential backoff (2s, 4s, 8s)...\n"
            "  • If retries are exhausted, wait 1 minute and try again\n"
            "  • Check your usage at https://aistudio.google.com/"
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
            provider = provider_for_model(model)
            if provider:
                recommended = default_model_for(provider)
                return (
                    f"Model '{model}' was not found. It may be retired or unavailable "
                    f"for your account.\n"
                    f"  Current Loafer default for {provider}: {recommended}"
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


def _execute_code(
    code: str, data: list[dict[str, Any]], state: PipelineState
) -> list[dict[str, Any]]:
    """Execute transform code against data in a resource-limited sandbox."""
    cfg = state.get("sandbox_config")
    timeout = getattr(cfg, "timeout", 60)
    max_memory_mb = getattr(cfg, "max_memory_mb", 512)
    return run_sandboxed(code, data, timeout=timeout, max_memory_mb=max_memory_mb)


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
        instruction: str = state["transform_instruction"]

        custom_code: str | None = None
        if transform_config.custom_path:
            custom_code = _load_custom_code(transform_config.custom_path)

        start = time.monotonic()
        # Drain the stream (streaming mode) or read raw_data — exactly once.
        input_rows = materialize_input_rows(state)
        raw_data_snapshot = copy.deepcopy(input_rows)
        schema_sample = build_schema_sample(input_rows, max_sample_rows=5)
        state["schema_sample"] = schema_sample

        # Determine data flow: custom_first or ai_first
        order = transform_config.custom_order
        data = list(input_rows)

        # Step 1: Run custom transform first if order is custom_first
        if custom_code and order == "custom_first":
            data = self._run_custom_code(custom_code, data, state)
            state["transformed_data"] = data
            schema_sample = build_schema_sample(data, max_sample_rows=5)
            state["schema_sample"] = schema_sample

        # Step 2: Run AI transform
        while True:
            ai_code = self._generate_ai_code(
                llm_provider, schema_sample, instruction, custom_code, state
            )

            # Human review if requested
            if transform_config.review and not _ask_user_confirmation(ai_code):
                # User rejected — skip AI, keep custom result if any
                state["transformed_data"] = data
                state["duration_ms"]["transform"] = (time.monotonic() - start) * 1000
                return state

            try:
                data = self._run_ai_code(ai_code, data, state)
                break
            except TransformError as exc:
                retry_count = state.get("retry_count", 0) + 1
                previous_error = f"Execution error: {exc}"
                state["retry_count"] = retry_count
                state["last_error"] = previous_error
                state["generated_code"] = ai_code
                if retry_count >= _MAX_RETRIES:
                    raise TransformError(
                        f"AI transform failed after {_MAX_RETRIES} attempts. "
                        f"Last error: {previous_error}"
                    ) from exc

        # Step 3: Run custom transform after AI if order is ai_first
        if custom_code and order == "ai_first":
            data = self._run_custom_code(custom_code, data, state)

        state["transformed_data"] = data
        state["last_error"] = None
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
        instruction: str = state["transform_instruction"]

        previous_error: str | None = state.get("last_error")
        previous_code: str | None = state.get("generated_code")
        retry_count: int = state.get("retry_count", 0)
        total_tokens: dict[str, int] = state.get("token_usage", {})

        start = time.monotonic()
        # Drain the stream (streaming mode) or read raw_data — exactly once,
        # before the retry loop so the single-use iterator isn't re-consumed.
        input_rows = materialize_input_rows(state)
        raw_data_snapshot = copy.deepcopy(input_rows)
        schema_sample = build_schema_sample(input_rows, max_sample_rows=5)
        state["schema_sample"] = schema_sample

        while retry_count < _MAX_RETRIES:
            if retry_count > 0:
                wait = 2**retry_count  # 2s, 4s, 8s
                time.sleep(wait)
            try:
                result: TransformPromptResult = llm_provider.generate_transform_function(
                    schema_sample,
                    instruction,
                    previous_error=previous_error,
                    previous_code=previous_code,
                )
            except LLMAuthError as exc:
                # Non-retryable: a bad key won't fix itself across retries.
                raise TransformError(_human_readable_llm_error(exc)) from exc
            except Exception as exc:
                if not _is_retryable_llm_error(exc):
                    raise TransformError(_human_readable_llm_error(exc)) from exc
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
                transformed = _execute_code(code, list(input_rows), state)
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
        input_rows = materialize_input_rows(state)
        raw_data_snapshot = copy.deepcopy(input_rows)
        data = list(input_rows)
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
            return _execute_code(code, data, state)
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
            if retry_count > 0:
                wait = 2**retry_count  # 2s, 4s, 8s
                time.sleep(wait)
            try:
                result: TransformPromptResult = llm_provider.generate_transform_function(
                    schema_sample,
                    instruction,
                    previous_error=previous_error,
                    previous_code=previous_code,
                    custom_code=custom_code,
                )
            except LLMAuthError as exc:
                # Non-retryable: a bad key won't fix itself across retries.
                raise TransformError(_human_readable_llm_error(exc)) from exc
            except Exception as exc:
                if not _is_retryable_llm_error(exc):
                    raise TransformError(_human_readable_llm_error(exc)) from exc
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
            return _execute_code(code, data, state)
        except Exception:
            raise TransformError(f"AI transform execution failed: {traceback.format_exc()}")
