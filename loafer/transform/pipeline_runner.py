"""Multi-step transform pipeline runner.

Executes a sequence of transform steps.  Each step's output becomes the
next step's input.  Every step has its own retry budget, timing, token
accounting, and a freshly recomputed schema sample — so a failure anywhere
in the chain is located precisely.

Steps run in isolation: a step receives only the previous step's output and
cannot reach back into earlier steps' state.  Per-step destructive detection
is suppressed (via ``auto_confirmed``); a single destructive check runs at
the end comparing the original input to the final output, matching the
semantics of a single-step transform.
"""

from __future__ import annotations

import copy
import time
from typing import Any

from loafer.config import (
    AITransformConfig,
    CustomTransformConfig,
    PipelineTransformConfig,
    SQLTransformConfig,
)
from loafer.core.destructive import detect_destructive_operations, raise_if_destructive
from loafer.exceptions import TransformError
from loafer.graph.state import PipelineState, StepResult
from loafer.llm.schema import build_schema_sample
from loafer.transform import TransformRunner, materialize_input_rows

_StepConfig = AITransformConfig | CustomTransformConfig | SQLTransformConfig


def _resolve_step_runner(transform_type: str) -> TransformRunner:
    """Instantiate the runner for a single pipeline step.

    Mirrors the top-level transform agent dispatch, but scoped to the three
    executable step types (a pipeline cannot nest another pipeline).
    """
    from loafer.transform.ai_runner import AiTransformRunner
    from loafer.transform.custom_runner import CustomTransformRunner
    from loafer.transform.sql_runner import SqlTransformRunner

    match transform_type:
        case "ai":
            return AiTransformRunner()
        case "custom":
            return CustomTransformRunner()
        case "sql":
            return SqlTransformRunner()
        case _:
            raise TransformError(
                f"Unknown pipeline step type: '{transform_type}'. Expected one of: ai, custom, sql"
            )


class PipelineTransformRunner(TransformRunner):
    """Execute an ordered list of transform steps."""

    def run(self, state: PipelineState) -> PipelineState:
        config = state.get("transform_config")
        if not isinstance(config, PipelineTransformConfig):
            raise TransformError("pipeline transform requires a PipelineTransformConfig")

        overall_start = time.monotonic()

        # Drain the stream (streaming mode) or read raw_data — exactly once.
        input_rows = materialize_input_rows(state)
        original_input = copy.deepcopy(input_rows)

        step_results: list[StepResult] = state.setdefault("step_results", [])
        aggregate_tokens: dict[str, int] = dict(state.get("token_usage", {}))

        current_data: list[dict[str, Any]] = list(input_rows)

        for index, step_cfg in enumerate(config.steps):
            step_name = getattr(step_cfg, "name", None) or f"step_{index}"
            rows_in = len(current_data)
            step_start = time.monotonic()

            sub_state = self._build_step_state(state, step_cfg, current_data)
            runner = _resolve_step_runner(step_cfg.type)

            try:
                result_state = runner.run(sub_state)
            except TransformError as exc:
                duration_ms = (time.monotonic() - step_start) * 1000
                step_results.append(
                    StepResult(
                        index=index,
                        name=step_name,
                        type=step_cfg.type,
                        rows_in=rows_in,
                        rows_out=0,
                        duration_ms=duration_ms,
                        success=False,
                        error=str(exc),
                    )
                )
                raise TransformError(
                    _format_step_failure(index, step_name, step_cfg.type, exc, step_results)
                ) from exc

            current_data = result_state.get("transformed_data", [])
            duration_ms = (time.monotonic() - step_start) * 1000

            step_tokens = result_state.get("token_usage") if step_cfg.type == "ai" else None
            if step_tokens:
                for key, value in step_tokens.items():
                    aggregate_tokens[key] = aggregate_tokens.get(key, 0) + value

            step_results.append(
                StepResult(
                    index=index,
                    name=step_name,
                    type=step_cfg.type,
                    rows_in=rows_in,
                    rows_out=len(current_data),
                    duration_ms=duration_ms,
                    success=True,
                    error=None,
                    token_usage=step_tokens or None,
                )
            )

            for warning in result_state.get("warnings", []):
                state.setdefault("warnings", []).append(f"[{step_name}] {warning}")

            if len(current_data) == 0 and config.stop_on_empty:
                raise TransformError(_format_empty_step(index, step_name, step_results))

        state["transformed_data"] = current_data
        state["token_usage"] = aggregate_tokens
        state["last_error"] = None
        state["retry_count"] = 0
        state["duration_ms"]["transform"] = (time.monotonic() - overall_start) * 1000

        if len(current_data) == 0:
            state.setdefault("warnings", []).append(
                "Transform pipeline returned 0 rows (filtering may have removed all data)"
            )

        before_state = {"raw_data": original_input}
        after_state = {"transformed_data": current_data}
        threshold = state.get("destructive_filter_threshold", 0.3)
        warnings = detect_destructive_operations(before_state, after_state, threshold)
        raise_if_destructive(warnings, state.get("auto_confirmed", False))
        if warnings:
            state.setdefault("destructive_warnings", []).extend(warnings)

        return state

    def _build_step_state(
        self,
        parent: PipelineState,
        step_cfg: _StepConfig,
        input_rows: list[dict[str, Any]],
    ) -> PipelineState:
        """Build an isolated sub-state for one step.

        A shallow copy of the parent with only the fields a step is allowed to
        see or mutate overridden.  ``auto_confirmed`` is forced True so the
        sub-runner never raises on per-step destructive changes — the pipeline
        performs one destructive check over the whole chain at the end.
        """
        sub: dict[str, Any] = dict(parent)
        sub["transform_config"] = step_cfg
        sub["raw_data"] = input_rows
        sub["transformed_data"] = []
        sub["is_streaming"] = False
        sub["stream_iterator"] = None
        sub["schema_sample"] = build_schema_sample(input_rows)
        sub["retry_count"] = 0
        sub["transform_retry_count"] = 0
        sub["last_error"] = None
        sub["generated_code"] = ""
        sub["token_usage"] = {}
        sub["warnings"] = []
        sub["duration_ms"] = {}
        sub["destructive_warnings"] = []
        sub["auto_confirmed"] = True
        # Pipeline steps always transform in memory; the final load is separate.
        sub["mode"] = "etl"

        if isinstance(step_cfg, AITransformConfig):
            sub["transform_instruction"] = step_cfg.instruction

        return sub  # type: ignore[return-value]


def _format_step_failure(
    index: int,
    name: str,
    step_type: str,
    error: Exception,
    step_results: list[StepResult],
) -> str:
    """Build a precise failure message locating the broken step in the chain."""
    lines = [
        f"Transform pipeline failed at step {index} ('{name}', type={step_type}).",
        f"  Error: {error}",
    ]
    prior = [r for r in step_results if r.index < index and r.success]
    if prior:
        lines.append("  Prior steps:")
        for r in prior:
            lines.append(
                f"    • step {r.index} ('{r.name}', {r.type}): {r.rows_in} → {r.rows_out} rows"
            )
    return "\n".join(lines)


def _format_empty_step(index: int, name: str, step_results: list[StepResult]) -> str:
    """Build a message when a step empties the dataset under stop_on_empty."""
    lines = [
        f"Transform pipeline halted: step {index} ('{name}') produced 0 rows "
        "and stop_on_empty is enabled.",
        "  Row counts per step:",
    ]
    for r in step_results:
        lines.append(
            f"    • step {r.index} ('{r.name}', {r.type}): {r.rows_in} → {r.rows_out} rows"
        )
    lines.append(
        "  Set 'stop_on_empty: false' on the pipeline transform to continue past empty steps."
    )
    return "\n".join(lines)
