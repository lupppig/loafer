"""Prepare row-local transform artifacts once and execute them per batch."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loafer.config import (
    AITransformConfig,
    CustomTransformConfig,
    PipelineTransformConfig,
)
from loafer.core.sandbox import run_sandboxed
from loafer.exceptions import TransformError
from loafer.graph.state import PipelineState
from loafer.llm.schema import build_schema_sample
from loafer.transform.ai_runner import AiTransformRunner
from loafer.transform.code_validator import validate_transform_function


@dataclass(frozen=True)
class CodeArtifact:
    """One validated custom or AI-generated row-local code artifact."""

    name: str
    kind: str
    code: str


@dataclass(frozen=True)
class PreparedBatchTransform:
    """Immutable transform artifact reused for every batch in one run."""

    version: str
    steps: tuple[CodeArtifact, ...]
    stop_on_empty: bool = False


def prepare_transform_artifact(
    state: PipelineState,
    sample_rows: list[dict[str, Any]],
) -> PreparedBatchTransform:
    """Generate/load and validate all code once from a bounded sample."""
    config = state.get("transform_config")
    if isinstance(config, CustomTransformConfig):
        steps = (_custom_artifact(config.path, config.name or "custom"),)
        return _prepared(steps)
    if isinstance(config, AITransformConfig):
        steps = _prepare_ai_artifacts(config, state, sample_rows)
        return _prepared(steps)
    if isinstance(config, PipelineTransformConfig):
        return _prepare_pipeline(config, state, sample_rows)
    raise TransformError(
        "bounded row_local execution supports custom, AI, or custom/AI pipeline transforms"
    )


def transform_batch(
    artifact: PreparedBatchTransform,
    rows: list[dict[str, Any]],
    state: PipelineState,
) -> list[dict[str, Any]]:
    """Execute the same prepared artifact against one bounded batch."""
    current = list(rows)
    for step in artifact.steps:
        current = _execute(step.code, current, state)
        if not current and artifact.stop_on_empty:
            raise TransformError(
                f"row-local transform step '{step.name}' produced 0 rows and "
                "stop_on_empty is enabled"
            )
    return current


def _prepare_pipeline(
    config: PipelineTransformConfig,
    state: PipelineState,
    sample_rows: list[dict[str, Any]],
) -> PreparedBatchTransform:
    artifacts: list[CodeArtifact] = []
    current_sample = list(sample_rows)

    for index, step in enumerate(config.steps):
        name = step.name or f"step_{index}"
        if isinstance(step, CustomTransformConfig):
            step_artifacts = (_custom_artifact(step.path, name),)
        elif isinstance(step, AITransformConfig):
            step_artifacts = _prepare_ai_artifacts(step, state, current_sample, name=name)
        else:
            raise TransformError(
                "SQL pipeline steps require global relational semantics and cannot execute "
                "through transform_batch"
            )

        artifacts.extend(step_artifacts)
        for artifact in step_artifacts:
            current_sample = _execute(artifact.code, current_sample, state)
        if not current_sample and config.stop_on_empty:
            raise TransformError(
                f"row-local transform step '{name}' produced 0 sample rows and "
                "stop_on_empty is enabled"
            )

    return _prepared(tuple(artifacts), stop_on_empty=config.stop_on_empty)


def _prepare_ai_artifacts(
    config: AITransformConfig,
    state: PipelineState,
    sample_rows: list[dict[str, Any]],
    *,
    name: str | None = None,
) -> tuple[CodeArtifact, ...]:
    custom: CodeArtifact | None = None
    if config.custom_path:
        custom = _custom_artifact(config.custom_path, f"{name or 'ai'}_custom")

    if config.bypass_ai:
        if custom is None:
            raise TransformError(
                "bypass_ai is set but no custom_path is configured for the row-local transform"
            )
        return (custom,)

    prompt_rows = list(sample_rows)
    if custom is not None and config.custom_order == "custom_first":
        prompt_rows = _execute(custom.code, prompt_rows, state)

    provider = state.get("llm_provider")
    if provider is None:
        raise TransformError("row-local AI transform requires an LLM provider")

    generator = AiTransformRunner()
    generated = generator._generate_ai_code(
        provider,
        build_schema_sample(prompt_rows, max_sample_rows=5),
        config.instruction,
        custom.code if custom is not None else None,
        state,
    )
    if not generated:
        raise TransformError("AI provider returned no transform artifact")

    if config.review:
        reviewer = state.get("reviewer")
        approved = reviewer.approve_transform(generated) if reviewer is not None else False
        if not approved:
            return (custom,) if custom is not None else ()

    ai = CodeArtifact(name=name or config.name or "ai", kind="ai", code=generated)
    if custom is None:
        return (ai,)
    if config.custom_order == "custom_first":
        return custom, ai
    return ai, custom


def _custom_artifact(path: str, name: str) -> CodeArtifact:
    source = Path(path)
    if not source.exists():
        raise TransformError(f"transform file not found: {source}")
    code = source.read_text(encoding="utf-8")
    valid, reason = validate_transform_function(code)
    if not valid:
        raise TransformError(f"Custom transform validation failed: {reason}")
    return CodeArtifact(name=name, kind="custom", code=code)


def _prepared(
    steps: tuple[CodeArtifact, ...],
    *,
    stop_on_empty: bool = False,
) -> PreparedBatchTransform:
    serialized = json.dumps(
        [{"name": step.name, "kind": step.kind, "code": step.code} for step in steps],
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    version = f"sha256:{hashlib.sha256(serialized).hexdigest()}"
    return PreparedBatchTransform(
        version=version,
        steps=steps,
        stop_on_empty=stop_on_empty,
    )


def _execute(
    code: str,
    rows: list[dict[str, Any]],
    state: PipelineState,
) -> list[dict[str, Any]]:
    sandbox = state.get("sandbox_config")
    timeout = getattr(sandbox, "timeout", 60)
    max_memory_mb = getattr(sandbox, "max_memory_mb", 512)
    return run_sandboxed(
        code,
        rows,
        timeout=timeout,
        max_memory_mb=max_memory_mb,
    )


__all__ = [
    "CodeArtifact",
    "PreparedBatchTransform",
    "prepare_transform_artifact",
    "transform_batch",
]
