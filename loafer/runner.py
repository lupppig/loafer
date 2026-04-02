"""Pipeline runner — composition root.

Parses config, builds state, instantiates LLM provider, selects the
correct graph (ETL or ELT), and invokes it.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loafer.config import PipelineConfig, load_config
from loafer.exceptions import PipelineError
from loafer.graph.state import PipelineState

if TYPE_CHECKING:
    from collections.abc import Iterator

    from loafer.llm.base import LLMProvider


def _build_llm_provider(config: PipelineConfig) -> LLMProvider:
    """Instantiate the LLM provider from config."""
    llm_config = config.llm
    api_key = llm_config.api_key
    if not api_key:
        import os

        api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise PipelineError(
            "LLM API key not found. Set 'llm.api_key' in config or GEMINI_API_KEY env var."
        )

    match llm_config.provider:
        case "gemini":
            from loafer.llm.gemini import GeminiProvider

            return GeminiProvider(api_key=api_key, model=llm_config.model)
        case _:
            raise PipelineError(f"Unsupported LLM provider: {llm_config.provider}")


def _build_initial_state(config: PipelineConfig) -> PipelineState:
    """Build the initial PipelineState from a validated config."""
    return PipelineState(
        source_config=config.source,
        target_config=config.target,
        transform_config=config.transform,
        llm_config=config.llm,
        transform_instruction=_get_transform_instruction(config),
        mode=config.mode,
        chunk_size=config.chunk_size,
        streaming_threshold=config.streaming_threshold,
        destructive_filter_threshold=config.destructive_filter_threshold,
        raw_data=[],
        transformed_data=[],
        schema_sample={},
        validation_report={},
        validation_passed=False,
        max_null_rate=config.validation.max_null_rate,
        strict_validation=config.validation.strict,
        generated_code="",
        retry_count=0,
        transform_retry_count=0,
        last_error=None,
        token_usage={},
        raw_table_name=None,
        generated_sql=None,
        run_id=uuid.uuid4().hex[:12],
        rows_extracted=0,
        rows_loaded=0,
        duration_ms={},
        warnings=[],
        is_streaming=False,
        stream_iterator=None,
        destructive_warnings=[],
        auto_confirmed=False,
    )


def _get_transform_instruction(config: PipelineConfig) -> str:
    """Extract the transform instruction from the config."""
    transform = config.transform
    if hasattr(transform, "instruction"):
        return transform.instruction
    if hasattr(transform, "path"):
        return transform.path
    if hasattr(transform, "query"):
        return transform.query
    return ""


def run_pipeline(
    config_path: str | Path,
    dry_run: bool = False,
    verbose: bool = False,
    yes: bool = False,
) -> PipelineState:
    """Run a full ETL or ELT pipeline from a YAML config file.

    Args:
        config_path: Path to the pipeline YAML config.
        dry_run: If True, stop after transform without loading to target.
        verbose: If True, print detailed agent output.
        yes: If True, skip destructive operation confirmations.

    Returns:
        The final PipelineState after pipeline execution.

    Raises:
        PipelineError: If any stage of the pipeline fails.
    """
    start = time.monotonic()

    config = load_config(config_path)
    state = _build_initial_state(config)
    state["auto_confirmed"] = yes

    if config.transform.type == "ai":
        llm_provider = _build_llm_provider(config)
        state["llm_provider"] = llm_provider

    mode = config.mode

    if mode == "etl":
        graph = _build_etl_graph()
    elif mode == "elt":
        graph = _build_elt_graph()
    else:
        raise PipelineError(f"Unknown pipeline mode: {mode}")

    try:
        if dry_run:
            state = _run_dry_run(graph, state, mode)
        else:
            state = graph.invoke(state)
    except Exception as exc:
        total_ms = (time.monotonic() - start) * 1000
        state["duration_ms"]["total"] = total_ms
        _cleanup_source_connector(state)
        raise PipelineError(f"Pipeline failed (run_id={state['run_id']}): {exc}") from exc

    total_ms = (time.monotonic() - start) * 1000
    state["duration_ms"]["total"] = total_ms
    _cleanup_source_connector(state)

    if verbose:
        _print_summary(state)

    return state


def run_pipeline_streaming(
    config_path: str | Path,
    dry_run: bool = False,
    yes: bool = False,
) -> Iterator[tuple[str, str, PipelineState]]:
    """Run pipeline and yield (stage_name, status, state) per completed node.

    Yields:
        ("extract", "done"|"failed", state) — after extraction completes
        ("validate", "done"|"failed"|"skipped", state) — after validation
        ("transform", "done"|"failed"|"skipped", state) — after transform
        ("load", "done"|"failed"|"skipped", state) — after load

    The final yield always has the complete state. Raises PipelineError on
    any stage failure.
    """
    start = time.monotonic()

    config = load_config(config_path)
    state = _build_initial_state(config)
    state["auto_confirmed"] = yes

    if config.transform.type == "ai":
        llm_provider = _build_llm_provider(config)
        state["llm_provider"] = llm_provider

    mode = config.mode

    if mode == "etl":
        graph = _build_etl_graph()
    elif mode == "elt":
        graph = _build_elt_graph()
    else:
        raise PipelineError(f"Unknown pipeline mode: {mode}")

    try:
        if dry_run:
            yield from _stream_dry_run(graph, state, mode, start)
        else:
            yield from _stream_graph(graph, state, mode, start)
    except PipelineError:
        _cleanup_source_connector(state)
        raise
    except Exception as exc:
        total_ms = (time.monotonic() - start) * 1000
        state["duration_ms"]["total"] = total_ms
        _cleanup_source_connector(state)
        raise PipelineError(f"Pipeline failed (run_id={state['run_id']}): {exc}") from exc
    else:
        _cleanup_source_connector(state)


def _stream_graph(
    graph: Any,
    state: PipelineState,
    mode: str,
    start: float,
) -> Iterator[tuple[str, str, PipelineState]]:
    """Stream graph execution, yielding per-node updates."""
    nodes_executed: set[str] = set()

    try:
        for event in graph.stream(state, stream_mode="updates"):
            for node_name, delta in event.items():
                nodes_executed.add(node_name)

                # Merge delta into state
                for key, value in delta.items():
                    state[key] = value  # type: ignore[literal-required]

                # Determine status
                status = "done"
                if node_name in (
                    "extract",
                    "validate",
                    "transform",
                    "load",
                    "load_raw",
                    "transform_in_target",
                ):
                    yield (node_name, status, state)

        # Mark skipped stages
        if mode == "etl":
            expected = {"extract", "validate", "transform", "load"}
        else:
            expected = {"extract", "load_raw", "transform_in_target"}

        for stage in expected - nodes_executed:
            yield (stage, "skipped", state)

    except Exception as exc:
        # Determine which stage failed
        failed_stage = _last_executed_node(nodes_executed, mode)
        if failed_stage:
            yield (failed_stage, "failed", state)
        total_ms = (time.monotonic() - start) * 1000
        state["duration_ms"]["total"] = total_ms
        raise PipelineError(f"Pipeline failed (run_id={state['run_id']}): {exc}") from exc
    else:
        total_ms = (time.monotonic() - start) * 1000
        state["duration_ms"]["total"] = total_ms


def _stream_dry_run(
    graph: Any,
    state: PipelineState,
    mode: str,
    start: float,
) -> Iterator[tuple[str, str, PipelineState]]:
    """Stream dry-run graph execution."""
    from langgraph.graph import END, START, StateGraph

    from loafer.agents.extract import extract_agent
    from loafer.agents.transform import transform_agent
    from loafer.agents.validate import validate_agent

    dry_graph = StateGraph(state_schema=PipelineState)
    dry_graph.add_node("extract", extract_agent)
    dry_graph.add_node("validate", validate_agent)
    dry_graph.add_node("transform", transform_agent)
    dry_graph.add_edge(START, "extract")
    dry_graph.add_edge("extract", "validate")

    def _check_validation_dry(state: PipelineState) -> str:
        if state.get("validation_passed", False):
            return "transform"
        return "end"

    dry_graph.add_conditional_edges(
        "validate",
        _check_validation_dry,
        {"transform": "transform", "end": END},
    )
    dry_graph.add_edge("transform", END)

    compiled = dry_graph.compile()
    nodes_executed: set[str] = set()

    try:
        for event in compiled.stream(state, stream_mode="updates"):  # type: ignore[arg-type]
            for node_name, delta in event.items():
                nodes_executed.add(node_name)
                for key, value in delta.items():
                    state[key] = value  # type: ignore[literal-required]
                yield (node_name, "done", state)

        for stage in {"extract", "validate", "transform"} - nodes_executed:
            yield (stage, "skipped", state)

    except Exception as exc:
        failed_stage = _last_executed_node(nodes_executed, "etl")
        if failed_stage:
            yield (failed_stage, "failed", state)
        total_ms = (time.monotonic() - start) * 1000
        state["duration_ms"]["total"] = total_ms
        raise PipelineError(f"Pipeline failed (run_id={state['run_id']}): {exc}") from exc
    else:
        total_ms = (time.monotonic() - start) * 1000
        state["duration_ms"]["total"] = total_ms


def _last_executed_node(nodes_executed: set[str], mode: str) -> str | None:
    """Return the last node that was executed, for error reporting."""
    if mode == "etl":
        order = ["extract", "validate", "transform", "load"]
    else:
        order = ["extract", "load_raw", "transform_in_target"]
    for node in reversed(order):
        if node in nodes_executed:
            return node
    return None


def _run_dry_run(graph: Any, state: PipelineState, mode: str) -> PipelineState:
    """Run pipeline without the final load step."""
    from langgraph.graph import END, START, StateGraph

    from loafer.agents.extract import extract_agent
    from loafer.agents.transform import transform_agent
    from loafer.agents.validate import validate_agent

    dry_graph = StateGraph(state_schema=PipelineState)
    dry_graph.add_node("extract", extract_agent)
    dry_graph.add_node("validate", validate_agent)
    dry_graph.add_node("transform", transform_agent)

    dry_graph.add_edge(START, "extract")
    dry_graph.add_edge("extract", "validate")

    def _check_validation_dry(state: PipelineState) -> str:
        if state.get("validation_passed", False):
            return "transform"
        return "end"

    dry_graph.add_conditional_edges(
        "validate",
        _check_validation_dry,
        {"transform": "transform", "end": END},
    )
    dry_graph.add_edge("transform", END)

    compiled = dry_graph.compile()
    return compiled.invoke(state)  # type: ignore[arg-type, return-value]


def _print_summary(state: PipelineState) -> None:
    """Print a summary of the pipeline run."""
    from rich.console import Console

    console = Console()
    console.print(f"\n[bold]Pipeline Summary[/bold] (run_id={state.get('run_id', 'unknown')})")
    console.print(f"  Rows extracted: {state.get('rows_extracted', 0)}")
    console.print(f"  Rows loaded:    {state.get('rows_loaded', 0)}")
    console.print(f"  Warnings:       {len(state.get('warnings', []))}")

    if state.get("token_usage"):
        console.print(f"  Token usage:    {state.get('token_usage', {})}")

    console.print(f"  Duration:       {state.get('duration_ms', {}).get('total', 0):.0f}ms")

    if state.get("warnings"):
        console.print("\n[yellow]Warnings:[/yellow]")
        for w in state["warnings"]:
            console.print(f"  - {w}")


def _build_etl_graph() -> Any:
    """Build the ETL graph."""
    from loafer.graph.etl import build_etl_graph

    return build_etl_graph()


def _build_elt_graph() -> Any:
    """Build the ELT graph."""
    from loafer.graph.elt import build_elt_graph

    return build_elt_graph()


def validate_config(config_path: str | Path) -> PipelineConfig:
    """Validate a pipeline config file without running it.

    Args:
        config_path: Path to the pipeline YAML config.

    Returns:
        The validated PipelineConfig.

    Raises:
        PipelineError: If the config is invalid.
    """
    try:
        return load_config(config_path)
    except Exception as exc:
        raise PipelineError(f"Config validation failed: {exc}") from exc


def list_connectors() -> dict[str, list[str]]:
    """List all registered source and target connectors.

    Returns:
        Dict with 'sources' and 'targets' keys listing connector types.
    """
    from loafer.connectors.registry import _SOURCE_REGISTRY, _TARGET_REGISTRY

    return {
        "sources": sorted(_SOURCE_REGISTRY.keys()),
        "targets": sorted(_TARGET_REGISTRY.keys()),
    }


def _cleanup_source_connector(state: PipelineState) -> None:
    """Disconnect the source connector if it was kept alive for streaming."""
    connector = state.get("_source_connector")
    if connector is not None:
        try:
            connector.disconnect()
        except Exception:
            pass
        state["_source_connector"] = None
