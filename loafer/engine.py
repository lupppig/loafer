"""In-process ETL/ELT engine.

This module owns graph selection and execution. It has no CLI rendering or
client-framework dependencies; callers use the application service.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from loafer.config import PipelineConfig
from loafer.exceptions import LLMError, PipelineError
from loafer.graph.state import PipelineState

if TYPE_CHECKING:
    from collections.abc import Iterator

    from loafer.llm.base import LLMProvider
    from loafer.ports.runtime import (
        CancellationPort,
        CheckpointPort,
        ReviewPort,
        SecretResolver,
    )

_PROVIDER_ENV_VARS = {
    "gemini": "GEMINI_API_KEY",
    "claude": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "qwen": "DASHSCOPE_API_KEY",
}

# Hard backstop on graph hops. The ELT graph caps its own retries via the
# transform_in_target counter, but a stuck conditional edge could still loop;
# this ensures LangGraph raises GraphRecursionError instead of spinning
# forever (the original BUG-3 symptom under graph.stream).
_GRAPH_CONFIG = {"recursion_limit": 25}


class ProviderFactory(Protocol):
    """Construct an LLM provider using the caller's secret boundary."""

    def __call__(
        self,
        config: PipelineConfig,
        secret_resolver: SecretResolver | None,
    ) -> LLMProvider: ...


def _build_llm_provider(
    config: PipelineConfig,
    secret_resolver: SecretResolver | None = None,
) -> LLMProvider:
    """Instantiate the LLM provider from config."""
    llm_config = config.llm
    provider = llm_config.provider
    api_key = llm_config.api_key

    if not api_key:
        env_var = _PROVIDER_ENV_VARS.get(provider)
        if env_var and secret_resolver is not None:
            api_key = secret_resolver.resolve(env_var)
        if not api_key:
            raise LLMError(
                f"Missing API key for {provider}.\n"
                f"Set 'llm.api_key' in your config file, or export the environment variable:\n"
                f'  export {_PROVIDER_ENV_VARS.get(provider, "API_KEY")}="your-key"'
            )

    match provider:
        case "gemini":
            from loafer.llm.gemini import GeminiProvider

            return GeminiProvider(api_key=api_key, model=llm_config.model)
        case "claude":
            from loafer.llm.claude import ClaudeProvider

            return ClaudeProvider(api_key=api_key, model=llm_config.model)
        case "openai":
            from loafer.llm.openai import OpenAIProvider

            return OpenAIProvider(api_key=api_key, model=llm_config.model)
        case "qwen":
            from loafer.llm.qwen import QwenProvider

            return QwenProvider(api_key=api_key, model=llm_config.model)
        case _:
            available = ", ".join(_PROVIDER_ENV_VARS.keys())
            raise LLMError(f"Unknown LLM provider: {provider!r}.\nSupported providers: {available}")


def _build_initial_state(
    config: PipelineConfig,
    config_path: str | Path | None = None,
    full_refresh: bool = False,
    run_id: str | None = None,
    reviewer: ReviewPort | None = None,
) -> PipelineState:
    """Build the initial PipelineState from a validated config.

    When ``config.incremental`` is set, the saved watermark is loaded from the
    state file next to *config_path* (unless *full_refresh*), falling back to
    ``incremental.initial``.
    """
    state_key = config.name or (Path(config_path).stem if config_path else "pipeline")
    state_store_path: str | None = None
    cursor_value: Any = None

    if config.incremental is not None and config_path is not None:
        from loafer.core.incremental import StateStore, state_path_for

        store_path = state_path_for(config_path)
        state_store_path = str(store_path)
        if not full_refresh:
            cursor_value = StateStore(store_path).get_cursor(state_key)
        if cursor_value is None:
            cursor_value = config.incremental.initial

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
        execution_config=config.execution,
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
        run_id=run_id or uuid.uuid4().hex[:12],
        rows_extracted=0,
        rows_transformed=0,
        rows_loaded=0,
        rows_rejected=0,
        rows_filtered=0,
        batches_completed=0,
        bytes_in=0,
        bytes_out=0,
        input_checksum=None,
        output_checksum=None,
        schema_version=None,
        transform_artifact_version=None,
        last_batch_envelope=None,
        target_published=False,
        duration_ms={},
        warnings=[],
        is_streaming=False,
        stream_iterator=None,
        destructive_warnings=[],
        auto_confirmed=False,
        incremental_config=config.incremental,
        cursor_value=cursor_value,
        new_cursor=cursor_value,
        state_key=state_key,
        state_store_path=state_store_path,
        sandbox_config=config.sandbox,
        reviewer=reviewer,
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


def execute_pipeline(
    config: PipelineConfig,
    *,
    config_path: str | Path | None = None,
    dry_run: bool = False,
    auto_confirm: bool = False,
    full_refresh: bool = False,
    run_id: str | None = None,
    reviewer: ReviewPort | None = None,
    secret_resolver: SecretResolver | None = None,
    provider_factory: ProviderFactory | None = None,
    cancellation: CancellationPort | None = None,
    checkpoints: CheckpointPort | None = None,
) -> PipelineState:
    """Execute a validated ETL or ELT pipeline.

    Args:
        config: Validated pipeline configuration.
        config_path: Original path, used for incremental cursor state.
        dry_run: If True, stop after transform without loading to target.
        auto_confirm: If True, skip destructive operation confirmations.
        full_refresh: If True, ignore a saved incremental cursor.
        run_id: Caller-assigned run identifier.
        reviewer: Human-review port for generated code.
        secret_resolver: Resolver for provider credential references.

    Returns:
        The final PipelineState after pipeline execution.

    Raises:
        PipelineError: If any stage of the pipeline fails.
    """
    start = time.monotonic()

    state = _build_initial_state(
        config,
        config_path,
        full_refresh,
        run_id=run_id,
        reviewer=reviewer,
    )
    state["auto_confirmed"] = auto_confirm

    if _transform_requires_llm(config):
        factory = provider_factory or _build_llm_provider
        state["llm_provider"] = factory(config, secret_resolver)

    from loafer.data_plane import stream_bounded_pipeline, uses_bounded_data_plane

    if uses_bounded_data_plane(config):
        try:
            for _stage, _status, _state in stream_bounded_pipeline(
                config,
                state,
                dry_run=dry_run,
                cancellation=cancellation,
                checkpoints=checkpoints,
            ):
                pass
        except PipelineError:
            raise
        except Exception as exc:
            raise PipelineError(f"Pipeline failed (run_id={state['run_id']}): {exc}") from exc
        if not dry_run:
            _persist_cursor(state)
        return state

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
            state = graph.invoke(state, config=_GRAPH_CONFIG)
            _raise_on_terminal_failure(state, mode)
    except Exception as exc:
        total_ms = (time.monotonic() - start) * 1000
        state["duration_ms"]["total"] = total_ms
        _cleanup_source_connector(state)
        _cleanup_elt_staging(state, mode)
        raise PipelineError(f"Pipeline failed (run_id={state['run_id']}): {exc}") from exc

    total_ms = (time.monotonic() - start) * 1000
    state["duration_ms"]["total"] = total_ms
    _cleanup_source_connector(state)
    _cleanup_elt_staging(state, mode)

    if not dry_run:
        _persist_cursor(state)

    return state


def stream_pipeline(
    config: PipelineConfig,
    *,
    config_path: str | Path | None = None,
    dry_run: bool = False,
    auto_confirm: bool = False,
    full_refresh: bool = False,
    run_id: str | None = None,
    reviewer: ReviewPort | None = None,
    secret_resolver: SecretResolver | None = None,
    provider_factory: ProviderFactory | None = None,
    cancellation: CancellationPort | None = None,
    checkpoints: CheckpointPort | None = None,
) -> Iterator[tuple[str, str, PipelineState]]:
    """Execute a validated pipeline and yield per-stage runtime updates.

    Yields:
        ("extract", "done"|"failed", state) — after extraction completes
        ("validate", "done"|"failed"|"skipped", state) — after validation
        ("transform", "done"|"failed"|"skipped", state) — after transform
        ("load", "done"|"failed"|"skipped", state) — after load

    The final yield always has the complete state. Raises PipelineError on
    any stage failure.
    """
    start = time.monotonic()

    state = _build_initial_state(
        config,
        config_path,
        full_refresh,
        run_id=run_id,
        reviewer=reviewer,
    )
    state["auto_confirmed"] = auto_confirm

    if _transform_requires_llm(config):
        factory = provider_factory or _build_llm_provider
        state["llm_provider"] = factory(config, secret_resolver)

    from loafer.data_plane import stream_bounded_pipeline, uses_bounded_data_plane

    if uses_bounded_data_plane(config):
        try:
            yield from stream_bounded_pipeline(
                config,
                state,
                dry_run=dry_run,
                cancellation=cancellation,
                checkpoints=checkpoints,
            )
        finally:
            if not dry_run and state.get("target_published", False):
                _persist_cursor(state)
        return

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
        _cleanup_elt_staging(state, mode)
        raise
    except Exception as exc:
        total_ms = (time.monotonic() - start) * 1000
        state["duration_ms"]["total"] = total_ms
        _cleanup_source_connector(state)
        _cleanup_elt_staging(state, mode)
        raise PipelineError(f"Pipeline failed (run_id={state['run_id']}): {exc}") from exc
    else:
        _cleanup_source_connector(state)
        _cleanup_elt_staging(state, mode)
        if not dry_run:
            _persist_cursor(state)


def _stream_graph(
    graph: Any,
    state: PipelineState,
    mode: str,
    start: float,
) -> Iterator[tuple[str, str, PipelineState]]:
    """Stream graph execution, yielding per-node updates."""
    nodes_executed: set[str] = set()

    stage_order = (
        ["extract", "validate", "transform", "load"]
        if mode == "etl"
        else ["extract", "load_raw", "transform_in_target"]
    )

    # Yield "running" before the first stage starts
    if stage_order:
        yield (stage_order[0], "running", state)

    try:
        for event in graph.stream(state, stream_mode="updates", config=_GRAPH_CONFIG):
            for node_name, delta in event.items():
                nodes_executed.add(node_name)

                # Merge delta into state
                for key, value in delta.items():
                    state[key] = value  # type: ignore[literal-required]

                # Yield "done" for completed stages
                if node_name in (
                    "extract",
                    "validate",
                    "transform",
                    "load",
                    "load_raw",
                    "transform_in_target",
                ):
                    if node_name == "transform_in_target" and state.get("last_error"):
                        continue
                    yield (node_name, "done", state)

                    # Yield "running" for the next expected stage
                    next_stages = [s for s in stage_order if s not in nodes_executed]
                    if next_stages:
                        yield (next_stages[0], "running", state)

        # Mark skipped stages
        expected = set(stage_order)

        for stage in expected - nodes_executed:
            yield (stage, "skipped", state)

        if mode == "elt" and state.get("last_error"):
            yield ("transform_in_target", "failed", state)
            total_ms = (time.monotonic() - start) * 1000
            state["duration_ms"]["total"] = total_ms
            raise PipelineError(
                f"Pipeline failed (run_id={state['run_id']}): {state['last_error']}"
            )

    except PipelineError:
        raise
    except Exception as exc:
        # The stage that failed is the next expected stage that hasn't completed
        failed_stage = next(
            (s for s in stage_order if s not in nodes_executed),
            _last_executed_node(nodes_executed, mode),
        )
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


def _build_etl_graph() -> Any:
    """Build the ETL graph."""
    from loafer.graph.etl import build_etl_graph

    return build_etl_graph()


def _build_elt_graph() -> Any:
    """Build the ELT graph."""
    from loafer.graph.elt import build_elt_graph

    return build_elt_graph()


def _persist_cursor(state: PipelineState) -> None:
    """Advance the saved watermark after a successful incremental run."""
    if state.get("incremental_config") is None:
        return
    store_path = state.get("state_store_path")
    new_cursor = state.get("new_cursor")
    if not store_path or new_cursor is None:
        return

    from loafer.core.incremental import StateStore

    StateStore(store_path).set_cursor(state["state_key"], new_cursor)


def _transform_requires_llm(config: PipelineConfig) -> bool:
    """Return whether the top-level transform or any pipeline step uses AI."""
    transform = config.transform
    if transform.type == "ai":
        return not transform.bypass_ai
    if transform.type == "pipeline":
        return any(step.type == "ai" and not step.bypass_ai for step in transform.steps)
    return False


def _cleanup_source_connector(state: PipelineState) -> None:
    """Disconnect the source connector if it was kept alive for streaming."""
    connector = state.get("_source_connector")
    if connector is not None:
        try:
            connector.disconnect()
        except Exception:
            pass
        state["_source_connector"] = None


def _raise_on_terminal_failure(state: PipelineState, mode: str) -> None:
    """Raise when a graph exhausted retries but returned an error state."""
    if mode == "elt" and state.get("last_error"):
        raise PipelineError(str(state["last_error"]))


def _cleanup_elt_staging(state: PipelineState, mode: str) -> None:
    """Best-effort removal of the per-run PostgreSQL staging table."""
    if mode != "elt":
        return

    raw_table = state.get("raw_table_name")
    target_config = state.get("target_config")
    if not raw_table or not hasattr(target_config, "url"):
        return

    conn: Any | None = None
    cursor: Any | None = None
    try:
        import psycopg2
        from psycopg2 import sql as pg_sql

        from loafer.adapters.postgres_sql import qualified_identifier

        conn = psycopg2.connect(target_config.url)
        cursor = conn.cursor()
        cursor.execute(
            pg_sql.SQL("DROP TABLE IF EXISTS {}").format(qualified_identifier(raw_table))
        )
        conn.commit()
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        state.setdefault("warnings", []).append(
            f"Could not remove ELT staging table '{raw_table}': {exc}"
        )
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
