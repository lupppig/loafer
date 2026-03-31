"""ETL Pipeline Graph — LangGraph StateGraph.

Flow: extract → validate → (if passed) → transform → load → END
                           (if failed) → END with error

Transform has graph-level retry: up to 2 retries after internal retry exhaustion.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from langgraph.graph import END, START, StateGraph

from loafer.agents.extract import extract_agent
from loafer.agents.load import load_agent
from loafer.agents.transform import transform_agent
from loafer.agents.validate import validate_agent

if TYPE_CHECKING:
    from loafer.graph.state import PipelineState

_MAX_GRAPH_RETRIES = 2


def _check_validation(state: PipelineState) -> str:
    """Route based on validation result."""
    if state.get("validation_passed", False):
        return "transform"
    return "end"


def _check_transform_retry(state: PipelineState) -> str:
    """Route based on transform success or retry availability."""
    if state.get("last_error") is None:
        return "load"

    retry_count = state.get("transform_retry_count", 0)
    if retry_count < _MAX_GRAPH_RETRIES:
        state["transform_retry_count"] = retry_count + 1
        return "transform"

    return "end"


def _clear_transform_error(state: PipelineState) -> PipelineState:
    """Reset transform error state before retry."""
    state["last_error"] = None
    return state


def build_etl_graph() -> Any:
    """Build and compile the ETL pipeline graph.

    Returns a CompiledStateGraph ready to invoke.
    """
    from loafer.graph.state import PipelineState

    graph = StateGraph(state_schema=PipelineState)

    graph.add_node("extract", extract_agent)
    graph.add_node("validate", validate_agent)
    graph.add_node("transform", transform_agent)
    graph.add_node("load", load_agent)

    graph.add_edge(START, "extract")
    graph.add_edge("extract", "validate")

    graph.add_conditional_edges(
        "validate",
        _check_validation,
        {
            "transform": "transform",
            "end": END,
        },
    )

    graph.add_conditional_edges(
        "transform",
        _check_transform_retry,
        {
            "load": "load",
            "transform": "transform",
            "end": END,
        },
    )

    graph.add_edge("load", END)

    return graph.compile()
