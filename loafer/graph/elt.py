"""ELT Pipeline Graph — LangGraph StateGraph.

Flow: extract → validate → (if passed) → load_raw → transform_in_target → END
                               (if failed) → END with error

Transform-in-target has graph-level retry: up to 2 retries after internal
retry exhaustion.
"""

from __future__ import annotations

from typing import Any

from langgraph.graph import END, START, StateGraph

from loafer.agents.extract import extract_agent
from loafer.agents.load_raw import load_raw_agent
from loafer.agents.transform_in_target import transform_in_target_agent
from loafer.agents.validate import validate_agent
from loafer.graph.state import PipelineState

_MAX_GRAPH_RETRIES = 2


def _check_validation(state: PipelineState) -> str:
    """Route based on validation result."""
    if state.get("validation_passed", False):
        return "load_raw"
    return "end"


def _check_transform_in_target_retry(state: PipelineState) -> str:
    """Route based on transform-in-target success or retry availability."""
    if state.get("last_error") is None:
        return "end"

    retry_count = state.get("transform_in_target_retry_count", 0)
    if retry_count < _MAX_GRAPH_RETRIES:
        state["transform_in_target_retry_count"] = retry_count + 1
        return "transform_in_target"

    return "end"


def build_elt_graph() -> Any:
    """Build and compile the ELT pipeline graph.

    Returns a CompiledStateGraph ready to invoke.
    """

    graph = StateGraph(state_schema=PipelineState)

    graph.add_node("extract", extract_agent)
    graph.add_node("validate", validate_agent)
    graph.add_node("load_raw", load_raw_agent)
    graph.add_node("transform_in_target", transform_in_target_agent)

    graph.add_edge(START, "extract")
    graph.add_edge("extract", "validate")

    graph.add_conditional_edges(
        "validate",
        _check_validation,
        {
            "load_raw": "load_raw",
            "end": END,
        },
    )

    graph.add_edge("load_raw", "transform_in_target")

    graph.add_conditional_edges(
        "transform_in_target",
        _check_transform_in_target_retry,
        {
            "transform_in_target": "transform_in_target",
            "end": END,
        },
    )

    return graph.compile()
