"""ELT Pipeline Graph — LangGraph StateGraph.

Flow: extract → load_raw → transform_in_target → END

Raw data is loaded to the target database as a staging table,
then LLM-generated SQL creates the output table via CREATE TABLE AS SELECT.
"""

from __future__ import annotations

from typing import Any

from langgraph.graph import END, START, StateGraph

from loafer.agents.extract import extract_agent
from loafer.agents.load_raw import load_raw_agent
from loafer.agents.transform_in_target import transform_in_target_agent
from loafer.graph.state import PipelineState


def build_elt_graph() -> Any:
    """Build and compile the ELT pipeline graph.

    Returns a CompiledStateGraph ready to invoke.
    """

    graph = StateGraph(state_schema=PipelineState)

    graph.add_node("extract", extract_agent)
    graph.add_node("load_raw", load_raw_agent)
    graph.add_node("transform_in_target", transform_in_target_agent)

    graph.add_edge(START, "extract")
    graph.add_edge("extract", "load_raw")
    graph.add_edge("load_raw", "transform_in_target")
    graph.add_edge("transform_in_target", END)

    return graph.compile()
