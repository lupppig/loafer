"""Tests for ELT graph routing logic."""

from __future__ import annotations

from langgraph.graph import END, START, StateGraph

from loafer.graph.elt import (
    _MAX_GRAPH_RETRIES,
    _check_transform_in_target_retry,
    _check_validation,
    build_elt_graph,
)
from loafer.graph.state import PipelineState


class TestCheckValidation:
    def test_validation_passed_routes_to_load_raw(self) -> None:
        state = {"validation_passed": True}
        assert _check_validation(state) == "load_raw"

    def test_validation_failed_routes_to_end(self) -> None:
        state = {"validation_passed": False}
        assert _check_validation(state) == "end"

    def test_missing_key_routes_to_end(self) -> None:
        state: dict = {}
        assert _check_validation(state) == "end"


class TestCheckTransformInTargetRetry:
    """BUG-3: the router must be pure — it reads the counter but never writes it.

    LangGraph discards state writes made inside routing functions, so the old
    router that bumped the counter saw 0 every pass and retried forever. The
    counter is now incremented in the node; the router only reads it.
    """

    def test_no_error_routes_to_end(self) -> None:
        state: dict = {"last_error": None}
        assert _check_transform_in_target_retry(state) == "end"

    def test_error_with_retries_available(self) -> None:
        state: dict = {"last_error": "some error", "transform_in_target_retry_count": 1}
        assert _check_transform_in_target_retry(state) == "transform_in_target"

    def test_router_does_not_mutate_state(self) -> None:
        state: dict = {"last_error": "some error", "transform_in_target_retry_count": 1}
        _check_transform_in_target_retry(state)
        # Router is pure: counter unchanged (the node owns the increment).
        assert state["transform_in_target_retry_count"] == 1

    def test_error_after_max_retries_routes_to_end(self) -> None:
        state: dict = {"last_error": "still failing", "transform_in_target_retry_count": 3}
        assert _check_transform_in_target_retry(state) == "end"
        assert state["transform_in_target_retry_count"] == 3  # not mutated

    def test_boundary_at_max_retries_still_retries(self) -> None:
        # count == _MAX_GRAPH_RETRIES (2) is the last allowed retry.
        state: dict = {"last_error": "error", "transform_in_target_retry_count": 2}
        assert _check_transform_in_target_retry(state) == "transform_in_target"


class TestBuildEltGraph:
    def test_build_returns_compiled_graph(self) -> None:
        graph = build_elt_graph()
        assert graph is not None

    def test_graph_has_expected_nodes(self) -> None:
        graph = build_elt_graph()
        nodes = list(graph.get_graph().nodes)
        assert "extract" in nodes
        assert "validate" in nodes
        assert "load_raw" in nodes
        assert "transform_in_target" in nodes

    def test_graph_has_edges(self) -> None:
        graph = build_elt_graph()
        edges = graph.get_graph().edges
        assert (
            len(edges) >= 4
        )  # START→extract, extract→validate, load_raw→transform_in_target, + conditional


class TestRetryLoopTerminates:
    """BUG-3: a persistently failing transform must terminate, not spin forever.

    Drives the real pure router against a node that always fails (and bumps
    the counter, as the real node does). The original mutate-in-router bug
    made this loop unboundedly; here it must stop after _MAX_GRAPH_RETRIES.
    """

    def test_always_failing_transform_terminates(self) -> None:
        hops = {"n": 0}

        def failing_transform(state: PipelineState) -> PipelineState:
            hops["n"] += 1
            state["last_error"] = "in-target SQL keeps failing"
            state["transform_in_target_retry_count"] = (
                state.get("transform_in_target_retry_count", 0) + 1
            )
            return state

        graph = StateGraph(state_schema=PipelineState)
        graph.add_node("transform_in_target", failing_transform)
        graph.add_edge(START, "transform_in_target")
        graph.add_conditional_edges(
            "transform_in_target",
            _check_transform_in_target_retry,
            {"transform_in_target": "transform_in_target", "end": END},
        )
        compiled = graph.compile()

        final = compiled.invoke(
            {"last_error": None, "transform_in_target_retry_count": 0},
            config={"recursion_limit": 25},
        )

        # Initial attempt + _MAX_GRAPH_RETRIES retries, then it stops.
        assert hops["n"] == _MAX_GRAPH_RETRIES + 1
        assert final["last_error"] == "in-target SQL keeps failing"
