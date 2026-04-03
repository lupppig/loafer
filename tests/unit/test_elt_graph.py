"""Tests for ELT graph routing logic."""

from __future__ import annotations

from loafer.graph.elt import _check_transform_in_target_retry, _check_validation, build_elt_graph


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
    def test_no_error_routes_to_end(self) -> None:
        state: dict = {"last_error": None}
        assert _check_transform_in_target_retry(state) == "end"

    def test_error_with_retries_available(self) -> None:
        state: dict = {"last_error": "some error", "transform_in_target_retry_count": 0}
        assert _check_transform_in_target_retry(state) == "transform_in_target"
        assert state["transform_in_target_retry_count"] == 1

    def test_error_after_max_retries_routes_to_end(self) -> None:
        state: dict = {"last_error": "still failing", "transform_in_target_retry_count": 2}
        assert _check_transform_in_target_retry(state) == "end"
        assert state["transform_in_target_retry_count"] == 2  # not incremented

    def test_first_retry_increments_count(self) -> None:
        state: dict = {"last_error": "error", "transform_in_target_retry_count": 0}
        _check_transform_in_target_retry(state)
        assert state["transform_in_target_retry_count"] == 1

    def test_second_retry_increments_count(self) -> None:
        state: dict = {"last_error": "error", "transform_in_target_retry_count": 1}
        _check_transform_in_target_retry(state)
        assert state["transform_in_target_retry_count"] == 2


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
