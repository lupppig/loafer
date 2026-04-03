"""Tests for ETL graph routing logic and streaming paths."""

from __future__ import annotations

from loafer.graph.etl import (
    _check_transform_retry,
    _check_validation,
    _clear_transform_error,
    build_etl_graph,
)


class TestCheckValidation:
    def test_validation_passed_routes_to_transform(self) -> None:
        state = {"validation_passed": True}
        assert _check_validation(state) == "transform"

    def test_validation_failed_routes_to_end(self) -> None:
        state = {"validation_passed": False}
        assert _check_validation(state) == "end"

    def test_missing_key_routes_to_end(self) -> None:
        state: dict = {}
        assert _check_validation(state) == "end"


class TestCheckTransformRetry:
    def test_no_error_routes_to_load(self) -> None:
        state: dict = {"last_error": None}
        assert _check_transform_retry(state) == "load"

    def test_error_with_retries_available(self) -> None:
        state: dict = {"last_error": "some error", "transform_retry_count": 0}
        assert _check_transform_retry(state) == "transform"
        assert state["transform_retry_count"] == 1

    def test_error_after_max_retries_routes_to_end(self) -> None:
        state: dict = {"last_error": "still failing", "transform_retry_count": 2}
        assert _check_transform_retry(state) == "end"
        assert state["transform_retry_count"] == 2  # not incremented

    def test_first_retry_increments_count(self) -> None:
        state: dict = {"last_error": "error", "transform_retry_count": 0}
        _check_transform_retry(state)
        assert state["transform_retry_count"] == 1

    def test_second_retry_increments_count(self) -> None:
        state: dict = {"last_error": "error", "transform_retry_count": 1}
        _check_transform_retry(state)
        assert state["transform_retry_count"] == 2


class TestClearTransformError:
    def test_clears_last_error(self) -> None:
        state: dict = {"last_error": "some error"}
        result = _clear_transform_error(state)
        assert result["last_error"] is None
        assert state is result  # same object mutated


class TestBuildEtlGraph:
    def test_build_returns_compiled_graph(self) -> None:
        graph = build_etl_graph()
        assert graph is not None

    def test_graph_has_expected_nodes(self) -> None:
        graph = build_etl_graph()
        nodes = list(graph.get_graph().nodes)
        assert "extract" in nodes
        assert "validate" in nodes
        assert "transform" in nodes
        assert "load" in nodes

    def test_graph_has_edges(self) -> None:
        graph = build_etl_graph()
        edges = graph.get_graph().edges
        assert len(edges) >= 4  # START→extract, extract→validate, load→END, + conditional
