"""Tests for run_pipeline_streaming."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from loafer.exceptions import PipelineError
from loafer.runner import _raise_on_terminal_failure, _transform_requires_llm


class TestRunPipelineStreaming:
    """Tests for the streaming pipeline execution."""

    def test_streaming_yields_all_etl_stages(self, tmp_path: Path) -> None:
        """ETL mode should yield extract, validate, transform, load."""
        from loafer.runner import run_pipeline_streaming

        # Create a minimal config file
        transform_file = tmp_path / "t.py"
        transform_file.write_text("def transform(data): return data\n")

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n")

        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(
            f"source:\n  type: csv\n  path: {csv_file}\n"
            f"target:\n  type: json\n  path: {tmp_path / 'out.json'}\n"
            f"transform:\n  type: custom\n  path: {transform_file}\n"
            f"mode: etl\n"
        )

        events = list(run_pipeline_streaming(config_file))
        node_names = [e[0] for e in events]

        assert "extract" in node_names
        assert "validate" in node_names
        assert "transform" in node_names
        assert "load" in node_names

    def test_streaming_yields_all_dry_run_stages(self, tmp_path: Path) -> None:
        """Dry run should yield extract, validate, transform (no load)."""
        from loafer.runner import run_pipeline_streaming

        transform_file = tmp_path / "t.py"
        transform_file.write_text("def transform(data): return data\n")

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n")

        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(
            f"source:\n  type: csv\n  path: {csv_file}\n"
            f"target:\n  type: json\n  path: {tmp_path / 'out.json'}\n"
            f"transform:\n  type: custom\n  path: {transform_file}\n"
            f"mode: etl\n"
        )

        events = list(run_pipeline_streaming(config_file, dry_run=True))
        node_names = [e[0] for e in events]

        assert "extract" in node_names
        assert "validate" in node_names
        assert "transform" in node_names
        assert "load" not in node_names

    def test_streaming_status_is_done_for_success(self, tmp_path: Path) -> None:
        """All stages should have status 'done' on success."""
        from loafer.runner import run_pipeline_streaming

        transform_file = tmp_path / "t.py"
        transform_file.write_text("def transform(data): return data\n")

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n")

        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(
            f"source:\n  type: csv\n  path: {csv_file}\n"
            f"target:\n  type: json\n  path: {tmp_path / 'out.json'}\n"
            f"transform:\n  type: custom\n  path: {transform_file}\n"
            f"mode: etl\n"
        )

        events = list(run_pipeline_streaming(config_file))
        statuses = {node_name: status for node_name, status, _state in events}
        # "running" events are emitted before stages complete, "done" after
        for node_name, status in statuses.items():
            assert status in ("running", "done", "skipped"), (
                f"Stage {node_name} had unexpected status: {status}"
            )
        # Verify each stage eventually reaches "done"
        done_stages = {n for n, s in statuses.items() if s == "done"}
        assert "extract" in done_stages
        assert "validate" in done_stages
        assert "transform" in done_stages
        assert "load" in done_stages

    def test_streaming_state_has_rows_extracted(self, tmp_path: Path) -> None:
        """After extract stage, state should have rows_extracted set."""
        from loafer.runner import run_pipeline_streaming

        transform_file = tmp_path / "t.py"
        transform_file.write_text("def transform(data): return data\n")

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(
            f"source:\n  type: csv\n  path: {csv_file}\n"
            f"target:\n  type: json\n  path: {tmp_path / 'out.json'}\n"
            f"transform:\n  type: custom\n  path: {transform_file}\n"
            f"mode: etl\n"
        )

        events = list(run_pipeline_streaming(config_file))
        extract_event = next(e for e in events if e[0] == "extract")
        assert extract_event[2].get("rows_extracted", 0) == 2

    def test_streaming_state_has_duration(self, tmp_path: Path) -> None:
        """Each stage should set its duration in duration_ms."""
        from loafer.runner import run_pipeline_streaming

        transform_file = tmp_path / "t.py"
        transform_file.write_text("def transform(data): return data\n")

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n")

        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(
            f"source:\n  type: csv\n  path: {csv_file}\n"
            f"target:\n  type: json\n  path: {tmp_path / 'out.json'}\n"
            f"transform:\n  type: custom\n  path: {transform_file}\n"
            f"mode: etl\n"
        )

        events = list(run_pipeline_streaming(config_file))
        final_state = events[-1][2]
        assert "extract" in final_state.get("duration_ms", {})
        assert "total" in final_state.get("duration_ms", {})


class TestRunnerFailureContracts:
    def test_exhausted_elt_error_is_terminal(self) -> None:
        with pytest.raises(PipelineError, match="relation does not exist"):
            _raise_on_terminal_failure(
                {"last_error": "relation does not exist"},  # type: ignore[arg-type]
                "elt",
            )

    def test_etl_state_is_not_interpreted_as_elt_terminal_error(self) -> None:
        _raise_on_terminal_failure(
            {"last_error": "handled elsewhere"},  # type: ignore[arg-type]
            "etl",
        )

    def test_pipeline_ai_step_requires_provider(self) -> None:
        ai_step = MagicMock(type="ai", bypass_ai=False)
        custom_step = MagicMock(type="custom")
        config = MagicMock()
        config.transform.type = "pipeline"
        config.transform.steps = [custom_step, ai_step]
        assert _transform_requires_llm(config) is True

    def test_pipeline_bypassed_ai_step_needs_no_provider(self) -> None:
        ai_step = MagicMock(type="ai", bypass_ai=True)
        config = MagicMock()
        config.transform.type = "pipeline"
        config.transform.steps = [ai_step]
        assert _transform_requires_llm(config) is False
