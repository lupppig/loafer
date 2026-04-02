"""End-to-end CLI schedule command tests.

Tests the full CLI scheduling flow using typer.testing.CliRunner.
Each test uses unique job IDs to avoid conflicts with the shared SQLite store.
"""

from __future__ import annotations

import uuid
from pathlib import Path

from typer.testing import CliRunner

from loafer.cli import app

runner = CliRunner(env={"GEMINI_API_KEY": "test-key-for-cli-tests"})


def _uid() -> str:
    return f"test-{uuid.uuid4().hex[:8]}"


def _make_config(tmp_path: Path) -> Path:
    """Create a minimal pipeline config file."""
    csv_path = tmp_path / "input.csv"
    csv_path.write_text("id,name\n1,Alice\n")

    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(f"""
name: Scheduled Pipeline
source:
  type: csv
  path: {csv_path}

target:
  type: json
  path: {tmp_path}/output.json

transform:
  type: ai
  instruction: lowercase name

mode: etl
chunk_size: 10
streaming_threshold: 1000
""")
    return config_path


class TestCliSchedule:
    """CLI schedule command tests."""

    def test_add_schedule_cron(self, tmp_path: Path) -> None:
        """loafer schedule with --cron → exit code 0."""
        config_path = _make_config(tmp_path)
        job_id = _uid()

        result = runner.invoke(
            app,
            ["schedule", str(config_path), "--cron", "0 9 * * *", "--id", job_id],
        )

        assert result.exit_code == 0
        assert job_id in result.output

    def test_add_schedule_interval(self, tmp_path: Path) -> None:
        """loafer schedule with --interval → exit code 0."""
        config_path = _make_config(tmp_path)
        job_id = _uid()

        result = runner.invoke(
            app,
            ["schedule", str(config_path), "--interval", "1h", "--id", job_id],
        )

        assert result.exit_code == 0
        assert job_id in result.output

    def test_add_schedule_no_trigger_exits_one(self, tmp_path: Path) -> None:
        """loafer schedule without --cron or --interval → exit code 1."""
        config_path = _make_config(tmp_path)

        result = runner.invoke(
            app,
            ["schedule", str(config_path), "--id", _uid()],
        )

        assert result.exit_code == 1

    def test_invalid_cron_exits_one(self, tmp_path: Path) -> None:
        """loafer schedule with invalid cron → exit code 1."""
        config_path = _make_config(tmp_path)

        result = runner.invoke(
            app,
            ["schedule", str(config_path), "--cron", "invalid", "--id", _uid()],
        )

        assert result.exit_code == 1

    def test_replace_schedule(self, tmp_path: Path) -> None:
        """loafer schedule with --replace → exit code 0, replaces existing job."""
        config_path = _make_config(tmp_path)
        job_id = _uid()

        runner.invoke(
            app,
            ["schedule", str(config_path), "--cron", "0 9 * * *", "--id", job_id],
        )

        result = runner.invoke(
            app,
            ["schedule", str(config_path), "--cron", "0 18 * * *", "--id", job_id, "--replace"],
        )

        assert result.exit_code == 0
        assert job_id in result.output
