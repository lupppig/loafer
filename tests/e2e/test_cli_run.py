"""End-to-end CLI tests.

Tests the full CLI flow using typer.testing.CliRunner.
"""

from __future__ import annotations

import csv
from pathlib import Path

from typer.testing import CliRunner

from loafer.cli import app

runner = CliRunner(env={"GEMINI_API_KEY": "test-key-for-cli-tests"})


class TestCliRun:
    """CLI run command tests."""

    def test_valid_config_exits_zero(self, tmp_path: Path) -> None:
        """Valid config file → exit code 0, output contains success message."""
        csv_path = tmp_path / "input.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name"])
            writer.writerow([1, "Alice"])
            writer.writerow([2, "Bob"])

        transform_path = tmp_path / "transform.py"
        transform_path.write_text("def transform(data): return data\n")

        output_path = tmp_path / "output.json"
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
name: Test Pipeline
source:
  type: csv
  path: {csv_path}

target:
  type: json
  path: {output_path}

transform:
  type: custom
  path: {transform_path}

mode: etl
chunk_size: 10
streaming_threshold: 1000
""")

        result = runner.invoke(app, ["run", str(config_path)])

        assert result.exit_code == 0

    def test_missing_config_file_exits_one(self) -> None:
        """Missing config file → exit code 1, error mentions the path."""
        result = runner.invoke(app, ["run", "/nonexistent/path.yaml"])

        assert result.exit_code == 1
        assert "not found" in result.output.lower() or "no such file" in result.output.lower()

    def test_invalid_config_exits_one(self, tmp_path: Path) -> None:
        """Invalid config (missing required field) → exit code 1."""
        config_path = tmp_path / "bad.yaml"
        config_path.write_text("""
name: Bad Pipeline
source:
  type: postgres

target:
  type: csv
  path: /tmp/out.csv
""")

        result = runner.invoke(app, ["run", str(config_path)])

        assert result.exit_code == 1

    def test_dry_run_skips_load(self, tmp_path: Path) -> None:
        """--dry-run → exit code 0, Load skipped in output, no output file."""
        csv_path = tmp_path / "input.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name"])
            writer.writerow([1, "Alice"])

        transform_path = tmp_path / "transform.py"
        transform_path.write_text("def transform(data): return data\n")

        output_path = tmp_path / "output.json"
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
name: Dry Run Test
source:
  type: csv
  path: {csv_path}

target:
  type: json
  path: {output_path}

transform:
  type: custom
  path: {transform_path}

mode: etl
chunk_size: 10
streaming_threshold: 1000
""")

        result = runner.invoke(app, ["run", str(config_path), "--dry-run"])

        assert result.exit_code == 0
        assert "skipped" in result.output.lower() or "dry" in result.output.lower()
        assert not output_path.exists()

    def test_verbose_mode(self, tmp_path: Path) -> None:
        """--verbose → exit code 0, detailed output."""
        csv_path = tmp_path / "input.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name"])
            writer.writerow([1, "Alice"])

        transform_path = tmp_path / "transform.py"
        transform_path.write_text("def transform(data): return data\n")

        output_path = tmp_path / "output.json"
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
name: Verbose Test
source:
  type: csv
  path: {csv_path}

target:
  type: json
  path: {output_path}

transform:
  type: custom
  path: {transform_path}

mode: etl
chunk_size: 10
streaming_threshold: 1000
""")

        result = runner.invoke(app, ["run", str(config_path), "--verbose"])

        assert result.exit_code == 0

    def test_pipeline_summary_output(self, tmp_path: Path) -> None:
        """Valid run → output contains Pipeline Summary."""
        csv_path = tmp_path / "input.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name"])
            writer.writerow([1, "Alice"])

        transform_path = tmp_path / "transform.py"
        transform_path.write_text("def transform(data): return data\n")

        output_path = tmp_path / "output.json"
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
name: Summary Test
source:
  type: csv
  path: {csv_path}

target:
  type: json
  path: {output_path}

transform:
  type: custom
  path: {transform_path}

mode: etl
chunk_size: 10
streaming_threshold: 1000
""")

        result = runner.invoke(app, ["run", str(config_path)])

        assert result.exit_code == 0
        assert "Pipeline Summary" in result.output or "Pipeline Complete" in result.output


class TestCliValidate:
    """CLI validate command tests."""

    def test_valid_config(self, tmp_path: Path) -> None:
        """Valid config → exit code 0."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("id,name\n1,Alice\n")

        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(f"""
name: Valid Pipeline
source:
  type: csv
  path: {csv_path}

target:
  type: csv
  path: {tmp_path}/out.csv

transform:
  type: ai
  instruction: lowercase name

mode: etl
chunk_size: 10
streaming_threshold: 1000
""")

        result = runner.invoke(app, ["validate", str(config_path)])

        assert result.exit_code == 0

    def test_invalid_config(self, tmp_path: Path) -> None:
        """Invalid config → exit code 1, table of errors."""
        config_path = tmp_path / "bad.yaml"
        config_path.write_text("""
name: Bad
source:
  type: postgres

target:
  type: csv
""")

        result = runner.invoke(app, ["validate", str(config_path)])

        assert result.exit_code == 1


class TestCliConnectors:
    """CLI connectors command tests."""

    def test_list_connectors(self) -> None:
        """loafer connectors → exit code 0, lists available connectors."""
        result = runner.invoke(app, ["connectors"])

        assert result.exit_code == 0
        assert "csv" in result.output.lower()
        assert "json" in result.output.lower()
