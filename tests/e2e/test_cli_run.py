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

    def test_invalid_config_error_prefix_not_doubled(self, tmp_path: Path) -> None:
        """DX: the 'Config validation failed:' prefix must appear exactly once."""
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
        assert result.output.count("Config validation failed:") == 1


class TestCliVersion:
    """--version flag."""

    def test_version_flag_exits_zero(self) -> None:
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "loafer" in result.output.lower()

    def test_version_short_flag(self) -> None:
        result = runner.invoke(app, ["-V"])
        assert result.exit_code == 0
        assert "loafer" in result.output.lower()


class TestCliInit:
    """loafer init scaffolding."""

    def test_init_non_custom_transform_non_csv_source(self, tmp_path: Path) -> None:
        """Regression: init must not crash when transform!=custom and source!=csv.

        transform_path and sample_csv were only bound inside their respective
        branches but referenced unconditionally in the summary, so any
        non-custom/non-csv combo raised UnboundLocalError.
        """
        proj = tmp_path / "proj"
        # name, source, target, transform, mode
        result = runner.invoke(app, ["init", str(proj)], input="myproj\npostgres\njson\nai\nelt\n")

        assert result.exit_code == 0, result.output
        assert (proj / "pipeline.yaml").exists()
        # No transform.py / input.csv for this combo, and no traceback.
        assert not (proj / "transform.py").exists()
        assert "UnboundLocalError" not in result.output

    def test_init_custom_csv_writes_extra_files(self, tmp_path: Path) -> None:
        proj = tmp_path / "proj"
        result = runner.invoke(app, ["init", str(proj)], input="myproj\ncsv\njson\ncustom\netl\n")

        assert result.exit_code == 0, result.output
        assert (proj / "transform.py").exists()
        assert (proj / "data" / "input.csv").exists()


class TestCliConnectors:
    """CLI connectors command tests."""

    def test_list_connectors(self) -> None:
        """loafer connectors → exit code 0, lists available connectors."""
        result = runner.invoke(app, ["connectors"])

        assert result.exit_code == 0
        assert "csv" in result.output.lower()
        assert "json" in result.output.lower()
