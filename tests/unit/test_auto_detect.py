"""Tests for auto-detection of source, target, and transform types."""

from __future__ import annotations

import pytest

from loafer.config import (
    PipelineConfig,
    _infer_source_type,
    _infer_target_type,
    _infer_transform_type,
)
from loafer.exceptions import ConfigError

# ---------------------------------------------------------------------------
# _infer_source_type
# ---------------------------------------------------------------------------


class TestInferSourceType:
    """URL scheme and file extension detection for sources."""

    @pytest.mark.parametrize(
        "url,expected",
        [
            ("postgresql://user:pass@localhost/db", "postgres"),
            ("postgres://user:pass@localhost/db", "postgres"),
            ("POSTGRESQL://USER@HOST/DB", "postgres"),
            ("mysql://user:pass@localhost/db", "mysql"),
            ("mysql+pymysql://user:pass@localhost/db", "mysql"),
            ("mongodb://localhost:27017", "mongo"),
            ("mongodb+srv://cluster.example.net", "mongo"),
            ("http://api.example.com/data", "rest_api"),
            ("https://api.example.com/v2/users", "rest_api"),
        ],
    )
    def test_url_schemes(self, url: str, expected: str) -> None:
        assert _infer_source_type({"url": url}) == expected

    @pytest.mark.parametrize(
        "path,expected",
        [
            ("data/sales.csv", "csv"),
            ("/tmp/REPORT.CSV", "csv"),
            ("data/report.xlsx", "excel"),
            ("data/report.xls", "excel"),
            ("docs/invoice.pdf", "pdf"),
            ("data/app.db", "sqlite"),
            ("data/app.sqlite", "sqlite"),
            ("data/app.sqlite3", "sqlite"),
        ],
    )
    def test_file_extensions(self, path: str, expected: str) -> None:
        assert _infer_source_type({"path": path}) == expected

    def test_unknown_extension_returns_none(self) -> None:
        assert _infer_source_type({"path": "data/output"}) is None

    def test_unknown_scheme_returns_none(self) -> None:
        assert _infer_source_type({"url": "ftp://server/file"}) is None

    def test_empty_dict_returns_none(self) -> None:
        assert _infer_source_type({}) is None

    def test_url_takes_precedence_over_path(self) -> None:
        result = _infer_source_type(
            {
                "url": "postgresql://localhost/db",
                "path": "data.csv",
            }
        )
        assert result == "postgres"


# ---------------------------------------------------------------------------
# _infer_target_type
# ---------------------------------------------------------------------------


class TestInferTargetType:
    """URL scheme and file extension detection for targets."""

    @pytest.mark.parametrize(
        "url,expected",
        [
            ("postgresql://user:pass@localhost/db", "postgres"),
            ("postgres://user:pass@localhost/db", "postgres"),
            ("mongodb://localhost:27017", "mongo"),
            ("mongodb+srv://cluster.example.net", "mongo"),
        ],
    )
    def test_url_schemes(self, url: str, expected: str) -> None:
        assert _infer_target_type({"url": url}) == expected

    @pytest.mark.parametrize(
        "path,expected",
        [
            ("output/results.csv", "csv"),
            ("output/results.json", "json"),
            ("output/results.jsonl", "json"),
        ],
    )
    def test_file_extensions(self, path: str, expected: str) -> None:
        assert _infer_target_type({"path": path}) == expected

    def test_unknown_extension_returns_none(self) -> None:
        assert _infer_target_type({"path": "output/data"}) is None

    def test_empty_dict_returns_none(self) -> None:
        assert _infer_target_type({}) is None


# ---------------------------------------------------------------------------
# _infer_transform_type
# ---------------------------------------------------------------------------


class TestInferTransformType:
    """Field-based detection for transforms."""

    def test_instruction_means_ai(self) -> None:
        assert _infer_transform_type({"instruction": "summarize"}) == "ai"

    def test_path_means_custom(self) -> None:
        assert _infer_transform_type({"path": "transform.py"}) == "custom"

    def test_query_means_sql(self) -> None:
        assert _infer_transform_type({"query": "SELECT *"}) == "sql"

    def test_instruction_takes_precedence(self) -> None:
        """If both instruction and path are present, it's AI (with custom_path)."""
        assert _infer_transform_type({"instruction": "clean", "path": "t.py"}) == "ai"

    def test_empty_dict_returns_none(self) -> None:
        assert _infer_transform_type({}) is None


# ---------------------------------------------------------------------------
# PipelineConfig integration — auto-detection via model_validator
# ---------------------------------------------------------------------------


class TestPipelineConfigAutoDetect:
    """End-to-end tests: type is injected before Pydantic validation."""

    def test_source_auto_detected_csv(self, tmp_path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        json_file = tmp_path / "out.json"

        cfg = PipelineConfig(
            source={"path": str(csv_file)},
            target={"type": "json", "path": str(json_file)},
            transform={"type": "ai", "instruction": "passthrough"},
        )
        assert cfg.source.type == "csv"

    def test_target_auto_detected_json(self, tmp_path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        json_file = tmp_path / "out.json"

        cfg = PipelineConfig(
            source={"type": "csv", "path": str(csv_file)},
            target={"path": str(json_file)},
            transform={"type": "ai", "instruction": "passthrough"},
        )
        assert cfg.target.type == "json"

    def test_transform_auto_detected_ai(self, tmp_path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        json_file = tmp_path / "out.json"

        cfg = PipelineConfig(
            source={"type": "csv", "path": str(csv_file)},
            target={"type": "json", "path": str(json_file)},
            transform={"instruction": "clean the data"},
        )
        assert cfg.transform.type == "ai"

    def test_all_three_auto_detected(self, tmp_path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        json_file = tmp_path / "out.json"

        cfg = PipelineConfig(
            source={"path": str(csv_file)},
            target={"path": str(json_file)},
            transform={"instruction": "clean the data"},
        )
        assert cfg.source.type == "csv"
        assert cfg.target.type == "json"
        assert cfg.transform.type == "ai"

    def test_explicit_type_not_overridden(self, tmp_path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        json_file = tmp_path / "out.json"

        cfg = PipelineConfig(
            source={"type": "csv", "path": str(csv_file)},
            target={"type": "json", "path": str(json_file)},
            transform={"type": "ai", "instruction": "passthrough"},
        )
        assert cfg.source.type == "csv"

    def test_unrecognized_source_raises(self) -> None:
        with pytest.raises(ConfigError, match="cannot auto-detect source type"):
            PipelineConfig(
                source={"path": "data/unknown_file"},
                target={"type": "json", "path": "out.json"},
                transform={"type": "ai", "instruction": "x"},
            )

    def test_unrecognized_target_raises(self, tmp_path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")

        with pytest.raises(ConfigError, match="cannot auto-detect target type"):
            PipelineConfig(
                source={"type": "csv", "path": str(csv_file)},
                target={"path": "output/noext"},
                transform={"type": "ai", "instruction": "x"},
            )

    def test_unrecognized_transform_raises(self, tmp_path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")

        with pytest.raises(ConfigError, match="cannot auto-detect transform type"):
            PipelineConfig(
                source={"type": "csv", "path": str(csv_file)},
                target={"type": "json", "path": "out.json"},
                transform={"unknown_field": "value"},
            )

    def test_plain_string_transform_still_works(self, tmp_path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        json_file = tmp_path / "out.json"

        cfg = PipelineConfig(
            source={"path": str(csv_file)},
            target={"path": str(json_file)},
            transform="clean the data",
        )
        assert cfg.transform.type == "ai"
        assert cfg.transform.instruction == "clean the data"
