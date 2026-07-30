"""Configuration contracts for bounded and global execution semantics."""

from __future__ import annotations

from pathlib import Path

import pytest

from loafer.application.service import _delivery_guarantee
from loafer.config import PipelineConfig
from loafer.exceptions import ConfigError


def _custom_path(tmp_path: Path) -> Path:
    path = tmp_path / "transform.py"
    path.write_text("def transform(data):\n    return data\n", encoding="utf-8")
    return path


def test_existing_pipelines_keep_materialized_execution_default(tmp_path: Path) -> None:
    config = PipelineConfig(
        source={"type": "rest_api", "url": "https://example.test/rows"},
        target={"type": "json", "path": str(tmp_path / "out.json")},
        transform={"type": "custom", "path": str(_custom_path(tmp_path))},
    )

    assert config.execution.transform_class == "materialized"
    assert config.execution.schema_drift == "fail"


def test_sql_is_classified_as_global_relational_by_default(tmp_path: Path) -> None:
    config = PipelineConfig(
        source={"type": "rest_api", "url": "https://example.test/rows"},
        target={"type": "json", "path": str(tmp_path / "out.json")},
        transform={"type": "sql", "query": "SELECT * FROM {{source}}"},
    )

    assert config.execution.transform_class == "global_relational"


def test_row_local_sql_is_rejected_with_global_semantics_guidance(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="global semantics"):
        PipelineConfig(
            source={"type": "rest_api", "url": "https://example.test/rows"},
            target={"type": "json", "path": str(tmp_path / "out.json")},
            transform={"type": "sql", "query": "SELECT * FROM {{source}}"},
            execution={"transform_class": "row_local"},
        )


def test_row_local_mongo_target_is_rejected_until_atomic_protocol_exists() -> None:
    with pytest.raises(ValueError, match="PostgreSQL staging target"):
        PipelineConfig(
            source={"type": "rest_api", "url": "https://example.test/rows"},
            target={
                "type": "mongo",
                "url": "mongodb://localhost/db",
                "database": "test",
                "collection": "output",
            },
            transform={"type": "ai", "instruction": "copy rows"},
            execution={"transform_class": "row_local"},
        )


def test_quarantine_policy_requires_a_path(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="quarantine_path"):
        PipelineConfig(
            source={"type": "rest_api", "url": "https://example.test/rows"},
            target={"type": "json", "path": str(tmp_path / "out.json")},
            transform={"type": "custom", "path": str(_custom_path(tmp_path))},
            execution={
                "transform_class": "row_local",
                "schema_drift": "quarantine",
            },
        )


def test_csv_cannot_evolve_schema_after_header_publication(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="header is fixed"):
        PipelineConfig(
            source={"type": "rest_api", "url": "https://example.test/rows"},
            target={"type": "csv", "path": str(tmp_path / "out.csv")},
            transform={"type": "custom", "path": str(_custom_path(tmp_path))},
            execution={
                "transform_class": "row_local",
                "schema_drift": "evolve",
            },
        )


def test_postgres_staging_cannot_evolve_schema_after_creation(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="staging table schema is fixed"):
        PipelineConfig(
            source={"type": "rest_api", "url": "https://example.test/rows"},
            target={
                "type": "postgres",
                "url": "postgresql://localhost/db",
                "table": "public.output",
            },
            transform={"type": "custom", "path": str(_custom_path(tmp_path))},
            execution={
                "transform_class": "row_local",
                "schema_drift": "evolve",
            },
        )


@pytest.mark.parametrize(
    ("write_mode", "key", "expected"),
    [
        ("replace", None, "atomic_transactional_replace"),
        ("error", None, "atomic_transactional_create_once"),
        ("append", None, "at_least_once_atomic_merge"),
        ("upsert", "id", "idempotent_keyed_atomic_merge"),
    ],
)
def test_postgres_delivery_guarantee_is_explicit(
    tmp_path: Path,
    write_mode: str,
    key: str | None,
    expected: str,
) -> None:
    config = PipelineConfig(
        source={"type": "rest_api", "url": "https://example.test/rows"},
        target={
            "type": "postgres",
            "url": "postgresql://localhost/db",
            "table": "public.output",
            "write_mode": write_mode,
            "key": key,
        },
        transform={"type": "custom", "path": str(_custom_path(tmp_path))},
        execution={"transform_class": "row_local"},
    )

    assert _delivery_guarantee(config) == expected


def test_validation_null_rate_must_be_a_fraction(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="between 0 and 1"):
        PipelineConfig(
            source={"type": "rest_api", "url": "https://example.test/rows"},
            target={"type": "json", "path": str(tmp_path / "out.json")},
            transform={"type": "custom", "path": str(_custom_path(tmp_path))},
            validation={"max_null_rate": 1.5},
        )


def test_load_config_wraps_bounded_contract_errors(tmp_path: Path) -> None:
    from loafer.config import load_config

    config = tmp_path / "pipeline.yaml"
    config.write_text(
        "\n".join(
            [
                "source:",
                "  type: rest_api",
                "  url: https://example.test/rows",
                "target:",
                "  type: json",
                f"  path: {tmp_path / 'out.json'}",
                "transform:",
                "  type: sql",
                "  query: SELECT * FROM {{source}}",
                "execution:",
                "  transform_class: row_local",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="global semantics"):
        load_config(config)
