"""Tests for bounded-batch schema, validation, and checksum policies."""

from __future__ import annotations

import pytest

from loafer.config import ValidationConfig
from loafer.core.batches import (
    RollingRowsDigest,
    SchemaTracker,
    validate_batch,
)
from loafer.exceptions import ValidationError


def test_rolling_checksum_is_independent_of_batch_boundaries() -> None:
    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]
    whole = RollingRowsDigest()
    split = RollingRowsDigest()

    whole.update(rows)
    split.update(rows[:1])
    split.update(rows[1:])

    assert split.rows == whole.rows == 3
    assert split.bytes == whole.bytes
    assert split.checksum == whole.checksum


def test_schema_fail_policy_rejects_new_columns() -> None:
    tracker = SchemaTracker()
    tracker.apply([{"id": 1, "name": "a"}], "fail")

    with pytest.raises(ValidationError, match="new columns: extra"):
        tracker.apply([{"id": 2, "name": "b", "extra": True}], "fail")


def test_schema_quarantine_rejects_only_drifted_rows() -> None:
    tracker = SchemaTracker()
    tracker.apply([{"id": 1}], "quarantine")

    result = tracker.apply([{"id": 2}, {"id": "wrong"}], "quarantine")

    assert result.rows == [{"id": 2}]
    assert len(result.rejected) == 1
    assert "changed from integer to string" in result.rejected[0].reason


def test_schema_coerce_normalizes_rows_to_baseline() -> None:
    tracker = SchemaTracker()
    first = tracker.apply([{"id": 1, "active": True}], "coerce")
    second = tracker.apply(
        [{"id": "2", "active": "false", "ignored": "drop"}],
        "coerce",
    )

    assert first.version == second.version
    assert second.rows == [{"active": False, "id": 2}]
    assert second.rejected == []


def test_schema_evolve_changes_content_addressed_version() -> None:
    tracker = SchemaTracker()
    first = tracker.apply([{"id": 1}], "evolve")
    second = tracker.apply([{"id": 2, "name": "new"}], "evolve")

    assert second.evolved is True
    assert second.schema == {"id": "integer", "name": "string"}
    assert second.version != first.version


def test_validation_fail_reports_first_invalid_row() -> None:
    config = ValidationConfig(
        required_columns=["id"],
        column_types={"id": "integer"},
        on_failure="fail",
    )

    with pytest.raises(ValidationError, match="row 1"):
        validate_batch([{"id": 1}, {"id": "bad"}], config)


def test_validation_quarantine_keeps_valid_rows_and_counts_nulls() -> None:
    config = ValidationConfig(
        max_null_rate=0.25,
        strict=True,
        required_columns=["id"],
        column_types={"id": "integer"},
        on_failure="quarantine",
    )

    result = validate_batch(
        [{"id": 1, "name": "a"}, {"id": None, "name": "b"}],
        config,
    )

    assert result.rows == [{"id": 1, "name": "a"}]
    assert len(result.rejected) == 1
    assert result.column_counts["id"] == {"total_count": 2, "null_count": 1}
