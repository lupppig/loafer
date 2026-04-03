"""Tests for CsvTargetConnector."""

from __future__ import annotations

import csv
from typing import Any

import pytest

from loafer.connectors.targets.csv_target import CsvTargetConnector
from loafer.exceptions import LoadError


class TestCsvTargetConnector:
    def test_write_basic(self, tmp_path: Any) -> None:
        f = tmp_path / "out.csv"
        with CsvTargetConnector(str(f)) as conn:
            conn.write_chunk([{"a": 1, "b": 2}, {"a": 3, "b": 4}])

        rows = list(csv.DictReader(f.open()))
        assert len(rows) == 2
        assert rows[0] == {"a": "1", "b": "2"}

    def test_header_written_once(self, tmp_path: Any) -> None:
        f = tmp_path / "out.csv"
        with CsvTargetConnector(str(f)) as conn:
            conn.write_chunk([{"x": 1}])
            conn.write_chunk([{"x": 2}])

        text = f.read_text()
        assert text.count("x") == 1  # header only once

    def test_none_values_as_empty(self, tmp_path: Any) -> None:
        f = tmp_path / "out.csv"
        with CsvTargetConnector(str(f)) as conn:
            conn.write_chunk([{"a": None, "b": "hello"}])

        rows = list(csv.DictReader(f.open()))
        assert rows[0]["a"] == ""
        assert rows[0]["b"] == "hello"

    def test_creates_parent_dirs(self, tmp_path: Any) -> None:
        f = tmp_path / "sub" / "dir" / "out.csv"
        with CsvTargetConnector(str(f)) as conn:
            conn.write_chunk([{"x": 1}])

        assert f.exists()

    def test_write_mode_error(self, tmp_path: Any) -> None:
        f = tmp_path / "existing.csv"
        f.write_text("old data")

        with (
            pytest.raises(LoadError, match="already exists"),
            CsvTargetConnector(str(f), write_mode="error") as _,
        ):
            pass

    def test_write_mode_overwrite(self, tmp_path: Any) -> None:
        f = tmp_path / "existing.csv"
        f.write_text("old data")

        with CsvTargetConnector(str(f), write_mode="overwrite") as conn:
            conn.write_chunk([{"new": "data"}])

        text = f.read_text()
        assert "old data" not in text
        assert "new" in text

    def test_empty_chunk(self, tmp_path: Any) -> None:
        f = tmp_path / "out.csv"
        with CsvTargetConnector(str(f)) as conn:
            count = conn.write_chunk([])

        assert count == 0

    def test_values_with_commas(self, tmp_path: Any) -> None:
        f = tmp_path / "out.csv"
        with CsvTargetConnector(str(f)) as conn:
            conn.write_chunk([{"text": "hello, world"}])

        rows = list(csv.DictReader(f.open()))
        assert rows[0]["text"] == "hello, world"
