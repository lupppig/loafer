"""Tests for CsvSourceConnector."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from loafer.adapters.sources.csv_source import CsvSourceConnector
from loafer.exceptions import ExtractionError


class TestCsvSourceConnector:
    def test_stream_basic(self, tmp_path: Any) -> None:
        f = tmp_path / "data.csv"
        f.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n", encoding="utf-8")

        with CsvSourceConnector(str(f)) as conn:
            rows = conn.read_all()

        assert len(rows) == 3
        assert rows[0] == {"id": "1", "name": "Alice"}

    def test_stream_chunk_size(self, tmp_path: Any) -> None:
        lines = ["id,val"] + [f"{i},x" for i in range(10)]
        f = tmp_path / "data.csv"
        f.write_text("\n".join(lines), encoding="utf-8")

        with CsvSourceConnector(str(f)) as conn:
            chunks = list(conn.stream(chunk_size=3))

        assert len(chunks) == 4  # 3+3+3+1
        assert sum(len(c) for c in chunks) == 10

    def test_file_not_found(self, tmp_path: Any) -> None:
        with (
            pytest.raises(ExtractionError, match="not found"),
            CsvSourceConnector(str(tmp_path / "nope.csv")) as _,
        ):
            pass

    def test_empty_file(self, tmp_path: Any) -> None:
        f = tmp_path / "empty.csv"
        f.write_text("id,name\n", encoding="utf-8")

        with CsvSourceConnector(str(f)) as conn:
            rows = conn.read_all()

        assert rows == []

    def test_truly_empty_file(self, tmp_path: Any) -> None:
        f = tmp_path / "empty.csv"
        f.write_text("", encoding="utf-8")

        with CsvSourceConnector(str(f)) as conn:
            rows = conn.read_all()

        assert rows == []

    def test_malformed_rows_skipped(self, tmp_path: Any) -> None:
        f = tmp_path / "bad.csv"
        f.write_text("a,b\n1,2\n3\n4,5\n", encoding="utf-8")

        with CsvSourceConnector(str(f)) as conn:
            rows = conn.read_all()

        assert len(rows) == 2
        assert rows[0] == {"a": "1", "b": "2"}

    def test_latin1_fallback(self, tmp_path: Any) -> None:
        f = tmp_path / "latin.csv"
        f.write_bytes(b"name\nCaf\xe9\n")

        with CsvSourceConnector(str(f)) as conn:
            rows = conn.read_all()

        assert len(rows) == 1
        assert rows[0]["name"] == "Caf\xe9"

    def test_count_after_stream(self, tmp_path: Any) -> None:
        f = tmp_path / "data.csv"
        f.write_text("x\n1\n2\n3\n", encoding="utf-8")

        with CsvSourceConnector(str(f)) as conn:
            # connect() pre-counts rows
            assert conn.count() == 3
            rows = conn.read_all()
            assert len(rows) == 3
            assert conn.count() == 3

    def test_connect_scans_encoding_with_bounded_reads(self, tmp_path: Any) -> None:
        f = tmp_path / "data.csv"
        f.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
        real_open = open
        read_sizes: list[int] = []
        calls = 0

        class TrackingFile:
            def __init__(self, handle: Any) -> None:
                self._handle = handle

            def read(self, size: int = -1) -> str:
                read_sizes.append(size)
                return self._handle.read(size)

            def __iter__(self) -> Any:
                return iter(self._handle)

            def __getattr__(self, name: str) -> Any:
                return getattr(self._handle, name)

        def tracked_open(*args: Any, **kwargs: Any) -> Any:
            nonlocal calls
            calls += 1
            handle = real_open(*args, **kwargs)
            return TrackingFile(handle) if calls == 1 else handle

        with patch("builtins.open", side_effect=tracked_open):
            connector = CsvSourceConnector(str(f))
            connector.connect()
            connector.disconnect()

        assert read_sizes
        assert all(size > 0 for size in read_sizes)
