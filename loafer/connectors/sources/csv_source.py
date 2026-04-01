"""CSV source connector — streaming-first file reader.

Handles encoding fallback, malformed rows, header detection, and
streams data using Python's ``csv`` module without buffering.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import io

from loafer.connectors.base import SourceConnector
from loafer.exceptions import ConfigError, ExtractionError

logger = logging.getLogger(__name__)


class CsvSourceConnector(SourceConnector):
    """Read rows from a CSV file."""

    def __init__(
        self,
        path: str,
        has_header: bool = True,
        encoding: str = "utf-8",
        column_names: list[str] | None = None,
    ) -> None:
        self._path = Path(path)
        self._has_header = has_header
        self._encoding = encoding
        self._column_names = column_names
        self._file: io.TextIOWrapper | None = None
        self._row_count: int | None = None
        self._actual_encoding: str = encoding

    # -- lifecycle -----------------------------------------------------------

    def connect(self) -> None:
        if not self._path.exists():
            raise ExtractionError(f"CSV file not found: {self._path}")

        if not self._has_header and not self._column_names:
            raise ConfigError(
                "CSV file has no header — set has_header: false and provide column_names"
            )

        try:
            self._file = open(self._path, encoding=self._encoding, newline="")
            self._file.read()
            self._file.seek(0)
        except UnicodeDecodeError:
            logger.warning("UTF-8 decode failed for %s, falling back to latin-1", self._path)
            self._file = open(self._path, encoding="latin-1", newline="")
            self._actual_encoding = "latin-1"

        self._row_count = self._count_rows()

    def _count_rows(self) -> int | None:
        total = 0
        with open(self._path, encoding=self._actual_encoding, newline="") as f:
            reader = csv.reader(f)
            if self._has_header:
                try:
                    next(reader)
                except StopIteration:
                    return 0
            for _row in reader:
                total += 1
        return total if total > 0 else None

    def disconnect(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()
            self._file = None

    # -- streaming -----------------------------------------------------------

    def stream(self, chunk_size: int) -> Any:  # Iterator[list[dict[str, Any]]]
        if self._file is None:
            raise ExtractionError("connect() must be called before stream()")

        self._file.seek(0)
        reader = csv.reader(self._file)

        # Resolve headers.
        if self._has_header:
            try:
                headers = next(reader)
            except StopIteration:
                # Empty file (header only or truly empty).
                logger.warning("CSV file is empty: %s", self._path)
                return
        else:
            headers = list(self._column_names or [])

        expected_cols = len(headers)
        chunk: list[dict[str, Any]] = []
        total_rows = 0
        skipped = 0

        for row in reader:
            if len(row) != expected_cols:
                skipped += 1
                continue
            chunk.append(dict(zip(headers, row, strict=True)))
            total_rows += 1
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        if chunk:
            yield chunk

        if skipped:
            logger.warning(
                "Skipped %d malformed rows in %s (expected %d columns)",
                skipped,
                self._path,
                expected_cols,
            )

        self._row_count = total_rows

    def count(self) -> int | None:
        return self._row_count
