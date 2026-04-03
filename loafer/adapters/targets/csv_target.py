"""CSV target connector.

Streams rows to a CSV file — writes the header on the first chunk only,
creates output directories as needed, and supports write_mode semantics.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import io

from loafer.connectors.base import TargetConnector
from loafer.exceptions import LoadError


class CsvTargetConnector(TargetConnector):
    """Write rows to a CSV file."""

    def __init__(self, path: str, write_mode: str = "overwrite") -> None:
        self._path = Path(path)
        self._write_mode = write_mode
        self._file: io.TextIOWrapper | None = None
        self._writer: csv.DictWriter[str] | None = None
        self._header_written = False
        self._rows_written = 0

    # -- lifecycle -----------------------------------------------------------

    def connect(self) -> None:
        if self._write_mode == "error" and self._path.exists():
            raise LoadError(f"Output file already exists: {self._path}")

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self._path, "w", newline="", encoding="utf-8")
        self._header_written = False
        self._rows_written = 0

    def disconnect(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()
            self._file = None
        self._writer = None

    # -- writing -------------------------------------------------------------

    def write_chunk(self, chunk: list[dict[str, Any]]) -> int:
        if self._file is None:
            raise LoadError("connect() must be called before write_chunk()")

        if not chunk:
            return 0

        if not self._header_written:
            fieldnames = list(chunk[0].keys())
            self._writer = csv.DictWriter(self._file, fieldnames=fieldnames)
            self._writer.writeheader()
            self._header_written = True

        if self._writer is None:  # pragma: no cover
            raise LoadError("Writer not initialised")

        for row in chunk:
            # None → empty string (standard CSV behaviour).
            clean = {k: ("" if v is None else v) for k, v in row.items()}
            self._writer.writerow(clean)

        self._rows_written += len(chunk)
        return len(chunk)

    def finalize(self) -> None:
        if self._file and not self._file.closed:
            self._file.flush()
