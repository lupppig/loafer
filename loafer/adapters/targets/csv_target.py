"""CSV target connector.

Streams rows to a CSV file — writes the header on the first chunk only,
creates output directories as needed, and supports write_mode semantics.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing import TextIO

from loafer.adapters.targets.atomic_file import (
    discard_temporary_file,
    open_temporary_text,
    publish_temporary_file,
    sync_and_close,
)
from loafer.exceptions import LoadError
from loafer.ports.connector import TargetConnector


class CsvTargetConnector(TargetConnector):
    """Write rows to a CSV file."""

    def __init__(self, path: str, write_mode: str = "overwrite") -> None:
        self._path = Path(path)
        self._write_mode = write_mode
        self._file: TextIO | None = None
        self._temporary_path: Path | None = None
        self._writer: csv.DictWriter[str] | None = None
        self._header_written = False
        self._rows_written = 0

    # -- lifecycle -----------------------------------------------------------

    def connect(self) -> None:
        if self._write_mode == "error" and self._path.exists():
            raise LoadError(f"Output file already exists: {self._path}")

        self._file, self._temporary_path = open_temporary_text(self._path, newline="")
        self._header_written = False
        self._rows_written = 0

    def disconnect(self) -> None:
        discard_temporary_file(self._file, self._temporary_path)
        self._file = None
        self._temporary_path = None
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
        if self._file is None or self._temporary_path is None:
            return

        sync_and_close(self._file)
        self._file = None
        publish_temporary_file(
            self._temporary_path,
            self._path,
            write_mode=self._write_mode,
        )
        self._temporary_path = None
