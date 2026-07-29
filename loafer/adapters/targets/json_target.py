"""JSON target connector — incremental JSON array writer.

Writes a valid JSON array incrementally without buffering the full
dataset.  Handles Decimal, UUID, and datetime serialization.
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, TextIO

from loafer.adapters.targets.atomic_file import (
    discard_temporary_file,
    open_temporary_text,
    publish_temporary_file,
    sync_and_close,
)
from loafer.exceptions import LoadError
from loafer.ports.connector import TargetConnector


class _SafeEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal, UUID, datetime, and date."""

    def default(self, o: object) -> Any:
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, uuid.UUID):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, date):
            return o.isoformat()
        return super().default(o)


class JsonTargetConnector(TargetConnector):
    """Write rows as a JSON array to a file, incrementally."""

    def __init__(self, path: str, write_mode: str = "overwrite") -> None:
        self._path = Path(path)
        self._write_mode = write_mode
        self._file: TextIO | None = None
        self._temporary_path: Path | None = None
        self._first_row = True
        self._rows_written = 0

    # -- lifecycle -----------------------------------------------------------

    def connect(self) -> None:
        if self._write_mode == "error" and self._path.exists():
            raise LoadError(f"Output file already exists: {self._path}")

        self._file, self._temporary_path = open_temporary_text(self._path)
        self._file.write("[\n")
        self._first_row = True
        self._rows_written = 0

    def disconnect(self) -> None:
        discard_temporary_file(self._file, self._temporary_path)
        self._file = None
        self._temporary_path = None

    # -- writing -------------------------------------------------------------

    def write_chunk(self, chunk: list[dict[str, Any]]) -> int:
        if self._file is None:
            raise LoadError("connect() must be called before write_chunk()")

        for row in chunk:
            if not self._first_row:
                self._file.write(",\n")
            self._file.write(json.dumps(row, cls=_SafeEncoder))
            self._first_row = False

        self._rows_written += len(chunk)
        return len(chunk)

    def finalize(self) -> None:
        if self._file is None or self._temporary_path is None:
            return

        self._file.write("\n]\n")
        sync_and_close(self._file)
        self._file = None
        publish_temporary_file(
            self._temporary_path,
            self._path,
            write_mode=self._write_mode,
        )
        self._temporary_path = None
