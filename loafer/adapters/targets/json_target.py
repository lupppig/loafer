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
from typing import Any

from loafer.connectors.base import TargetConnector
from loafer.exceptions import LoadError


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
        self._file: Any = None
        self._first_row = True
        self._rows_written = 0

    # -- lifecycle -----------------------------------------------------------

    def connect(self) -> None:
        if self._write_mode == "error" and self._path.exists():
            raise LoadError(f"Output file already exists: {self._path}")

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self._path, "w", encoding="utf-8")
        self._file.write("[\n")
        self._first_row = True
        self._rows_written = 0

    def disconnect(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()
            self._file = None

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
        if self._file and not self._file.closed:
            self._file.write("\n]\n")
            self._file.flush()
