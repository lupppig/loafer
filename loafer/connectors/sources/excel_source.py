"""Excel source connector using openpyxl.

Reads computed values (not formulas), handles merged cells, validates
sheet names, and streams rows chunk-by-chunk.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from loafer.connectors.base import SourceConnector
from loafer.exceptions import ExtractionError

logger = logging.getLogger(__name__)


class ExcelSourceConnector(SourceConnector):
    """Read rows from an Excel (.xlsx) file."""

    def __init__(self, path: str, sheet: str | None = None) -> None:
        self._path = Path(path)
        self._sheet = sheet
        self._rows: list[dict[str, Any]] = []

    # -- lifecycle -----------------------------------------------------------

    def connect(self) -> None:
        try:
            import openpyxl
        except ImportError as exc:  # pragma: no cover
            raise ExtractionError("openpyxl is required for Excel files") from exc

        if not self._path.exists():
            raise ExtractionError(f"Excel file not found: {self._path}")

        try:
            wb = openpyxl.load_workbook(str(self._path), data_only=True)
        except Exception as exc:
            raise ExtractionError(f"Failed to open Excel file {self._path}: {exc}") from exc

        if self._sheet:
            if self._sheet not in wb.sheetnames:
                raise ExtractionError(
                    f"Sheet '{self._sheet}' not found. Available: {wb.sheetnames}"
                )
            ws = wb[self._sheet]
        else:
            ws = wb.active
            if ws is None:  # pragma: no cover
                raise ExtractionError("Workbook has no active sheet")

        # Unmerge merged cells — fill merged area with the top-left value.
        for merge_range in list(ws.merged_cells.ranges):
            min_row = merge_range.min_row
            min_col = merge_range.min_col
            top_left_value = ws.cell(row=min_row, column=min_col).value
            ws.unmerge_cells(str(merge_range))
            for row_idx in range(merge_range.min_row, merge_range.max_row + 1):
                for col_idx in range(merge_range.min_col, merge_range.max_col + 1):
                    ws.cell(row=row_idx, column=col_idx).value = top_left_value

        # Read headers from row 1, data from row 2 onwards.
        rows_iter = ws.iter_rows(values_only=True)
        try:
            headers = [str(h) if h is not None else f"column_{i}" for i, h in enumerate(next(rows_iter))]
        except StopIteration:
            logger.warning("Excel file is empty: %s", self._path)
            wb.close()
            return

        self._rows = []
        for row in rows_iter:
            row_dict: dict[str, Any] = {}
            for header, value in zip(headers, row, strict=False):
                row_dict[header] = value
            self._rows.append(row_dict)

        wb.close()

    def disconnect(self) -> None:
        self._rows = []

    # -- streaming -----------------------------------------------------------

    def stream(self, chunk_size: int) -> Any:  # Iterator[list[dict[str, Any]]]
        for i in range(0, len(self._rows), chunk_size):
            yield self._rows[i : i + chunk_size]

    def count(self) -> int | None:
        return len(self._rows)
