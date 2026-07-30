"""PDF source connector adapter."""

from __future__ import annotations

import contextlib
import signal
import threading
import time
from pathlib import Path
from typing import Any

from loafer.exceptions import ConnectorError
from loafer.ports.connector import SourceConnector


class PdfSourceConnector(SourceConnector):
    """Extract text and tables from a PDF file using pdfplumber."""

    def __init__(
        self,
        path: str,
        extract_tables: bool = True,
        max_pages: int | None = None,
        max_file_size_mb: int = 100,
        page_timeout_seconds: float = 30.0,
        total_timeout_seconds: float = 300.0,
        page_failure_policy: str = "fail",
    ) -> None:
        self._path = path
        self._extract_tables = extract_tables
        self._max_pages = max_pages
        self._max_file_size_mb = max_file_size_mb
        self._page_timeout_seconds = page_timeout_seconds
        self._total_timeout_seconds = total_timeout_seconds
        self._page_failure_policy = page_failure_policy
        self._doc: Any = None
        self._diagnostics: list[str] = []

    def connect(self) -> None:
        path = Path(self._path)
        try:
            size_bytes = path.stat().st_size
        except OSError as exc:
            raise ConnectorError(f"failed to open PDF: {exc}") from exc
        limit_bytes = self._max_file_size_mb * 1024 * 1024
        if size_bytes > limit_bytes:
            raise ConnectorError(
                f"PDF file is {size_bytes} bytes, exceeding the configured "
                f"{self._max_file_size_mb}MB limit"
            )

        try:
            import pdfplumber
        except ImportError:
            raise ConnectorError("PDF connector requires 'pdfplumber'")

        try:
            self._doc = pdfplumber.open(self._path)
        except Exception as exc:
            raise ConnectorError(f"failed to open PDF: {exc}") from exc
        self._diagnostics = []
        page_count = len(self._doc.pages)
        if self._max_pages is not None and page_count > self._max_pages:
            self.disconnect()
            raise ConnectorError(
                f"PDF has {page_count} pages, exceeding the configured {self._max_pages}-page limit"
            )

    def disconnect(self) -> None:
        if self._doc:
            self._doc.close()
            self._doc = None

    def stream(self, chunk_size: int) -> Any:
        if self._doc is None:
            raise ConnectorError("not connected")

        chunk: list[dict[str, Any]] = []
        document_started = time.monotonic()
        for page_num, page in enumerate(self._doc.pages, start=1):
            elapsed = time.monotonic() - document_started
            if elapsed >= self._total_timeout_seconds:
                raise ConnectorError(
                    f"PDF extraction exceeded the configured "
                    f"{self._total_timeout_seconds:g}s document timeout before page {page_num}"
                )

            page_budget = min(
                self._page_timeout_seconds,
                self._total_timeout_seconds - elapsed,
            )
            try:
                with _time_limit(page_budget):
                    row = self._extract_page(page_num, page)
            except Exception as exc:
                message = f"PDF page {page_num} extraction failed: {exc}"
                if self._page_failure_policy == "fail":
                    raise ConnectorError(message) from exc
                self._diagnostics.append(message)
                continue
            chunk.append(row)

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        if chunk:
            yield chunk

    def count(self) -> int | None:
        if self._doc is None:
            return None
        return len(self._doc.pages)

    def diagnostics(self) -> list[str]:
        return list(self._diagnostics)

    def _extract_page(self, page_num: int, page: Any) -> dict[str, Any]:
        text = page.extract_text() or ""
        source_path = str(Path(self._path).resolve())
        row: dict[str, Any] = {
            "page": page_num,
            "page_number": page_num,
            "text": text,
            "provenance": {
                "source_path": source_path,
                "page_number": page_num,
                "content_type": "native_pdf",
                "ocr_applied": False,
            },
        }

        if self._extract_tables:
            tables = page.extract_tables() or []
            row["tables"] = tables
            row["table_count"] = len(tables)
            row["table_provenance"] = [
                {
                    "source_path": source_path,
                    "page_number": page_num,
                    "table_index": index,
                    "row_count": len(table),
                    "column_count": max((len(table_row) for table_row in table), default=0),
                }
                for index, table in enumerate(tables)
            ]
        return row


@contextlib.contextmanager
def _time_limit(seconds: float) -> Any:
    """Enforce a wall-clock limit where SIGALRM is available."""
    if not hasattr(signal, "SIGALRM") or threading.current_thread() is not threading.main_thread():
        started = time.monotonic()
        yield
        if time.monotonic() - started > seconds:
            raise TimeoutError(f"page exceeded {seconds:g}s timeout")
        return

    def _raise_timeout(_signum: int, _frame: Any) -> None:
        raise TimeoutError(f"page exceeded {seconds:g}s timeout")

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _raise_timeout)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, *previous_timer)
        signal.signal(signal.SIGALRM, previous_handler)
