"""PDF source connector adapter."""

from __future__ import annotations

from typing import Any

from loafer.ports.connector import SourceConnector


class PdfSourceConnector(SourceConnector):
    """Extract text and tables from a PDF file using pdfplumber."""

    def __init__(
        self,
        path: str,
        extract_tables: bool = True,
    ) -> None:
        self._path = path
        self._extract_tables = extract_tables
        self._doc: Any = None

    def connect(self) -> None:
        try:
            import pdfplumber
        except ImportError:
            from loafer.exceptions import ConnectorError

            raise ConnectorError("PDF connector requires 'pdfplumber'")

        try:
            self._doc = pdfplumber.open(self._path)
        except Exception as exc:
            from loafer.exceptions import ConnectorError

            raise ConnectorError(f"failed to open PDF: {exc}") from exc

    def disconnect(self) -> None:
        if self._doc:
            self._doc.close()
            self._doc = None

    def stream(self, chunk_size: int) -> Any:
        if self._doc is None:
            from loafer.exceptions import ConnectorError

            raise ConnectorError("not connected")

        chunk: list[dict[str, Any]] = []
        for page_num, page in enumerate(self._doc.pages, start=1):
            text = page.extract_text() or ""
            row: dict[str, Any] = {
                "page": page_num,
                "text": text,
            }

            if self._extract_tables:
                tables = page.extract_tables()
                row["tables"] = tables if tables else []
                row["table_count"] = len(tables) if tables else 0

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
