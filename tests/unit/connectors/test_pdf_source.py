"""Tests for PdfSourceConnector."""

from __future__ import annotations

from pathlib import Path

import pytest


class TestPdfSourceConnector:
    """Tests for the PDF source connector."""

    @pytest.fixture
    def pdf_path(self, tmp_path: Path) -> Path:
        """Create a simple PDF file using reportlab or a minimal PDF."""
        pdf = tmp_path / "test.pdf"
        # Write a minimal valid PDF with 3 pages
        pdf_content = b"""%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R 4 0 R 5 0 R] /Count 3 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 6 0 R /Resources << /Font << /F1 7 0 R >> >> >>
endobj
4 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 8 0 R /Resources << /Font << /F1 7 0 R >> >> >>
endobj
5 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 9 0 R /Resources << /Font << /F1 7 0 R >> >> >>
endobj
6 0 obj
<< /Length 44 >>
stream
BT /F1 12 Tf 72 720 Td (Page 1 text) Tj ET
endstream
endobj
7 0 obj
<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>
endobj
8 0 obj
<< /Length 44 >>
stream
BT /F1 12 Tf 72 720 Td (Page 2 text) Tj ET
endstream
endobj
9 0 obj
<< /Length 44 >>
stream
BT /F1 12 Tf 72 720 Td (Page 3 text) Tj ET
endstream
endobj
xref
0 10
0000000000 65535 f
0000000009 00000 n
0000000058 00000 n
0000000119 00000 n
0000000284 00000 n
0000000449 00000 n
0000000614 00000 n
0000000708 00000 n
0000000786 00000 n
0000000880 00000 n
trailer
<< /Size 10 /Root 1 0 R >>
startxref
974
%%EOF"""
        pdf.write_bytes(pdf_content)
        return pdf

    def test_connect_and_disconnect(self, pdf_path: Path) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        conn = PdfSourceConnector(str(pdf_path))
        conn.connect()
        assert conn._doc is not None
        conn.disconnect()
        assert conn._doc is None

    def test_count_returns_page_count(self, pdf_path: Path) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        conn = PdfSourceConnector(str(pdf_path))
        conn.connect()
        count = conn.count()
        conn.disconnect()

        assert count == 3

    def test_stream_yields_page_rows(self, pdf_path: Path) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        conn = PdfSourceConnector(str(pdf_path))
        conn.connect()
        chunks = list(conn.stream(chunk_size=2))
        conn.disconnect()

        all_rows = [row for chunk in chunks for row in chunk]
        assert len(all_rows) == 3
        assert all_rows[0]["page"] == 1
        assert all_rows[1]["page"] == 2
        assert all_rows[2]["page"] == 3

    def test_stream_includes_tables_when_enabled(self, pdf_path: Path) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        conn = PdfSourceConnector(str(pdf_path), extract_tables=True)
        conn.connect()
        chunks = list(conn.stream(chunk_size=10))
        conn.disconnect()

        all_rows = [row for chunk in chunks for row in chunk]
        assert "tables" in all_rows[0]
        assert "table_count" in all_rows[0]

    def test_stream_excludes_tables_when_disabled(self, pdf_path: Path) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        conn = PdfSourceConnector(str(pdf_path), extract_tables=False)
        conn.connect()
        chunks = list(conn.stream(chunk_size=10))
        conn.disconnect()

        all_rows = [row for chunk in chunks for row in chunk]
        assert "tables" not in all_rows[0]

    def test_stream_chunking(self, pdf_path: Path) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        conn = PdfSourceConnector(str(pdf_path))
        conn.connect()
        chunks = list(conn.stream(chunk_size=1))
        conn.disconnect()

        assert len(chunks) == 3  # 3 pages, 1 per chunk

    def test_read_all_convenience(self, pdf_path: Path) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        conn = PdfSourceConnector(str(pdf_path))
        conn.connect()
        rows = conn.read_all()
        conn.disconnect()

        assert len(rows) == 3

    def test_count_before_connect_returns_none(self) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        conn = PdfSourceConnector("/tmp/nonexistent.pdf")
        assert conn.count() is None

    def test_context_manager(self, pdf_path: Path) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        with PdfSourceConnector(str(pdf_path)) as conn:
            rows = conn.read_all()

        assert len(rows) == 3
        assert conn._doc is None

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        conn = PdfSourceConnector(str(tmp_path / "missing.pdf"))
        with pytest.raises(Exception, match="failed to open PDF"):
            conn.connect()
