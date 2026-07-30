"""Tests for PdfSourceConnector."""

from __future__ import annotations

import time
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

    @pytest.fixture
    def table_pdf_path(self, tmp_path: Path) -> Path:
        """Create a native-text PDF containing a ruled two-column table."""
        content = b"""0.5 w
72 720 m 300 720 l S
72 690 m 300 690 l S
72 660 m 300 660 l S
72 660 m 72 720 l S
180 660 m 180 720 l S
300 660 m 300 720 l S
BT /F1 12 Tf 82 702 Td (Name) Tj ET
BT /F1 12 Tf 190 702 Td (Value) Tj ET
BT /F1 12 Tf 82 672 Td (Alice) Tj ET
BT /F1 12 Tf 190 672 Td (42) Tj ET
"""
        objects = [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            (
                b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>"
            ),
            b"<< /Length " + str(len(content)).encode() + b" >>\nstream\n" + content + b"endstream",
            b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        ]
        payload = bytearray(b"%PDF-1.4\n")
        offsets = [0]
        for number, obj in enumerate(objects, start=1):
            offsets.append(len(payload))
            payload.extend(f"{number} 0 obj\n".encode())
            payload.extend(obj)
            payload.extend(b"\nendobj\n")
        xref_offset = len(payload)
        payload.extend(f"xref\n0 {len(objects) + 1}\n".encode())
        payload.extend(b"0000000000 65535 f \n")
        for offset in offsets[1:]:
            payload.extend(f"{offset:010d} 00000 n \n".encode())
        payload.extend(
            (
                f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
                f"startxref\n{xref_offset}\n%%EOF\n"
            ).encode()
        )
        path = tmp_path / "native-table.pdf"
        path.write_bytes(payload)
        return path

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
        assert "table_provenance" in all_rows[0]
        assert all_rows[0]["provenance"] == {
            "source_path": str(pdf_path.resolve()),
            "page_number": 1,
            "content_type": "native_pdf",
            "ocr_applied": False,
        }

    def test_stream_excludes_tables_when_disabled(self, pdf_path: Path) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        conn = PdfSourceConnector(str(pdf_path), extract_tables=False)
        conn.connect()
        chunks = list(conn.stream(chunk_size=10))
        conn.disconnect()

        all_rows = [row for chunk in chunks for row in chunk]
        assert "tables" not in all_rows[0]

    def test_native_table_fixture_preserves_page_and_table_provenance(
        self,
        table_pdf_path: Path,
    ) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        with PdfSourceConnector(str(table_pdf_path), extract_tables=True) as conn:
            rows = conn.read_all()

        assert rows[0]["tables"] == [[["Name", "Value"], ["Alice", "42"]]]
        assert rows[0]["table_provenance"] == [
            {
                "source_path": str(table_pdf_path.resolve()),
                "page_number": 1,
                "table_index": 0,
                "row_count": 2,
                "column_count": 2,
            }
        ]

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

    def test_page_limit_is_enforced_before_extraction(self, pdf_path: Path) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        conn = PdfSourceConnector(str(pdf_path), max_pages=2)

        with pytest.raises(Exception, match=r"3 pages.*2-page limit"):
            conn.connect()
        assert conn._doc is None

    def test_file_size_limit_is_enforced_before_parser_open(self, tmp_path: Path) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        large = tmp_path / "large.pdf"
        large.write_bytes(b"x" * (1024 * 1024 + 1))
        conn = PdfSourceConnector(str(large), max_file_size_mb=1)

        with pytest.raises(Exception, match=r"exceeding.*1MB limit"):
            conn.connect()

    def test_skip_policy_reports_page_failure(self) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        class _BrokenPage:
            def extract_text(self) -> str:
                raise RuntimeError("broken content stream")

        class _Document:
            def __init__(self) -> None:
                self.pages = [_BrokenPage()]

        conn = PdfSourceConnector("/tmp/fake.pdf", page_failure_policy="skip")
        conn._doc = _Document()

        assert list(conn.stream(chunk_size=1)) == []
        assert conn.diagnostics() == ["PDF page 1 extraction failed: broken content stream"]

    def test_page_timeout_is_enforced_and_reported(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from loafer.connectors.registry import PdfSourceConnector

        class _Page:
            pass

        class _Document:
            def __init__(self) -> None:
                self.pages = [_Page()]

        conn = PdfSourceConnector(
            "/tmp/fake.pdf",
            page_timeout_seconds=0.01,
            page_failure_policy="skip",
        )
        conn._doc = _Document()

        def _slow_page(_page_num: int, _page: object) -> dict[str, object]:
            time.sleep(0.1)
            return {}

        monkeypatch.setattr(conn, "_extract_page", _slow_page)

        assert list(conn.stream(chunk_size=1)) == []
        assert "page exceeded 0.01s timeout" in conn.diagnostics()[0]
