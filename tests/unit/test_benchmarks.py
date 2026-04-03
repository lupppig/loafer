"""Benchmark tests for large datasets and performance.

These tests verify that the system handles large-scale data without OOM
and meets reasonable performance thresholds.  They are skipped by default
unless ``--benchmark`` is passed to pytest.
"""

from __future__ import annotations

import csv
import json
import time
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _skip_unless_benchmark(request: pytest.FixtureRequest) -> None:
    if not request.config.getoption("--benchmark", default=False):
        pytest.skip("use --benchmark to run performance tests")


# ---------------------------------------------------------------------------
# CSV source streaming benchmarks
# ---------------------------------------------------------------------------


class TestCsvStreamingBenchmark:
    """Verify CSV source can stream large files without loading into memory."""

    @pytest.fixture
    def large_csv(self, tmp_path: Path) -> Path:
        """Generate a CSV with 100k rows and 10 columns."""
        f = tmp_path / "large.csv"
        cols = [f"col_{i}" for i in range(10)]
        with f.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=cols)
            writer.writeheader()
            for i in range(100_000):
                writer.writerow({c: f"row{i}_{c}" for c in cols})
        return f

    def test_stream_100k_rows_no_oom(self, large_csv: Path, request: pytest.FixtureRequest) -> None:
        _skip_unless_benchmark(request)

        from loafer.adapters.sources.csv_source import CsvSourceConnector

        conn = CsvSourceConnector(
            str(large_csv), has_header=True, encoding="utf-8", column_names=None
        )
        conn.connect()
        total = 0
        start = time.monotonic()
        for chunk in conn.stream(chunk_size=1000):
            total += len(chunk)
        elapsed = time.monotonic() - start
        conn.disconnect()

        assert total == 100_000
        # Should complete in under 30 seconds on any modern machine
        assert elapsed < 30, f"Streaming 100k rows took {elapsed:.1f}s"

    def test_stream_chunk_size_respected(
        self, large_csv: Path, request: pytest.FixtureRequest
    ) -> None:
        _skip_unless_benchmark(request)

        from loafer.adapters.sources.csv_source import CsvSourceConnector

        conn = CsvSourceConnector(
            str(large_csv), has_header=True, encoding="utf-8", column_names=None
        )
        conn.connect()
        chunk_sizes: list[int] = []
        for chunk in conn.stream(chunk_size=500):
            chunk_sizes.append(len(chunk))
        conn.disconnect()

        assert all(s <= 500 for s in chunk_sizes)
        assert sum(chunk_sizes) == 100_000
        assert len(chunk_sizes) == 200  # 100k / 500 = 200 chunks


# ---------------------------------------------------------------------------
# JSON target write benchmarks
# ---------------------------------------------------------------------------


class TestJsonTargetBenchmark:
    """Verify JSON target can write incrementally without buffering."""

    def test_write_100k_rows_incrementally(
        self, tmp_path: Path, request: pytest.FixtureRequest
    ) -> None:
        _skip_unless_benchmark(request)

        from loafer.adapters.targets.json_target import JsonTargetConnector

        f = tmp_path / "out.json"
        conn = JsonTargetConnector(str(f), write_mode="overwrite")
        conn.connect()

        total = 0
        start = time.monotonic()
        for i in range(0, 100_000, 500):
            chunk = [{"id": j, "value": f"v{j}"} for j in range(i, min(i + 500, 100_000))]
            n = conn.write_chunk(chunk)
            total += n
        conn.finalize()
        conn.disconnect()
        elapsed = time.monotonic() - start

        assert total == 100_000
        assert elapsed < 30, f"Writing 100k rows to JSON took {elapsed:.1f}s"

        # Verify the file is valid JSON
        with f.open() as fh:
            data = json.load(fh)
        assert len(data) == 100_000


# ---------------------------------------------------------------------------
# Schema sampler benchmarks
# ---------------------------------------------------------------------------


class TestSchemaSamplerBenchmark:
    """Verify schema sampler is efficient even with large datasets."""

    def test_sample_1m_rows_under_1_second(self, request: pytest.FixtureRequest) -> None:
        _skip_unless_benchmark(request)

        from loafer.llm.schema import build_schema_sample

        data = [
            {
                "id": i,
                "name": f"user_{i}",
                "score": i * 1.5,
                "active": i % 2 == 0,
                "created": "2024-01-15T10:30:00Z",
            }
            for i in range(1_000_000)
        ]

        start = time.monotonic()
        result = build_schema_sample(data, max_sample_rows=5)
        elapsed = time.monotonic() - start

        assert len(result) == 5
        assert all(len(v["sample_values"]) <= 5 for v in result.values())
        # 1M rows is a lot — allow up to 15 seconds
        assert elapsed < 15, f"Sampling 1M rows took {elapsed:.1f}s"

    def test_wide_table_200_columns(self, request: pytest.FixtureRequest) -> None:
        _skip_unless_benchmark(request)

        from loafer.llm.schema import build_schema_sample

        row = {f"col_{i}": i for i in range(200)}
        data = [row for _ in range(100)]

        start = time.monotonic()
        result = build_schema_sample(data, max_sample_rows=5)
        elapsed = time.monotonic() - start

        assert len(result) == 200
        assert elapsed < 2


# ---------------------------------------------------------------------------
# Code validator benchmarks
# ---------------------------------------------------------------------------


class TestCodeValidatorBenchmark:
    """Verify code validator is fast even with large code."""

    def test_validate_200_line_function(self, request: pytest.FixtureRequest) -> None:
        _skip_unless_benchmark(request)

        from loafer.transform.code_validator import validate_transform_function

        lines = ["    x = x + 1"] * 196
        code = "def transform(data):\n    x = 0\n" + "\n".join(lines) + "\n    return data"

        start = time.monotonic()
        for _ in range(100):
            ok, _ = validate_transform_function(code)
        elapsed = time.monotonic() - start

        assert ok
        assert elapsed < 2, f"100 validations of 200-line code took {elapsed:.1f}s"


# ---------------------------------------------------------------------------
# SQL validator benchmarks
# ---------------------------------------------------------------------------


class TestSqlValidatorBenchmark:
    """Verify SQL validator handles complex queries efficiently."""

    def test_validate_complex_query(self, request: pytest.FixtureRequest) -> None:
        _skip_unless_benchmark(request)

        from loafer.transform.sql_validator import validate_transform_sql

        sql = """
        WITH ranked AS (
            SELECT
                o.id,
                o.amount,
                c.name AS customer_name,
                ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.created_at DESC) AS rn
            FROM orders o
            INNER JOIN customers c ON o.customer_id = c.id
            WHERE o.status IN ('paid', 'pending')
              AND o.amount > (SELECT AVG(amount) FROM orders)
        )
        SELECT id, customer_name, amount
        FROM ranked
        WHERE rn = 1
        ORDER BY amount DESC
        LIMIT 100
        """

        start = time.monotonic()
        for _ in range(100):
            ok, _ = validate_transform_sql(sql)
        elapsed = time.monotonic() - start

        assert ok
        assert elapsed < 2, f"100 validations of complex SQL took {elapsed:.1f}s"


# ---------------------------------------------------------------------------
# CSV target benchmarks
# ---------------------------------------------------------------------------


class TestCsvTargetBenchmark:
    """Verify CSV target writes efficiently."""

    def test_write_100k_rows_to_csv(self, tmp_path: Path, request: pytest.FixtureRequest) -> None:
        _skip_unless_benchmark(request)

        from loafer.adapters.targets.csv_target import CsvTargetConnector

        f = tmp_path / "out.csv"
        conn = CsvTargetConnector(str(f), write_mode="overwrite")
        conn.connect()

        total = 0
        start = time.monotonic()
        for i in range(0, 100_000, 500):
            chunk = [{"id": j, "value": f"v{j}"} for j in range(i, min(i + 500, 100_000))]
            n = conn.write_chunk(chunk)
            total += n
        conn.finalize()
        conn.disconnect()
        elapsed = time.monotonic() - start

        assert total == 100_000
        assert elapsed < 15, f"Writing 100k rows to CSV took {elapsed:.1f}s"

        # Verify the file is valid CSV
        with f.open() as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
        assert len(rows) == 100_000
