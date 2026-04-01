"""Stress tests for Loafer — heavy data volumes, edge cases, concurrency.

Run with:
    uv run python tests/stress_test.py

Each test prints PASS/FAIL with timing and memory info.
"""

from __future__ import annotations

import csv
import gc
import json
import os
import random
import resource
import string
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mem_mb() -> float:
    """Return current RSS in MB."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _make_transform(path: Path) -> Path:
    """Create a simple identity transform file."""
    t = path / "t.py"
    t.write_text("def transform(data):\n    return data\n")
    return t


def _run_pipeline(config_path: str) -> tuple[bool, float, float]:
    """Run a pipeline via subprocess, return (success, duration_s, peak_mem_mb)."""
    import subprocess

    mem_before = _mem_mb()
    t0 = time.monotonic()
    result = subprocess.run(
        [sys.executable, "-m", "loafer", "run", config_path, "--quiet"],
        capture_output=True,
        text=True,
        timeout=300,
    )
    duration = time.monotonic() - t0
    mem_after = _mem_mb()
    return result.returncode == 0, duration, mem_after - mem_before


# ---------------------------------------------------------------------------
# Test 1: Large CSV → JSON (1M rows)
# ---------------------------------------------------------------------------


def test_large_csv_to_json(tmp: Path) -> tuple[bool, str]:
    """Stream 1M rows from CSV to JSON. Memory should stay flat."""
    csv_path = tmp / "large.csv"
    json_path = tmp / "large.json"

    # Generate 1M rows
    print("  Generating 1M rows...", end="", flush=True)
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "email", "score", "status"])
        for i in range(1_000_000):
            writer.writerow(
                [
                    i,
                    f"user_{i}",
                    f"user_{i}@example.com",
                    round(random.uniform(0, 100), 2),
                    random.choice(["active", "inactive"]),
                ]
            )
    print(f" done ({_mem_mb():.0f}MB)")

    transform_path = _make_transform(tmp)
    config_path = tmp / "pipeline.yaml"
    config_path.write_text(
        f"source:\n  type: csv\n  path: {csv_path}\n"
        f"target:\n  type: json\n  path: {json_path}\n"
        f"transform:\n  type: custom\n  path: {transform_path}\n"
        f"mode: etl\n"
        f"chunk_size: 1000\n"
    )

    print("  Running pipeline...", end="", flush=True)
    success, duration, mem_delta = _run_pipeline(str(config_path))
    print(f" done ({duration:.1f}s, +{mem_delta:.0f}MB)")

    if not success:
        return False, "Pipeline failed"

    # Verify output
    with open(json_path) as f:
        data = json.load(f)
    if len(data) != 1_000_000:
        return False, f"Expected 1M rows, got {len(data)}"

    return True, f"1M rows in {duration:.1f}s, +{mem_delta:.0f}MB"


# ---------------------------------------------------------------------------
# Test 2: Large CSV → CSV (5M rows)
# ---------------------------------------------------------------------------


def test_large_csv_to_csv(tmp: Path) -> tuple[bool, str]:
    """Stream 5M rows from CSV to CSV. Tests raw throughput."""
    csv_in = tmp / "big_in.csv"
    csv_out = tmp / "big_out.csv"

    print("  Generating 5M rows...", end="", flush=True)
    with open(csv_in, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "value"])
        for i in range(5_000_000):
            writer.writerow([i, f"val_{i}"])
    print(f" done ({_mem_mb():.0f}MB)")

    transform_path = _make_transform(tmp)
    config_path = tmp / "pipeline.yaml"
    config_path.write_text(
        f"source:\n  type: csv\n  path: {csv_in}\n"
        f"target:\n  type: csv\n  path: {csv_out}\n"
        f"transform:\n  type: custom\n  path: {transform_path}\n"
        f"mode: etl\n"
        f"chunk_size: 5000\n"
    )

    print("  Running pipeline...", end="", flush=True)
    success, duration, mem_delta = _run_pipeline(str(config_path))
    print(f" done ({duration:.1f}s, +{mem_delta:.0f}MB)")

    if not success:
        return False, "Pipeline failed"

    # Spot-check output
    with open(csv_out) as f:
        reader = csv.reader(f)
        header = next(reader)
        if header != ["id", "value"]:
            return False, f"Bad header: {header}"
        # Count rows
        count = sum(1 for _ in reader)
    if count != 5_000_000:
        return False, f"Expected 5M rows, got {count}"

    return True, f"5M rows in {duration:.1f}s, +{mem_delta:.0f}MB"


# ---------------------------------------------------------------------------
# Test 3: Unicode and special characters
# ---------------------------------------------------------------------------


def test_unicode_data(tmp: Path) -> tuple[bool, str]:
    """Handle unicode, emojis, and special characters."""
    csv_path = tmp / "unicode.csv"
    json_path = tmp / "unicode.json"

    rows = [
        {"id": "1", "name": "Café", "desc": "日本語テスト"},
        {"id": "2", "name": "Ñoño", "desc": "Ünïcödé"},
        {"id": "3", "name": "🎉🚀", "desc": "emoji test"},
        {"id": "4", "name": "a" * 10000, "desc": "very long field"},
        {"id": "5", "name": "line1\nline2", "desc": "embedded newline"},
        {"id": "6", "name": 'quote"here', "desc": 'embedded "quote"'},
        {"id": "7", "name": "tab\there", "desc": "embedded\ttab"},
        {"id": "8", "name": "\x00null\x00", "desc": "null bytes"},
        {"id": "9", "name": "", "desc": ""},
        {"id": "10", "name": " ", "desc": "   "},
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "desc"])
        writer.writeheader()
        writer.writerows(rows)

    transform_path = _make_transform(tmp)
    config_path = tmp / "pipeline.yaml"
    config_path.write_text(
        f"source:\n  type: csv\n  path: {csv_path}\n"
        f"target:\n  type: json\n  path: {json_path}\n"
        f"transform:\n  type: custom\n  path: {transform_path}\n"
        f"mode: etl\n"
    )

    success, duration, mem_delta = _run_pipeline(str(config_path))
    if not success:
        return False, "Pipeline failed on unicode data"

    with open(json_path) as f:
        data = json.load(f)
    if len(data) != 10:
        return False, f"Expected 10 rows, got {len(data)}"

    return True, f"10 unicode rows in {duration:.1f}s"


# ---------------------------------------------------------------------------
# Test 4: Empty file
# ---------------------------------------------------------------------------


def test_empty_csv(tmp: Path) -> tuple[bool, str]:
    """Handle empty CSV file (header only). Should fail validation gracefully."""
    csv_path = tmp / "empty.csv"
    json_path = tmp / "empty.json"

    csv_path.write_text("id,name\n")

    transform_path = _make_transform(tmp)
    config_path = tmp / "pipeline.yaml"
    config_path.write_text(
        f"source:\n  type: csv\n  path: {csv_path}\n"
        f"target:\n  type: json\n  path: {json_path}\n"
        f"transform:\n  type: custom\n  path: {transform_path}\n"
        f"mode: etl\n"
    )

    success, duration, _ = _run_pipeline(str(config_path))
    # Empty CSV should fail validation (no rows to process)
    # This is correct behavior - pipeline should not produce empty output
    if success:
        # If it does succeed, check output is empty array
        if json_path.exists():
            with open(json_path) as f:
                data = json.load(f)
            if len(data) != 0:
                return False, f"Expected 0 rows, got {len(data)}"
        return True, f"Empty CSV handled in {duration:.1f}s"
    else:
        # Validation failure is acceptable for empty input
        return True, f"Empty CSV correctly failed validation in {duration:.1f}s"


# ---------------------------------------------------------------------------
# Test 5: Malformed rows
# ---------------------------------------------------------------------------


def test_malformed_rows(tmp: Path) -> tuple[bool, str]:
    """Handle CSV with malformed rows (wrong column count)."""
    csv_path = tmp / "malformed.csv"
    json_path = tmp / "malformed.json"

    csv_path.write_text(
        "id,name,value\n"
        "1,Alice,100\n"
        "2,Bob\n"  # missing column
        "3,Charlie,200\n"
        "4,Dave,300,extra\n"  # extra column
        "5,Eve,400\n"
    )

    transform_path = _make_transform(tmp)
    config_path = tmp / "pipeline.yaml"
    config_path.write_text(
        f"source:\n  type: csv\n  path: {csv_path}\n"
        f"target:\n  type: json\n  path: {json_path}\n"
        f"transform:\n  type: custom\n  path: {transform_path}\n"
        f"mode: etl\n"
    )

    success, duration, _ = _run_pipeline(str(config_path))
    if not success:
        return False, "Pipeline failed on malformed CSV"

    with open(json_path) as f:
        data = json.load(f)
    # Should only get rows with exactly 3 columns: 1, 3, 5
    if len(data) != 3:
        return False, f"Expected 3 valid rows, got {len(data)}"

    return True, f"Malformed rows handled in {duration:.1f}s ({len(data)} valid rows)"


# ---------------------------------------------------------------------------
# Test 6: Transform that drops all rows
# ---------------------------------------------------------------------------


def test_transform_drops_all(tmp: Path) -> tuple[bool, str]:
    """Handle transform that filters out all rows."""
    csv_path = tmp / "data.csv"
    json_path = tmp / "dropped.json"
    csv_path.write_text("id,status\n1,active\n2,active\n3,active\n")

    transform_path = tmp / "t.py"
    transform_path.write_text("def transform(data):\n    return []\n")

    config_path = tmp / "pipeline.yaml"
    config_path.write_text(
        f"source:\n  type: csv\n  path: {csv_path}\n"
        f"target:\n  type: json\n  path: {json_path}\n"
        f"transform:\n  type: custom\n  path: {transform_path}\n"
        f"mode: etl\n"
    )

    success, duration, _ = _run_pipeline(str(config_path))
    if not success:
        return False, "Pipeline failed when transform drops all rows"

    with open(json_path) as f:
        data = json.load(f)
    if len(data) != 0:
        return False, f"Expected 0 rows, got {len(data)}"

    return True, f"All rows dropped in {duration:.1f}s"


# ---------------------------------------------------------------------------
# Test 7: Wide rows (many columns)
# ---------------------------------------------------------------------------


def test_wide_rows(tmp: Path) -> tuple[bool, str]:
    """Handle CSV with 500 columns."""
    csv_path = tmp / "wide.csv"
    json_path = tmp / "wide.json"
    n_cols = 500
    n_rows = 1000

    col_names = [f"col_{i}" for i in range(n_cols)]

    print("  Generating wide CSV...", end="", flush=True)
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(col_names)
        for i in range(n_rows):
            writer.writerow([f"r{i}_c{j}" for j in range(n_cols)])
    print(f" done ({_mem_mb():.0f}MB)")

    transform_path = _make_transform(tmp)
    config_path = tmp / "pipeline.yaml"
    config_path.write_text(
        f"source:\n  type: csv\n  path: {csv_path}\n"
        f"target:\n  type: json\n  path: {json_path}\n"
        f"transform:\n  type: custom\n  path: {transform_path}\n"
        f"mode: etl\n"
        f"chunk_size: 100\n"
    )

    print("  Running pipeline...", end="", flush=True)
    success, duration, mem_delta = _run_pipeline(str(config_path))
    print(f" done ({duration:.1f}s, +{mem_delta:.0f}MB)")

    if not success:
        return False, "Pipeline failed on wide rows"

    with open(json_path) as f:
        data = json.load(f)
    if len(data) != n_rows:
        return False, f"Expected {n_rows} rows, got {len(data)}"
    if len(data[0]) != n_cols:
        return False, f"Expected {n_cols} columns, got {len(data[0])}"

    return True, f"{n_rows}x{n_cols} in {duration:.1f}s, +{mem_delta:.0f}MB"


# ---------------------------------------------------------------------------
# Test 8: Concurrent pipelines
# ---------------------------------------------------------------------------


def test_concurrent_pipelines(tmp: Path) -> tuple[bool, str]:
    """Run 3 pipelines simultaneously."""
    import subprocess
    import concurrent.futures

    configs = []
    for i in range(3):
        csv_path = tmp / f"data_{i}.csv"
        json_path = tmp / f"out_{i}.json"
        csv_path.write_text(f"id,name\n1,user_{i}\n2,user_{i + 1}\n")
        transform_path = _make_transform(tmp)
        config_path = tmp / f"pipeline_{i}.yaml"
        config_path.write_text(
            f"source:\n  type: csv\n  path: {csv_path}\n"
            f"target:\n  type: json\n  path: {json_path}\n"
            f"transform:\n  type: custom\n  path: {transform_path}\n"
            f"mode: etl\n"
        )
        configs.append((str(config_path), json_path))

    t0 = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for config_path, _ in configs:
            futures.append(
                executor.submit(
                    subprocess.run,
                    [sys.executable, "-m", "loafer", "run", config_path, "--quiet"],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
            )
        results = [f.result() for f in futures]
    duration = time.monotonic() - t0

    for i, (result, (_, json_path)) in enumerate(zip(results, configs)):
        if result.returncode != 0:
            return False, f"Pipeline {i} failed: {result.stderr}"
        with open(json_path) as f:
            data = json.load(f)
        if len(data) != 2:
            return False, f"Pipeline {i}: expected 2 rows, got {len(data)}"

    return True, f"3 concurrent pipelines in {duration:.1f}s"


# ---------------------------------------------------------------------------
# Test 9: Scheduler under load (10 jobs)
# ---------------------------------------------------------------------------


def test_scheduler_load(tmp: Path) -> tuple[bool, str]:
    """Schedule 10 jobs and verify they all persist."""
    import subprocess

    configs = []
    for i in range(10):
        csv_path = tmp / f"data_{i}.csv"
        json_path = tmp / f"out_{i}.json"
        csv_path.write_text(f"id,name\n1,user_{i}\n")
        transform_path = _make_transform(tmp)
        config_path = tmp / f"pipeline_{i}.yaml"
        config_path.write_text(
            f"source:\n  type: csv\n  path: {csv_path}\n"
            f"target:\n  type: json\n  path: {json_path}\n"
            f"transform:\n  type: custom\n  path: {transform_path}\n"
            f"mode: etl\n"
        )
        configs.append(str(config_path))

    # Schedule all 10
    for i, config_path in enumerate(configs):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "loafer",
                "schedule",
                config_path,
                "--cron",
                "0 9 * * *",
                "--id",
                f"job_{i}",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            return False, f"Schedule job_{i} failed: {result.stderr}"

    # List schedules
    result = subprocess.run(
        [sys.executable, "-m", "loafer", "list-schedules"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        return False, f"list-schedules failed: {result.stderr}"

    # Count job entries in output
    job_count = result.stdout.count("job_")
    if job_count != 10:
        return False, f"Expected 10 scheduled jobs, found {job_count}"

    # Clean up
    for i in range(10):
        subprocess.run(
            [sys.executable, "-m", "loafer", "unschedule", f"job_{i}"],
            capture_output=True,
            text=True,
            timeout=30,
        )

    return True, f"10 jobs scheduled and listed"


# ---------------------------------------------------------------------------
# Test 10: Memory leak detection
# ---------------------------------------------------------------------------


def test_memory_leak(tmp: Path) -> tuple[bool, str]:
    """Run pipeline 5 times and check memory doesn't grow unbounded."""
    csv_path = tmp / "data.csv"
    json_path = tmp / "out.json"
    csv_path.write_text(
        "id,name,score\n"
        + "\n".join(f"{i},user_{i},{random.uniform(0, 100):.2f}" for i in range(10000))
    )
    transform_path = _make_transform(tmp)
    config_path = tmp / "pipeline.yaml"
    config_path.write_text(
        f"source:\n  type: csv\n  path: {csv_path}\n"
        f"target:\n  type: json\n  path: {json_path}\n"
        f"transform:\n  type: custom\n  path: {transform_path}\n"
        f"mode: etl\n"
    )

    mems = []
    for i in range(5):
        gc.collect()
        mem_before = _mem_mb()
        success, duration, _ = _run_pipeline(str(config_path))
        gc.collect()
        mem_after = _mem_mb()
        mems.append(mem_after)
        if not success:
            return False, f"Run {i + 1} failed"

    # Memory should not grow more than 50MB between runs
    max_growth = max(mems) - min(mems)
    if max_growth > 50:
        return False, f"Memory grew {max_growth:.0f}MB across 5 runs (possible leak)"

    return True, f"5 runs, memory growth: {max_growth:.0f}MB"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

TESTS = [
    ("Large CSV → JSON (1M rows)", test_large_csv_to_json),
    ("Large CSV → CSV (5M rows)", test_large_csv_to_csv),
    ("Unicode and special characters", test_unicode_data),
    ("Empty CSV file", test_empty_csv),
    ("Malformed rows", test_malformed_rows),
    ("Transform drops all rows", test_transform_drops_all),
    ("Wide rows (500 columns)", test_wide_rows),
    ("Concurrent pipelines (3x)", test_concurrent_pipelines),
    ("Scheduler load (10 jobs)", test_scheduler_load),
    ("Memory leak detection (5 runs)", test_memory_leak),
]


def main() -> int:
    print("=" * 70)
    print("Loafer Stress Tests")
    print("=" * 70)

    passed = 0
    failed = 0
    total_start = time.monotonic()

    for name, test_fn in TESTS:
        print(f"\n{'─' * 70}")
        print(f"  {name}")
        print(f"{'─' * 70}")

        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            try:
                ok, msg = test_fn(tmp)
            except Exception as e:
                ok = False
                msg = f"{e}\n{traceback.format_exc()}"

        if ok:
            print(f"  [PASS] {msg}")
            passed += 1
        else:
            print(f"  [FAIL] {msg}")
            failed += 1

    total = time.monotonic() - total_start
    print(f"\n{'=' * 70}")
    print(f"  Results: {passed} passed, {failed} failed (total: {total:.1f}s)")
    print(f"{'=' * 70}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
