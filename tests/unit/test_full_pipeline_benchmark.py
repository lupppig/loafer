"""Tests for the full-pipeline benchmark's deterministic primitives."""

from __future__ import annotations

import csv
import os
from pathlib import Path

import pytest

from benchmarks.full_pipeline import (
    _csv_data_row_count,
    _generate_input,
    _process_group_rss_bytes,
    _sha256,
    _verified_throughput,
)


def test_generated_input_is_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"

    _generate_input(first, 100)
    _generate_input(second, 100)

    assert _csv_data_row_count(first) == 100
    assert _sha256(first) == _sha256(second)


def test_checksum_detects_output_change(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    changed = tmp_path / "changed.csv"
    _generate_input(source, 10)
    _generate_input(changed, 10)

    rows = list(csv.reader(changed.open(newline="", encoding="utf-8")))
    rows[-1][-1] = "corrupted"
    with changed.open("w", newline="", encoding="utf-8") as handle:
        csv.writer(handle).writerows(rows)

    assert _sha256(source) != _sha256(changed)


@pytest.mark.skipif(not Path("/proc").is_dir(), reason="requires Linux /proc")
def test_process_group_rss_includes_current_process() -> None:
    assert _process_group_rss_bytes(os.getpgrp()) > 0


def test_throughput_is_only_reported_for_verified_output() -> None:
    assert _verified_throughput(1000, 2.0, correct=True) == 500.0
    assert _verified_throughput(10_000_000, 18.0, correct=False) is None
