#!/usr/bin/env python3
"""Manual test script for Phase 3 agents.

Usage:
    uv run python tests/manual_test.py
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from loafer.agents.extract import extract_agent
from loafer.agents.load import load_agent
from loafer.agents.transform import transform_agent
from loafer.agents.validate import validate_agent
from loafer.config import CsvSourceConfig, JsonTargetConfig


def main() -> None:
    tmpdir = Path(tempfile.mkdtemp())
    csv_path = tmpdir / "test_data.csv"
    csv_path.write_text(
        "id,name,score,active\n"
        "1,Alice,95.5,True\n"
        "2,Bob,88.0,False\n"
        "3,Charlie,72.3,True\n"
        "4,Diana,91.0,True\n"
        "5,Eve,65.0,False\n"
    )

    transform_path = tmpdir / "transform.py"
    transform_path.write_text(
        "def transform(data):\n"
        "    return [{**r, 'grade': 'A' if float(r.get('score', 0)) >= 90 else 'B'} for r in data]\n"
    )

    output_path = tmpdir / "output.json"

    print("=" * 60)
    print("PHASE 3 MANUAL TEST — ETL Pipeline")
    print("=" * 60)

    # Step 1: Extract
    print("\n[1/4] Extract Agent...")
    state: dict[str, Any] = {
        "source_config": CsvSourceConfig(type="csv", path=str(csv_path)),
        "streaming_threshold": 10000,
        "chunk_size": 500,
        "duration_ms": {},
        "warnings": [],
    }
    state = extract_agent(state)
    print(f"  rows_extracted: {state['rows_extracted']}")
    print(f"  is_streaming:   {state['is_streaming']}")
    print(f"  schema_sample:  {list(state['schema_sample'].keys())}")
    print(f"  duration_ms:    {state['duration_ms'].get('extract', 0):.1f}")

    # Step 2: Validate
    print("\n[2/4] Validate Agent...")
    state = validate_agent(state)
    print(f"  validation_passed: {state['validation_passed']}")
    report = state["validation_report"]
    print(f"  columns checked:   {list(report['columns'].keys())}")
    if report.get("soft_warnings"):
        for w in report["soft_warnings"]:
            print(f"  ⚠ {w}")
    print(f"  duration_ms:       {state['duration_ms'].get('validate', 0):.1f}")

    # Step 3: Transform (custom mode)
    print("\n[3/4] Transform Agent (custom)...")
    state["transform_config"] = {"type": "custom", "path": str(transform_path)}
    state = transform_agent(state)
    print(f"  rows_transformed: {len(state['transformed_data'])}")
    print(f"  sample row:       {state['transformed_data'][0]}")
    print(f"  duration_ms:      {state['duration_ms'].get('transform', 0):.1f}")

    # Step 4: Load
    print("\n[4/4] Load Agent...")
    state["target_config"] = JsonTargetConfig(type="json", path=str(output_path))
    state = load_agent(state)
    print(f"  rows_loaded:  {state['rows_loaded']}")
    print(f"  output file:  {output_path}")
    print(f"  duration_ms:  {state['duration_ms'].get('load', 0):.1f}")

    # Show output
    print("\n" + "=" * 60)
    print("OUTPUT:")
    print("=" * 60)
    output = json.loads(output_path.read_text())
    for row in output:
        print(f"  {row}")

    total_ms = sum(state["duration_ms"].values())
    print(f"\nTotal pipeline time: {total_ms:.1f}ms")
    print(f"Temp dir: {tmpdir}")


if __name__ == "__main__":
    main()
