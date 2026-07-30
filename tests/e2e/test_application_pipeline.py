"""End-to-end execution through the Phase 1 application interface."""

from __future__ import annotations

import json
from pathlib import Path

from loafer.application import RunRequest, get_local_application
from loafer.contracts import RunStatus


def test_csv_transform_json_runs_through_application_service(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    source.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    transform = tmp_path / "transform.py"
    transform.write_text(
        "def transform(data):\n    return [{**row, 'name': row['name'].upper()} for row in data]\n",
        encoding="utf-8",
    )
    output = tmp_path / "output.json"
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        "\n".join(
            [
                "name: phase-one-vertical-slice",
                "source:",
                "  type: csv",
                f"  path: {source}",
                "target:",
                "  type: json",
                f"  path: {output}",
                "transform:",
                "  type: custom",
                f"  path: {transform}",
                "mode: etl",
                "chunk_size: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )

    result = get_local_application().run_pipeline.run(
        RunRequest(
            config_path=str(config),
            run_id="phase-one-e2e",
            auto_confirm=True,
        )
    )

    assert result.status is RunStatus.SUCCEEDED
    assert result.snapshot.rows_extracted == 2
    assert result.snapshot.rows_loaded == 2
    assert result.snapshot.source_type == "csv"
    assert result.snapshot.target_type == "json"
    assert json.loads(output.read_text(encoding="utf-8")) == [
        {"id": "1", "name": "ALICE"},
        {"id": "2", "name": "BOB"},
    ]
