"""End-to-end contracts for declared bounded row-local execution."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from loafer.adapters.runtime import (
    EnvironmentSecretResolver,
    InputReviewPort,
    NeverCancelled,
    NullEventPublisher,
)
from loafer.application import RunRequest, get_local_application
from loafer.application.service import RunPipeline
from loafer.core.batches import RollingRowsDigest
from loafer.exceptions import PipelineError
from loafer.llm.base import TransformPromptResult


def _bounded_config(
    tmp_path: Path,
    *,
    transform_code: str | None = None,
    rows: int = 5,
    chunk_size: int = 2,
) -> tuple[Path, Path, Path]:
    source = tmp_path / "input.csv"
    source.write_text(
        "id,name\n" + "".join(f"{index},name-{index}\n" for index in range(1, rows + 1)),
        encoding="utf-8",
    )
    transform = tmp_path / "transform.py"
    transform.write_text(
        transform_code
        or (
            "def transform(data):\n"
            "    return [{**row, 'name': row['name'].upper()} for row in data]\n"
        ),
        encoding="utf-8",
    )
    output = tmp_path / "output.json"
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        "\n".join(
            [
                "name: bounded-data-plane",
                "mode: etl",
                "source:",
                "  type: csv",
                f"  path: {source}",
                "target:",
                "  type: json",
                f"  path: {output}",
                "transform:",
                "  type: custom",
                f"  path: {transform}",
                "execution:",
                "  transform_class: row_local",
                "  schema_drift: fail",
                f"chunk_size: {chunk_size}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return config, source, output


def test_csv_custom_json_flows_as_bounded_batches(tmp_path: Path) -> None:
    config, _source, output = _bounded_config(tmp_path)
    request = RunRequest(config_path=str(config), run_id="bounded-e2e", auto_confirm=True)

    events = list(get_local_application().run_pipeline.stream(request))
    batch_events = [event for event in events if event.stage == "batch"]
    final = events[-1].snapshot

    assert len(batch_events) == 3
    assert all(event.batch is not None for event in batch_events)
    assert all(event.batch.rows_in <= 2 for event in batch_events if event.batch)
    assert (
        len(
            {
                event.batch.transform_artifact_version
                for event in batch_events
                if event.batch is not None
            }
        )
        == 1
    )
    assert final.rows_extracted == 5
    assert final.rows_transformed == 5
    assert final.rows_loaded == 5
    assert final.rows_rejected == 0
    assert final.batches_completed == 3
    assert final.input_checksum
    assert final.output_checksum
    assert events[-1].stage == "load"
    assert json.loads(output.read_text(encoding="utf-8"))[-1]["name"] == "NAME-5"
    assert (
        get_local_application().validate(config).plan.delivery_guarantee == "atomic_run_publication"
    )


def test_run_checksums_reconcile_with_published_rows(tmp_path: Path) -> None:
    config, _source, output = _bounded_config(tmp_path, rows=4, chunk_size=1)

    result = get_local_application().run_pipeline.run(
        RunRequest(config_path=str(config), run_id="checksum-e2e", auto_confirm=True)
    )
    published = json.loads(output.read_text(encoding="utf-8"))
    expected_output = RollingRowsDigest()
    expected_output.update(published)

    assert result.snapshot.output_checksum == expected_output.checksum
    assert result.snapshot.rows_loaded == expected_output.rows
    assert result.snapshot.bytes_out == expected_output.bytes


def test_bounded_runtime_state_retains_no_run_sized_row_lists(tmp_path: Path) -> None:
    config, _source, _output = _bounded_config(tmp_path, rows=7, chunk_size=2)

    state = get_local_application().run_pipeline.run_state(
        RunRequest(config_path=str(config), run_id="bounded-state", auto_confirm=True)
    )

    assert state["raw_data"] == []
    assert state["transformed_data"] == []
    assert state["rows_extracted"] == 7
    assert state["rows_transformed"] == 7
    assert state["batches_completed"] == 4


def test_transform_failure_does_not_replace_existing_output(tmp_path: Path) -> None:
    config, _source, output = _bounded_config(
        tmp_path,
        transform_code=(
            "def transform(data):\n"
            "    for row in data:\n"
            "        if row['id'] == '3':\n"
            "            return 1 / 0\n"
            "    return data\n"
        ),
        rows=4,
        chunk_size=2,
    )
    original = [{"existing": True}]
    output.write_text(json.dumps(original), encoding="utf-8")

    with pytest.raises(PipelineError, match="division by zero"):
        get_local_application().run_pipeline.run(
            RunRequest(config_path=str(config), run_id="target-failure", auto_confirm=True)
        )

    assert json.loads(output.read_text(encoding="utf-8")) == original
    assert list(tmp_path.glob(".output.json.*.tmp")) == []


def test_target_failure_discards_all_staged_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loafer.adapters.targets.json_target import JsonTargetConnector
    from loafer.connectors import registry
    from loafer.exceptions import LoadError

    class _FailingJsonTarget(JsonTargetConnector):
        def __init__(self, path: str, write_mode: str = "overwrite") -> None:
            super().__init__(path, write_mode)
            self._batch_calls = 0

        def write_chunk(self, chunk: list[dict[str, Any]]) -> int:
            self._batch_calls += 1
            written = super().write_chunk(chunk)
            if self._batch_calls == 2:
                raise LoadError("injected target failure")
            return written

    monkeypatch.setitem(registry._TARGET_REGISTRY, "json", _FailingJsonTarget)
    config, _source, output = _bounded_config(tmp_path, rows=5, chunk_size=2)
    original = [{"existing": True}]
    output.write_text(json.dumps(original), encoding="utf-8")

    with pytest.raises(PipelineError, match="injected target failure"):
        get_local_application().run_pipeline.run(
            RunRequest(config_path=str(config), run_id="load-failure", auto_confirm=True)
        )

    assert json.loads(output.read_text(encoding="utf-8")) == original
    assert list(tmp_path.glob(".output.json.*.tmp")) == []


class _CancelAfterFirstBatch:
    def __init__(self) -> None:
        self.calls = 0

    def is_cancelled(self, run_id: str) -> bool:
        assert run_id == "cancel-bounded"
        self.calls += 1
        return self.calls >= 3


class _RecordingCheckpoints:
    def __init__(self, output: Path) -> None:
        self.output = output
        self.saved: list[object] = []

    def load(self, run_id: str, partition_id: str) -> None:
        del run_id, partition_id
        return None

    def save(self, checkpoint: object) -> None:
        assert self.output.exists()
        self.saved.append(checkpoint)


def _run_use_case(
    cancellation: object,
    checkpoints: object,
) -> RunPipeline:
    return RunPipeline(
        cancellation=cancellation,  # type: ignore[arg-type]
        checkpoints=checkpoints,  # type: ignore[arg-type]
        secrets=EnvironmentSecretResolver(),
        events=NullEventPublisher(),
        reviewer=InputReviewPort(),
    )


def test_cancellation_at_batch_boundary_discards_staged_output(tmp_path: Path) -> None:
    config, _source, output = _bounded_config(tmp_path, rows=6, chunk_size=2)
    use_case = _run_use_case(_CancelAfterFirstBatch(), _RecordingCheckpoints(output))

    with pytest.raises(PipelineError, match="cancelled"):
        use_case.run(
            RunRequest(config_path=str(config), run_id="cancel-bounded", auto_confirm=True)
        )

    assert not output.exists()
    assert list(tmp_path.glob(".output.json.*.tmp")) == []


def test_checkpoint_is_saved_only_after_atomic_publication(tmp_path: Path) -> None:
    config, _source, output = _bounded_config(tmp_path, rows=3, chunk_size=2)
    checkpoints = _RecordingCheckpoints(output)
    use_case = _run_use_case(NeverCancelled(), checkpoints)

    use_case.run(
        RunRequest(config_path=str(config), run_id="checkpoint-bounded", auto_confirm=True)
    )

    assert len(checkpoints.saved) == 1


def test_validation_quarantine_reconciles_rejected_rows(tmp_path: Path) -> None:
    config, _source, output = _bounded_config(tmp_path, rows=3, chunk_size=2)
    quarantine = tmp_path / "quarantine.json"
    text = config.read_text(encoding="utf-8")
    text = text.replace(
        "  schema_drift: fail\n",
        (
            "  schema_drift: fail\n"
            f"  quarantine_path: {quarantine}\n"
            "validation:\n"
            "  column_types:\n"
            "    id: integer\n"
            "  on_failure: quarantine\n"
        ),
    )
    config.write_text(text, encoding="utf-8")

    result = get_local_application().run_pipeline.run(
        RunRequest(config_path=str(config), run_id="quarantine-bounded", auto_confirm=True)
    )

    assert result.snapshot.rows_extracted == 3
    assert result.snapshot.rows_loaded == 0
    assert result.snapshot.rows_rejected == 3
    assert json.loads(output.read_text(encoding="utf-8")) == []
    rejected = json.loads(quarantine.read_text(encoding="utf-8"))
    assert len(rejected) == 3
    assert rejected[0]["_loafer"]["stage"] == "validation"


def test_ai_artifact_is_generated_once_and_reused_per_batch(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    source.write_text("id\n1\n2\n3\n", encoding="utf-8")
    output = tmp_path / "output.json"
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        "\n".join(
            [
                "source:",
                "  type: csv",
                f"  path: {source}",
                "target:",
                "  type: json",
                f"  path: {output}",
                "transform:",
                "  type: ai",
                "  instruction: copy every row",
                "execution:",
                "  transform_class: row_local",
                "chunk_size: 1",
                "llm:",
                "  provider: openai",
                "  model: test-model",
                "  api_key: test-key",
                "",
            ]
        ),
        encoding="utf-8",
    )
    provider = MagicMock()
    provider.generate_transform_function.return_value = TransformPromptResult(
        code="def transform(data):\n    return data\n",
        raw_response="identity",
        token_usage={"total_tokens": 10},
    )

    get_local_application(provider_factory=lambda _config, _secrets: provider).run_pipeline.run(
        RunRequest(config_path=str(config), run_id="ai-bounded", auto_confirm=True)
    )

    provider.generate_transform_function.assert_called_once()
    assert len(json.loads(output.read_text(encoding="utf-8"))) == 3
