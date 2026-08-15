"""Tests for the Phase 1 local application boundary."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from loafer.adapters.runtime import (
    EnvironmentSecretResolver,
    InputReviewPort,
    NullCheckpointStore,
    NullEventPublisher,
    ScopedSecretResolver,
)
from loafer.application import RunRequest, get_local_application
from loafer.application.service import RunPipeline
from loafer.contracts import RunStatus, StageStatus
from loafer.exceptions import PipelineError


def _pipeline_fixture(tmp_path: Path) -> tuple[Path, Path]:
    source = tmp_path / "input.csv"
    source.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    transform = tmp_path / "transform.py"
    transform.write_text(
        "def transform(data):\n    return [{**row, 'name': row['name'].lower()} for row in data]\n",
        encoding="utf-8",
    )
    output = tmp_path / "output.json"
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        "\n".join(
            [
                "name: application-boundary",
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
                "chunk_size: 10",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return config, output


def test_csv_to_json_executes_through_application_interface(tmp_path: Path) -> None:
    config, output = _pipeline_fixture(tmp_path)
    request = RunRequest(config_path=str(config), run_id="application-e2e", auto_confirm=True)

    result = get_local_application().run_pipeline.run(request)

    assert result.status is RunStatus.SUCCEEDED
    assert result.run_id == "application-e2e"
    assert result.snapshot.rows_extracted == 2
    assert result.snapshot.rows_transformed == 2
    assert result.snapshot.rows_loaded == 2
    assert result.output_published is True
    assert json.loads(output.read_text(encoding="utf-8"))[0]["name"] == "alice"


def test_stream_emits_monotonic_serializable_events(tmp_path: Path) -> None:
    config, _output = _pipeline_fixture(tmp_path)
    request = RunRequest(config_path=str(config), run_id="stream-e2e", auto_confirm=True)

    events = list(get_local_application().run_pipeline.stream(request))

    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    assert {event.stage for event in events} >= {"extract", "validate", "transform", "load"}
    assert events[-1].status in {StageStatus.DONE, StageStatus.SKIPPED}
    assert all(
        type(event).model_validate_json(event.model_dump_json()) == event for event in events
    )
    rendered = "".join(event.model_dump_json() for event in events)
    assert "raw_data" not in rendered
    assert "stream_iterator" not in rendered
    assert "llm_provider" not in rendered


def test_plan_is_stable_and_does_not_expose_inline_api_key(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    source.write_text("id\n1\n", encoding="utf-8")
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        "\n".join(
            [
                "source:",
                "  type: csv",
                f"  path: {source}",
                "target:",
                "  type: json",
                f"  path: {tmp_path / 'output.json'}",
                "transform:",
                "  type: ai",
                "  instruction: return rows",
                "llm:",
                "  provider: openai",
                "  model: test-model",
                "  api_key: super-secret-key",
                "",
            ]
        ),
        encoding="utf-8",
    )
    request = RunRequest(config_path=str(config))
    use_case = get_local_application().run_pipeline

    first = use_case.create_plan(request)
    second = use_case.create_plan(request)

    assert first.plan_id == second.plan_id
    assert first.config_digest == second.config_digest
    assert "super-secret-key" not in first.model_dump_json()


class _AlwaysCancelled:
    def is_cancelled(self, run_id: str) -> bool:
        return run_id == "cancel-me"


def test_cancellation_is_checked_before_engine_execution(tmp_path: Path) -> None:
    config, output = _pipeline_fixture(tmp_path)
    use_case = RunPipeline(
        cancellation=_AlwaysCancelled(),
        checkpoints=NullCheckpointStore(),
        secrets=EnvironmentSecretResolver(),
        events=NullEventPublisher(),
        reviewer=InputReviewPort(),
    )

    with pytest.raises(PipelineError, match="cancelled"):
        list(use_case.stream(RunRequest(config_path=str(config), run_id="cancel-me")))

    assert not output.exists()


def test_validation_and_connector_listing_use_application_service(tmp_path: Path) -> None:
    config, _output = _pipeline_fixture(tmp_path)
    service = get_local_application()

    validation = service.validate(config)
    catalog = service.list_connectors()

    assert validation.valid is True
    assert validation.plan.source_type == "csv"
    assert "csv" in catalog.sources
    assert "json" in catalog.targets


def test_scoped_secret_resolver_enforces_allow_list_and_expiry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALLOWED_KEY", "visible")
    monkeypatch.setenv("OTHER_KEY", "hidden")
    now = [10.0]
    resolver = ScopedSecretResolver(
        EnvironmentSecretResolver(),
        {"ALLOWED_KEY"},
        ttl_seconds=5,
        clock=lambda: now[0],
    )

    assert resolver.resolve("ALLOWED_KEY") == "visible"
    assert resolver.resolve("OTHER_KEY") is None
    now[0] = 15.0
    assert resolver.resolve("ALLOWED_KEY") is None
