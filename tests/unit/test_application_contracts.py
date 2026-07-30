"""Round-trip tests for durable Phase 1 contracts."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TypeVar

from loafer.contracts import (
    BatchEnvelope,
    Checkpoint,
    ConnectorCatalog,
    ExecutionPlan,
    RunEvent,
    RunRequest,
    RunResult,
    RunSnapshot,
    RunStatus,
    StageStatus,
    TransformStepResult,
    ValidationResult,
)

ContractT = TypeVar("ContractT")


def _round_trip(value: ContractT) -> ContractT:
    model_type = type(value)
    restored = model_type.model_validate_json(value.model_dump_json())  # type: ignore[attr-defined]
    assert restored == value
    return restored


def _plan() -> ExecutionPlan:
    return ExecutionPlan(
        plan_id="plan-1",
        config_digest="a" * 64,
        config_path="/pipelines/orders.yaml",
        pipeline_name="orders",
        mode="etl",
        source_type="csv",
        target_type="json",
        transform_type="custom",
        chunk_size=500,
        streaming_threshold=10_000,
        validation_strict=False,
        llm_provider="openai",
        llm_model="gpt-test",
        incremental_column="updated_at",
        cursor_value="2026-07-30T00:00:00Z",
    )


def _snapshot() -> RunSnapshot:
    return RunSnapshot(
        run_id="run-1",
        plan_id="plan-1",
        pipeline_name="orders",
        mode="etl",
        source_type="csv",
        target_type="json",
        transform_type="pipeline",
        rows_extracted=10,
        rows_transformed=9,
        rows_loaded=9,
        validation_passed=True,
        duration_ms={"extract": 12.5, "total": 30.0},
        warnings=("one warning",),
        token_usage={"total_tokens": 42},
        step_results=(
            TransformStepResult(
                index=0,
                name="normalize",
                type="custom",
                rows_in=10,
                rows_out=9,
                duration_ms=4.2,
                success=True,
            ),
        ),
    )


def test_all_durable_contracts_round_trip_through_json() -> None:
    now = datetime(2026, 7, 30, tzinfo=UTC)
    plan = _round_trip(_plan())
    snapshot = _round_trip(_snapshot())
    request = _round_trip(
        RunRequest(
            config_path="/pipelines/orders.yaml",
            run_id="run-1",
            dry_run=True,
        )
    )
    batch = _round_trip(
        BatchEnvelope(
            run_id="run-1",
            stage_id="transform",
            partition_id="partition-1",
            batch_id="batch-1",
            attempt=0,
            source_position_start={"offset": 0},
            source_position_end={"offset": 499},
            schema_version="schema-1",
            transform_artifact_version="transform-1",
            rows_in=500,
            rows_out=490,
            rows_rejected=10,
            bytes_in=4096,
            bytes_out=3900,
            checksum="sha256:example",
        )
    )
    checkpoint = _round_trip(
        Checkpoint(
            checkpoint_id="checkpoint-1",
            run_id="run-1",
            partition_id="partition-1",
            batch_id="batch-1",
            source_position={"offset": 499},
            committed_at=now,
        )
    )
    event = _round_trip(
        RunEvent(
            run_id="run-1",
            plan_id="plan-1",
            sequence=1,
            stage="extract",
            status=StageStatus.DONE,
            occurred_at=now,
            snapshot=snapshot,
        )
    )
    result = _round_trip(
        RunResult(
            run_id="run-1",
            plan_id="plan-1",
            status=RunStatus.SUCCEEDED,
            started_at=now,
            finished_at=now,
            output_published=True,
            snapshot=snapshot,
        )
    )
    validation = _round_trip(ValidationResult(plan=plan))
    catalog = _round_trip(ConnectorCatalog(sources=("csv", "postgres"), targets=("json",)))

    assert request.run_id == batch.run_id == checkpoint.run_id == event.run_id == result.run_id
    assert validation.valid is True
    assert catalog.sources == ("csv", "postgres")


def test_execution_plan_contains_no_credentials_or_runtime_objects() -> None:
    rendered = _plan().model_dump_json()
    assert "api_key" not in rendered
    assert "password" not in rendered
    assert "connector" not in rendered
    assert "iterator" not in rendered
    assert "provider" in rendered  # Provider name is metadata, not a live provider object.
