"""Bounded single-node ETL data plane for declared row-local transforms."""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from loafer.config import JsonTargetConfig, PipelineConfig
from loafer.connectors.registry import (
    get_source_connector,
    get_staged_target_connector,
    get_target_connector,
)
from loafer.contracts import BatchEnvelope, Checkpoint
from loafer.core.batches import (
    RejectedRow,
    RollingRowsDigest,
    SchemaTracker,
    canonical_row_bytes,
    validate_batch,
)
from loafer.core.destructive import detect_destructive_operations, raise_if_destructive
from loafer.exceptions import PipelineError, ValidationError
from loafer.llm.schema import build_schema_sample
from loafer.transform.batch_runner import (
    PreparedBatchTransform,
    prepare_transform_artifact,
    transform_batch,
)

if TYPE_CHECKING:
    from loafer.graph.state import PipelineState
    from loafer.ports.connector import SourceConnector, TargetConnector
    from loafer.ports.metadata import BatchRecoveryPort
    from loafer.ports.runtime import CancellationPort, CheckpointPort

_INCREMENTAL_PUSHDOWN_SOURCES = {"postgres", "mysql", "sqlite", "rest_api"}
_PARTITION_ID = "default"


def uses_bounded_data_plane(config: PipelineConfig) -> bool:
    """Return whether the config explicitly selected bounded row-local execution."""
    return config.mode == "etl" and config.execution.transform_class == "row_local"


def stream_bounded_pipeline(
    config: PipelineConfig,
    state: PipelineState,
    *,
    dry_run: bool,
    cancellation: CancellationPort | None,
    checkpoints: CheckpointPort | None,
    recovery: BatchRecoveryPort | None = None,
) -> Iterator[tuple[str, str, PipelineState]]:
    """Flow source batches through validation, transform, and staged publication."""
    source: SourceConnector | None = None
    target: TargetConnector | None = None
    quarantine: TargetConnector | None = None
    active_stage = "extract"
    started = time.monotonic()
    stage_ms = {"extract": 0.0, "validate": 0.0, "transform": 0.0, "load": 0.0}

    input_digest = RollingRowsDigest()
    output_digest = RollingRowsDigest()
    input_schema = SchemaTracker()
    output_schema = SchemaTracker()
    artifact: PreparedBatchTransform | None = None
    last_envelope: BatchEnvelope | None = None
    quality_columns: dict[str, dict[str, int]] = {}
    quality_warnings: set[str] = set()
    destructive_seen: set[tuple[str, str]] = set()
    recovered_rows_in = 0
    recovered_bytes_in = 0
    resume_offset = 0

    state["is_streaming"] = True
    state["raw_data"] = []
    state["transformed_data"] = []
    yield ("extract", "running", state)

    try:
        source = _source_connector(config, state)
        source.connect()
        if not dry_run:
            target = get_staged_target_connector(
                config.target,
                run_id=state["run_id"],
            )
            target.connect()
        if config.execution.quarantine_path:
            quarantine = get_target_connector(
                JsonTargetConfig(
                    type="json",
                    path=config.execution.quarantine_path,
                    write_mode="overwrite",
                )
            )
            quarantine.connect()

        source_iterator = iter(source.stream(config.chunk_size))
        batch_index = 0
        source_offset = 0

        if recovery is not None and not dry_run:
            commits = recovery.restore(state["run_id"], _PARTITION_ID)
            for commit in commits:
                recovered_rows = recovery.read_rows(commit)
                if target is not None:
                    _write_target(target, recovered_rows)
                output_digest.update(recovered_rows)
                if recovered_rows:
                    output_schema.apply(recovered_rows, config.execution.schema_drift)
                recovered_rows_in += commit.envelope.rows_in
                recovered_bytes_in += commit.envelope.bytes_in
                resume_offset = max(
                    resume_offset,
                    _position_offset(commit.checkpoint.source_position) + 1,
                )
                batch_index += 1
                last_envelope = commit.envelope
            if commits:
                source_offset = resume_offset
                state["rows_extracted"] = recovered_rows_in
                state["rows_transformed"] = output_digest.rows
                state["rows_loaded"] = output_digest.rows
                state["batches_completed"] = batch_index
                state["bytes_in"] = recovered_bytes_in
                state["bytes_out"] = output_digest.bytes
                state["output_checksum"] = output_digest.checksum
                state["last_batch_envelope"] = last_envelope
                state.setdefault("warnings", []).append(
                    f"recovered {len(commits)} committed batch(es) from durable artifacts"
                )
                yield ("recovery", "done", state)

        rows_to_skip = resume_offset

        while True:
            _raise_if_cancelled(cancellation, state["run_id"])
            read_started = time.monotonic()
            try:
                raw_rows = next(source_iterator)
            except StopIteration:
                stage_ms["extract"] += (time.monotonic() - read_started) * 1000
                break
            stage_ms["extract"] += (time.monotonic() - read_started) * 1000

            raw_rows = _filter_incremental_rows(config, state, raw_rows)
            if rows_to_skip:
                skipped = min(rows_to_skip, len(raw_rows))
                raw_rows = raw_rows[skipped:]
                rows_to_skip -= skipped
            if not raw_rows:
                continue

            batch_index += 1
            batch_started = time.monotonic()
            batch_input_digest = RollingRowsDigest()
            batch_input_digest.update(raw_rows)
            input_digest.update(raw_rows)
            state["rows_extracted"] = input_digest.rows
            _advance_cursor(config, state, raw_rows)

            active_stage = "validate"
            validate_started = time.monotonic()
            schema_result = input_schema.apply(raw_rows, config.execution.schema_drift)
            validation_result = validate_batch(schema_result.rows, config.validation)
            rejected = [*schema_result.rejected, *validation_result.rejected]
            _merge_quality_counts(quality_columns, validation_result.column_counts)
            quality_warnings.update(validation_result.warnings)
            stage_ms["validate"] += (time.monotonic() - validate_started) * 1000

            accepted_rows = validation_result.rows
            if state.get("schema_sample") == {} and accepted_rows:
                state["schema_sample"] = build_schema_sample(
                    accepted_rows,
                    max_sample_rows=5,
                )
            state["schema_version"] = schema_result.version

            if rejected:
                _write_rejections(
                    quarantine,
                    rejected,
                    run_id=state["run_id"],
                    batch_id=f"batch-{batch_index:08d}",
                )
                state["rows_rejected"] = state.get("rows_rejected", 0) + len(rejected)

            active_stage = "transform"
            transform_started = time.monotonic()
            if artifact is None and accepted_rows:
                artifact = prepare_transform_artifact(state, accepted_rows[:5])
                state["transform_artifact_version"] = artifact.version
                state["last_error"] = None
            transformed = (
                transform_batch(artifact, accepted_rows, state)
                if artifact is not None and accepted_rows
                else []
            )
            transformed, output_rejected = _apply_output_schema(
                output_schema,
                transformed,
                config.execution.schema_drift,
            )
            if output_rejected:
                _write_rejections(
                    quarantine,
                    output_rejected,
                    run_id=state["run_id"],
                    batch_id=f"batch-{batch_index:08d}",
                )
                state["rows_rejected"] = state.get("rows_rejected", 0) + len(output_rejected)
            batch_filtered = max(
                0,
                len(accepted_rows) - len(transformed) - len(output_rejected),
            )
            state["rows_filtered"] = state.get("rows_filtered", 0) + batch_filtered

            warnings = detect_destructive_operations(
                {"raw_data": accepted_rows},
                {"transformed_data": transformed},
                state.get("destructive_filter_threshold", 0.3),
            )
            raise_if_destructive(warnings, state.get("auto_confirmed", False))
            for warning in warnings:
                key = (warning.reason.value, warning.message)
                if key not in destructive_seen:
                    state.setdefault("destructive_warnings", []).append(warning)
                    destructive_seen.add(key)
            stage_ms["transform"] += (time.monotonic() - transform_started) * 1000

            _raise_if_cancelled(cancellation, state["run_id"])
            batch_output_digest = RollingRowsDigest()
            batch_output_digest.update(transformed)

            source_start = source_offset
            source_offset += len(raw_rows)
            last_envelope = BatchEnvelope(
                run_id=state["run_id"],
                stage_id="load",
                partition_id=_PARTITION_ID,
                batch_id=f"batch-{batch_index:08d}",
                attempt=0,
                source_position_start={"offset": source_start},
                source_position_end={"offset": source_offset - 1},
                schema_version=schema_result.version,
                transform_artifact_version=artifact.version if artifact is not None else None,
                rows_in=len(raw_rows),
                rows_out=len(transformed),
                rows_rejected=len(rejected) + len(output_rejected),
                rows_filtered=batch_filtered,
                bytes_in=batch_input_digest.bytes,
                bytes_out=batch_output_digest.bytes,
                checksum=batch_output_digest.checksum,
                input_checksum=batch_input_digest.checksum,
                output_checksum=batch_output_digest.checksum,
                duration_ms=(time.monotonic() - batch_started) * 1000,
            )
            if recovery is not None and not dry_run:
                recovery.commit(last_envelope, transformed)

            active_stage = "load"
            load_started = time.monotonic()
            written = len(transformed) if dry_run else _write_target(target, transformed)
            stage_ms["load"] += (time.monotonic() - load_started) * 1000

            output_digest.update(transformed)
            state["rows_extracted"] = recovered_rows_in + input_digest.rows
            state["rows_transformed"] = output_digest.rows
            state["rows_loaded"] = state.get("rows_loaded", 0) + written
            state["batches_completed"] = batch_index
            state["bytes_in"] = recovered_bytes_in + input_digest.bytes
            state["bytes_out"] = output_digest.bytes
            state["input_checksum"] = input_digest.checksum if not recovered_rows_in else None
            state["output_checksum"] = output_digest.checksum
            state["validation_passed"] = True
            state["last_batch_envelope"] = last_envelope
            state["raw_data"] = []
            state["transformed_data"] = []
            yield ("batch", "done", state)

        if recovered_rows_in + input_digest.rows == 0:
            raise ValidationError("Source returned 0 rows — nothing to validate")

        state["validation_report"] = {
            "rows_checked": recovered_rows_in + input_digest.rows,
            "rows_rejected": state.get("rows_rejected", 0),
            "columns": quality_columns,
            "hard_failures": [],
            "soft_warnings": sorted(quality_warnings),
        }
        for warning in sorted(quality_warnings):
            if warning not in state.setdefault("warnings", []):
                state["warnings"].append(warning)

        diagnostics = source.diagnostics()
        for diagnostic in diagnostics:
            if diagnostic not in state.setdefault("warnings", []):
                state["warnings"].append(diagnostic)

        _raise_if_cancelled(cancellation, state["run_id"])
        active_stage = "load"
        if quarantine is not None:
            quarantine.finalize()
        if target is not None:
            target.finalize()
            state["target_published"] = True

        if (
            recovery is None
            and checkpoints is not None
            and last_envelope is not None
            and not dry_run
        ):
            checkpoints.save(
                Checkpoint(
                    checkpoint_id=uuid.uuid4().hex,
                    run_id=state["run_id"],
                    partition_id=_PARTITION_ID,
                    batch_id=last_envelope.batch_id,
                    source_position=last_envelope.source_position_end,
                )
            )

        state["duration_ms"].update(stage_ms)
        state["duration_ms"]["total"] = (time.monotonic() - started) * 1000
        yield ("extract", "done", state)
        yield ("validate", "done", state)
        yield ("transform", "done", state)
        yield ("load", "skipped" if dry_run else "done", state)
    except Exception as exc:
        state["last_error"] = str(exc)
        state["duration_ms"].update(stage_ms)
        state["duration_ms"]["total"] = (time.monotonic() - started) * 1000
        yield (active_stage, "failed", state)
        if isinstance(exc, PipelineError):
            raise
        raise PipelineError(f"Pipeline failed (run_id={state['run_id']}): {exc}") from exc
    finally:
        if quarantine is not None:
            quarantine.disconnect()
        if target is not None:
            target.disconnect()
        if source is not None:
            source.disconnect()


def _position_offset(position: object) -> int:
    if isinstance(position, dict):
        value = position.get("offset")
        if isinstance(value, int) and value >= 0:
            return value
    raise PipelineError(f"unsupported durable source position: {position!r}")


def _source_connector(config: PipelineConfig, state: PipelineState) -> SourceConnector:
    incremental = config.incremental
    if incremental is None or config.source.type not in _INCREMENTAL_PUSHDOWN_SOURCES:
        if incremental is not None:
            state.setdefault("warnings", []).append(
                f"Incremental filtering for source type '{config.source.type}' is applied "
                "client-side and scans the source"
            )
        return get_source_connector(config.source)
    return get_source_connector(
        config.source,
        incremental_column=incremental.column,
        incremental_param=incremental.param or incremental.column,
        cursor_value=state.get("cursor_value"),
    )


def _filter_incremental_rows(
    config: PipelineConfig,
    state: PipelineState,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    incremental = config.incremental
    if incremental is None or config.source.type in _INCREMENTAL_PUSHDOWN_SOURCES:
        return rows
    from loafer.core.incremental import filter_rows_after_cursor

    return filter_rows_after_cursor(
        rows,
        incremental.column,
        state.get("cursor_value"),
    )


def _advance_cursor(
    config: PipelineConfig,
    state: PipelineState,
    rows: list[dict[str, Any]],
) -> None:
    if config.incremental is None:
        return
    from loafer.core.incremental import max_cursor

    state["new_cursor"] = max_cursor(
        rows,
        config.incremental.column,
        state.get("new_cursor"),
    )


def _apply_output_schema(
    tracker: SchemaTracker,
    rows: list[dict[str, Any]],
    policy: str,
) -> tuple[list[dict[str, Any]], list[RejectedRow]]:
    if not rows:
        return [], []
    result = tracker.apply(rows, policy)
    rejected = [
        RejectedRow(row=item.row, stage="transform_output", reason=item.reason)
        for item in result.rejected
    ]
    return result.rows, rejected


def _write_target(
    target: TargetConnector | None,
    rows: list[dict[str, Any]],
) -> int:
    if target is None:
        raise PipelineError("target connector is unavailable")
    return target.write_chunk(rows)


def _write_rejections(
    quarantine: TargetConnector | None,
    rejected: list[RejectedRow],
    *,
    run_id: str,
    batch_id: str,
) -> None:
    if not rejected:
        return
    if quarantine is None:
        first = rejected[0]
        raise ValidationError(
            f"{len(rejected)} rows require quarantine but no quarantine target is configured; "
            f"first rejection: {first.reason}"
        )
    quarantine.write_chunk(
        [
            {
                "_loafer": {
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "stage": item.stage,
                    "reason": item.reason,
                },
                "row": item.row,
            }
            for item in rejected
        ]
    )


def _merge_quality_counts(
    aggregate: dict[str, dict[str, int]],
    batch: dict[str, dict[str, int]],
) -> None:
    for column, counts in batch.items():
        target = aggregate.setdefault(column, {"total_count": 0, "null_count": 0})
        target["total_count"] += counts["total_count"]
        target["null_count"] += counts["null_count"]


def _raise_if_cancelled(
    cancellation: CancellationPort | None,
    run_id: str,
) -> None:
    if cancellation is not None and cancellation.is_cancelled(run_id):
        raise PipelineError(f"Pipeline cancelled (run_id={run_id})")


def rows_size(rows: list[dict[str, Any]]) -> int:
    """Return canonical serialized byte size for tests and diagnostics."""
    return sum(len(canonical_row_bytes(row)) for row in rows)


__all__ = ["rows_size", "stream_bounded_pipeline", "uses_bounded_data_plane"]
