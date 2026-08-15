"""Local application service and pipeline use cases."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loafer.config import PipelineConfig, load_config
from loafer.contracts import (
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
from loafer.engine import ProviderFactory, stream_pipeline
from loafer.exceptions import PipelineError
from loafer.graph.state import PipelineState
from loafer.ports.metadata import BatchRecoveryPort
from loafer.ports.runtime import (
    CancellationPort,
    CheckpointPort,
    EventPublisher,
    ReviewPort,
    SecretResolver,
)

_URL_PASSWORD = re.compile(r"(?P<prefix>[a-z][a-z0-9+.-]*://[^:/@\s]+):[^@\s]+@")


def _json_value(value: Any) -> Any:
    """Convert config metadata to a JSON-compatible value."""
    return json.loads(json.dumps(value, default=str))


def _redact(text: str | None, config: PipelineConfig) -> str | None:
    """Remove configured keys and URL passwords from durable messages."""
    if text is None:
        return None
    redacted = _URL_PASSWORD.sub(r"\g<prefix>:***@", text)
    if config.llm.api_key:
        redacted = redacted.replace(config.llm.api_key, "***")
    return redacted


def _config_digest(config: PipelineConfig) -> str:
    rendered = json.dumps(
        config.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def _delivery_guarantee(config: PipelineConfig) -> str:
    if config.execution.transform_class != "row_local":
        return "target_defined"
    if config.target.type in {"csv", "json"}:
        return "atomic_run_publication"
    if config.target.type == "postgres":
        if config.target.write_mode == "replace":
            return "atomic_transactional_replace"
        if config.target.write_mode == "error":
            return "atomic_transactional_create_once"
        if config.target.write_mode == "upsert":
            return "idempotent_keyed_atomic_merge"
        return "at_least_once_atomic_merge"
    return "unsupported"


def _build_plan(
    config: PipelineConfig,
    request: RunRequest,
) -> ExecutionPlan:
    digest = _config_digest(config)
    resolved_path = str(Path(request.config_path).resolve())
    plan_id = hashlib.sha256(f"{resolved_path}:{digest}".encode()).hexdigest()[:16]
    cursor_value: Any = None

    if config.incremental is not None:
        cursor_value = config.incremental.initial
        if not request.full_refresh:
            from loafer.core.incremental import StateStore, state_path_for

            stored = StateStore(state_path_for(resolved_path)).get_cursor(
                config.name or Path(resolved_path).stem
            )
            if stored is not None:
                cursor_value = stored

    return ExecutionPlan(
        plan_id=plan_id,
        config_digest=digest,
        config_path=resolved_path,
        pipeline_name=config.name or Path(resolved_path).stem,
        mode=config.mode,
        source_type=config.source.type,
        target_type=config.target.type,
        transform_type=config.transform.type,
        transform_class=config.execution.transform_class,
        chunk_size=config.chunk_size,
        streaming_threshold=config.streaming_threshold,
        validation_strict=config.validation.strict,
        schema_drift_policy=config.execution.schema_drift,
        delivery_guarantee=_delivery_guarantee(config),
        llm_provider=config.llm.provider,
        llm_model=config.llm.model,
        incremental_column=config.incremental.column if config.incremental else None,
        cursor_value=_json_value(cursor_value),
        dry_run=request.dry_run,
        auto_confirm=request.auto_confirm,
        full_refresh=request.full_refresh,
    )


def _step_result(value: Any, config: PipelineConfig) -> TransformStepResult:
    if isinstance(value, dict):
        data = dict(value)
    elif is_dataclass(value):
        data = asdict(value)
    else:
        data = {
            name: getattr(value, name)
            for name in (
                "index",
                "name",
                "type",
                "rows_in",
                "rows_out",
                "duration_ms",
                "success",
                "error",
                "token_usage",
            )
        }
    data["error"] = _redact(data.get("error"), config)
    data["token_usage"] = data.get("token_usage") or {}
    return TransformStepResult.model_validate(data)


def _snapshot(
    state: PipelineState,
    plan: ExecutionPlan,
    config: PipelineConfig,
) -> RunSnapshot:
    warnings = tuple(_redact(str(item), config) or "" for item in state.get("warnings", []))
    durations = {
        str(name): max(0.0, float(value)) for name, value in state.get("duration_ms", {}).items()
    }
    token_usage = {str(name): int(value) for name, value in state.get("token_usage", {}).items()}
    steps = tuple(_step_result(item, config) for item in state.get("step_results", []))

    return RunSnapshot(
        run_id=state.get("run_id", ""),
        plan_id=plan.plan_id,
        pipeline_name=plan.pipeline_name,
        mode=plan.mode,
        source_type=plan.source_type,
        target_type=plan.target_type,
        transform_type=plan.transform_type,
        rows_extracted=max(0, int(state.get("rows_extracted", 0))),
        rows_transformed=max(
            0,
            int(
                state.get("rows_transformed", 0)
                if state.get("batches_completed", 0)
                else len(state.get("transformed_data", []))
            ),
        ),
        rows_loaded=max(0, int(state.get("rows_loaded", 0))),
        rows_rejected=max(0, int(state.get("rows_rejected", 0))),
        rows_filtered=max(0, int(state.get("rows_filtered", 0))),
        batches_completed=max(0, int(state.get("batches_completed", 0))),
        bytes_in=max(0, int(state.get("bytes_in", 0))),
        bytes_out=max(0, int(state.get("bytes_out", 0))),
        input_checksum=state.get("input_checksum"),
        output_checksum=state.get("output_checksum"),
        schema_version=state.get("schema_version"),
        transform_artifact_version=state.get("transform_artifact_version"),
        validation_passed=bool(state.get("validation_passed", False)),
        duration_ms=durations,
        warnings=warnings,
        token_usage=token_usage,
        step_results=steps,
        error=_redact(state.get("last_error"), config),
    )


class RunPipeline:
    """Application use case for planning and executing one pipeline."""

    def __init__(
        self,
        *,
        cancellation: CancellationPort,
        checkpoints: CheckpointPort,
        secrets: SecretResolver,
        events: EventPublisher,
        reviewer: ReviewPort,
        provider_factory: ProviderFactory | None = None,
        recovery: BatchRecoveryPort | None = None,
    ) -> None:
        self._cancellation = cancellation
        self._checkpoints = checkpoints
        self._secrets = secrets
        self._events = events
        self._reviewer = reviewer
        self._provider_factory = provider_factory
        self._recovery = recovery

    def create_plan(self, request: RunRequest) -> ExecutionPlan:
        """Validate a config and return its credential-free execution plan."""
        config = self._load_config(request.config_path)
        return _build_plan(config, request)

    def validate_config_model(self, config_path: str | Path) -> PipelineConfig:
        """Return the validated runtime model for legacy local callers."""
        return self._load_config(config_path)

    def run(self, request: RunRequest) -> RunResult:
        """Execute a pipeline and return only its durable result."""
        started_at = datetime.now(UTC)
        config, plan = self._prepare(request)
        state, output_published = self._consume(config, plan, request)
        return RunResult(
            run_id=request.run_id,
            plan_id=plan.plan_id,
            status=RunStatus.SUCCEEDED,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            output_published=output_published,
            snapshot=_snapshot(state, plan, config),
        )

    def run_state(self, request: RunRequest) -> PipelineState:
        """Execute through the use case while returning legacy runtime state."""
        config, plan = self._prepare(request)
        state, _output_published = self._consume(config, plan, request)
        return state

    def stream(self, request: RunRequest) -> Iterator[RunEvent]:
        """Yield sanitized, serializable application events."""
        for event, _state in self.stream_states(request):
            yield event

    def stream_states(
        self,
        request: RunRequest,
    ) -> Iterator[tuple[RunEvent, PipelineState]]:
        """Yield events plus ephemeral state for the compatibility facade."""
        config, plan = self._prepare(request)
        yield from self._stream_prepared(config, plan, request)

    def _consume(
        self,
        config: PipelineConfig,
        plan: ExecutionPlan,
        request: RunRequest,
    ) -> tuple[PipelineState, bool]:
        final_state: PipelineState | None = None
        output_published = False
        for event, state in self._stream_prepared(config, plan, request):
            final_state = state
            if event.status is StageStatus.DONE and event.stage in {
                "load",
                "transform_in_target",
            }:
                output_published = not request.dry_run
        if final_state is None:
            raise PipelineError(f"Pipeline produced no stages (run_id={request.run_id})")
        return final_state, output_published

    def _stream_prepared(
        self,
        config: PipelineConfig,
        plan: ExecutionPlan,
        request: RunRequest,
    ) -> Iterator[tuple[RunEvent, PipelineState]]:
        updates = stream_pipeline(
            config,
            config_path=plan.config_path,
            dry_run=request.dry_run,
            auto_confirm=request.auto_confirm,
            full_refresh=request.full_refresh,
            run_id=request.run_id,
            reviewer=self._reviewer,
            secret_resolver=self._secrets,
            provider_factory=self._provider_factory,
            cancellation=self._cancellation,
            checkpoints=self._checkpoints,
            recovery=self._recovery,
        )
        sequence = 0

        while True:
            if plan.transform_class != "row_local" and self._cancellation.is_cancelled(
                request.run_id
            ):
                raise PipelineError(f"Pipeline cancelled (run_id={request.run_id})")
            try:
                stage, status, state = next(updates)
            except StopIteration:
                return

            sequence += 1
            event = RunEvent(
                run_id=request.run_id,
                plan_id=plan.plan_id,
                sequence=sequence,
                stage=stage,
                status=StageStatus(status),
                snapshot=_snapshot(state, plan, config),
                batch=state.get("last_batch_envelope") if stage == "batch" else None,
            )
            self._events.publish(event)
            yield event, state

    def _prepare(self, request: RunRequest) -> tuple[PipelineConfig, ExecutionPlan]:
        config = self._load_config(request.config_path)
        return config, _build_plan(config, request)

    def _load_config(self, config_path: str | Path) -> PipelineConfig:
        try:
            return load_config(config_path, secret_resolver=self._secrets.resolve)
        except Exception as exc:
            raise PipelineError(f"Config validation failed: {exc}") from exc


class LocalApplicationService:
    """Application boundary used by the local CLI, scheduler, and Python API."""

    def __init__(self, run_pipeline: RunPipeline) -> None:
        self.run_pipeline = run_pipeline

    def validate(self, config_path: str | Path) -> ValidationResult:
        request = RunRequest(config_path=str(config_path))
        return ValidationResult(plan=self.run_pipeline.create_plan(request))

    def validate_config_model(self, config_path: str | Path) -> PipelineConfig:
        return self.run_pipeline.validate_config_model(config_path)

    def list_connectors(self) -> ConnectorCatalog:
        from loafer.connectors.registry import list_registered_connectors

        sources, targets = list_registered_connectors()
        return ConnectorCatalog(sources=tuple(sources), targets=tuple(targets))
