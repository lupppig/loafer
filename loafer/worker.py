"""Single-node durable worker process."""

from __future__ import annotations

import json
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

from loafer.adapters.runtime import (
    DurableBatchRecovery,
    EnvironmentSecretResolver,
    MetadataCancellation,
    NullCheckpointStore,
    NullEventPublisher,
)
from loafer.application.service import RunPipeline
from loafer.contracts import RunEvent, RunRequest, StageStatus
from loafer.core.run_state import RetryCategory, RunState, StageState
from loafer.exceptions import PipelineError
from loafer.metadata import RunLease
from loafer.ports.metadata import MetadataStore
from loafer.ports.object_storage import ObjectStoragePort


class RejectUnapprovedTransform:
    """Workers never grant interactive approval to newly generated code."""

    def approve_transform(self, generated_code: str) -> bool:
        del generated_code
        return False


class DurableWorker:
    """Claim and execute immutable runs under leases and fencing tokens."""

    def __init__(
        self,
        metadata: MetadataStore,
        objects: ObjectStoragePort,
        *,
        worker_id: str,
        lease_for: timedelta = timedelta(seconds=30),
        retry_delay: timedelta = timedelta(seconds=5),
        max_attempts: int = 3,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._metadata = metadata
        self._objects = objects
        self._worker_id = worker_id
        self._lease_for = lease_for
        self._retry_delay = retry_delay
        self._max_attempts = max_attempts

    def run_once(self) -> str | None:
        """Execute at most one runnable job, returning its run ID."""
        lease = self._metadata.claim_run(self._worker_id, self._lease_for)
        if lease is None:
            return None
        self.execute(lease)
        return lease.run.id

    def execute(self, lease: RunLease) -> None:
        """Execute one claimed run and persist all observable outcomes."""
        self._metadata.transition_run(lease, RunState.RUNNING)
        version = self._metadata.get_pipeline_version(lease.run.pipeline_version_id)
        config_document = version.config.get("document", version.config)
        recovery = DurableBatchRecovery(self._metadata, self._objects, lease)
        use_case = RunPipeline(
            cancellation=MetadataCancellation(self._metadata),
            checkpoints=NullCheckpointStore(),
            secrets=EnvironmentSecretResolver(),
            events=NullEventPublisher(),
            reviewer=RejectUnapprovedTransform(),
            recovery=recovery,
        )

        try:
            with tempfile.TemporaryDirectory(prefix="loafer-run-") as directory:
                config_path = Path(directory) / "pipeline.json"
                config_path.write_text(json.dumps(config_document), encoding="utf-8")
                request = RunRequest(config_path=str(config_path), run_id=lease.run.id)
                for engine_event in use_case.stream(request):
                    lease = self._metadata.heartbeat(lease, self._lease_for)
                    self._record_engine_event(lease, engine_event)
            self._metadata.transition_run(lease, RunState.SUCCEEDED)
        except Exception as exc:
            cancelled = self._metadata.cancellation_requested(lease.run.id)
            if cancelled:
                self._metadata.transition_run(
                    lease,
                    RunState.CANCELLED,
                    error={"type": type(exc).__name__, "message": str(exc)},
                )
                return
            if lease.run.attempt + 1 < self._max_attempts:
                checkpoint = self._metadata.latest_checkpoint(lease.run.id, "default")
                category = (
                    RetryCategory.FAILED_BATCH
                    if checkpoint is not None
                    else RetryCategory.INFRASTRUCTURE
                )
                self._metadata.transition_run(
                    lease,
                    RunState.RETRY_WAIT,
                    error={"type": type(exc).__name__, "message": str(exc)},
                    retry_category=category,
                    retry_at=_utc_after(self._retry_delay),
                )
                return
            self._metadata.transition_run(
                lease,
                RunState.FAILED,
                error={"type": type(exc).__name__, "message": str(exc)},
            )
            if isinstance(exc, PipelineError):
                return
            raise

    def run_forever(self, poll_interval: float = 1.0) -> None:
        """Poll for durable commands until the process is interrupted."""
        try:
            while True:
                if self.run_once() is None:
                    time.sleep(poll_interval)
        except (KeyboardInterrupt, SystemExit):
            return

    def close(self) -> None:
        """Release adapter resources owned by this worker composition."""
        close = getattr(self._metadata, "close", None)
        if close is not None:
            close()

    def _record_engine_event(self, lease: RunLease, event: RunEvent) -> None:
        if event.stage not in {"batch", "recovery"}:
            if event.status in {
                StageStatus.DONE,
                StageStatus.FAILED,
                StageStatus.CANCELLED,
            }:
                self._metadata.transition_stage(
                    lease,
                    event.stage,
                    StageState.RUNNING,
                )
            self._metadata.transition_stage(
                lease,
                event.stage,
                _stage_state(event.status),
            )
        self._metadata.append_event(
            lease,
            f"engine.{event.stage}.{event.status.value}",
            event.model_dump(mode="json", exclude={"sequence"}),
        )


def _stage_state(status: StageStatus) -> StageState:
    return {
        StageStatus.RUNNING: StageState.RUNNING,
        StageStatus.DONE: StageState.SUCCEEDED,
        StageStatus.SKIPPED: StageState.SKIPPED,
        StageStatus.FAILED: StageState.FAILED,
        StageStatus.CANCELLED: StageState.CANCELLED,
    }[status]


def _utc_after(delta: timedelta) -> Any:
    from loafer.metadata import utc_now

    return utc_now() + delta
