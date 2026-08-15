"""Single-node durable worker process."""

from __future__ import annotations

import json
import re
import tempfile
import threading
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

from loafer.adapters.runtime import (
    DurableBatchRecovery,
    EnvironmentSecretResolver,
    MetadataCancellation,
    NullCheckpointStore,
    NullEventPublisher,
    ScopedSecretResolver,
)
from loafer.application.service import RunPipeline
from loafer.contracts import RunEvent, RunRequest, StageStatus
from loafer.core.roles import WorkerRole
from loafer.core.run_state import RetryCategory, RunState, StageState
from loafer.exceptions import MetadataError, PipelineError, StaleFenceError
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
        role: WorkerRole = WorkerRole.ETL,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._metadata = metadata
        self._objects = objects
        self._worker_id = worker_id
        self._lease_for = lease_for
        self._retry_delay = retry_delay
        self._max_attempts = max_attempts
        self._role = role
        self._shutdown = threading.Event()

    def run_once(self) -> str | None:
        """Execute at most one runnable job, returning its run ID."""
        lease = self._metadata.claim_run(self._worker_id, self._lease_for, role=self._role)
        if lease is None:
            return None
        self.execute(lease)
        return lease.run.id

    def execute(
        self,
        lease: RunLease,
        *,
        heartbeat_callback: Callable[[], None] | None = None,
    ) -> None:
        """Execute one claimed run and persist all observable outcomes."""
        self._metadata.transition_run(lease, RunState.RUNNING)
        try:
            with _LeaseKeeper(
                self._metadata,
                lease,
                self._lease_for,
                heartbeat_callback,
            ) as keeper:
                version = self._metadata.get_pipeline_version(lease.run.pipeline_version_id)
                config_document = version.config.get("document", version.config)
                allowed_secrets = _secret_references(config_document)
                provider = config_document.get("llm", {}).get("provider", "gemini")
                provider_reference = _PROVIDER_SECRET_REFERENCES.get(str(provider))
                if provider_reference is not None:
                    allowed_secrets.add(provider_reference)
                secrets = ScopedSecretResolver(
                    EnvironmentSecretResolver(),
                    allowed_secrets,
                    ttl_seconds=300,
                )
                recovery = DurableBatchRecovery(self._metadata, self._objects, lease)
                use_case = RunPipeline(
                    cancellation=MetadataCancellation(self._metadata),
                    checkpoints=NullCheckpointStore(),
                    secrets=secrets,
                    events=NullEventPublisher(),
                    reviewer=RejectUnapprovedTransform(),
                    recovery=recovery,
                )
                with tempfile.TemporaryDirectory(prefix="loafer-run-") as directory:
                    config_path = Path(directory) / "pipeline.json"
                    config_path.write_text(json.dumps(config_document), encoding="utf-8")
                    request = RunRequest(config_path=str(config_path), run_id=lease.run.id)
                    for engine_event in use_case.stream(request):
                        self._record_engine_event(keeper.current(), engine_event)
                lease = keeper.current()
            self._metadata.transition_run(lease, RunState.SUCCEEDED)
        except (MetadataError, StaleFenceError):
            # Once authority is unavailable or superseded, this worker must not
            # attempt another mutation with its old fence.
            raise
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
            while not self._shutdown.is_set():
                if self.run_once() is None:
                    self._shutdown.wait(poll_interval)
        except (KeyboardInterrupt, SystemExit):
            return

    def request_shutdown(self) -> None:
        """Stop polling before another run is claimed."""
        self._shutdown.set()

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


class _LeaseKeeper:
    """Renew one fence in the background and surface renewal failures."""

    def __init__(
        self,
        metadata: MetadataStore,
        lease: RunLease,
        lease_for: timedelta,
        callback: Callable[[], None] | None,
    ) -> None:
        self._metadata = metadata
        self._lease = lease
        self._lease_for = lease_for
        self._callback = callback
        self._interval = max(0.1, lease_for.total_seconds() / 3)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._error: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run,
            name=f"loafer-heartbeat-{lease.run.id}",
            daemon=True,
        )

    def __enter__(self) -> _LeaseKeeper:
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval + 1)

    def current(self) -> RunLease:
        with self._lock:
            if self._error is not None:
                raise self._error
            return self._lease

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                with self._lock:
                    lease = self._lease
                renewed = self._metadata.heartbeat(lease, self._lease_for)
                with self._lock:
                    self._lease = renewed
                if self._callback is not None:
                    self._callback()
            except BaseException as exc:
                with self._lock:
                    self._error = exc
                self._stop.set()
                return


_SECRET_REFERENCE_PATTERN = re.compile(r"\$\{([^}]+)}")
_PROVIDER_SECRET_REFERENCES = {
    "gemini": "GEMINI_API_KEY",
    "openai": "OPENAI_API_KEY",
    "claude": "ANTHROPIC_API_KEY",
    "qwen": "DASHSCOPE_API_KEY",
}


def _secret_references(value: object) -> set[str]:
    if isinstance(value, str):
        return set(_SECRET_REFERENCE_PATTERN.findall(value))
    if isinstance(value, dict):
        references: set[str] = set()
        for item in value.values():
            references.update(_secret_references(item))
        return references
    if isinstance(value, list):
        references = set()
        for item in value:
            references.update(_secret_references(item))
        return references
    return set()
