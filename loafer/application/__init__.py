"""Versioned application boundary for Loafer clients."""

from typing import Any

from loafer.application.local import get_local_application
from loafer.application.service import LocalApplicationService, RunPipeline
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
    ValidationResult,
)
from loafer.ports.runtime import CancellationPort, CheckpointPort, SecretResolver


def enqueue_pipeline(*args: Any, **kwargs: Any) -> Any:
    """Lazily create a durable run without coupling package import to worker setup."""
    from loafer.application.durable import enqueue_pipeline as enqueue

    return enqueue(*args, **kwargs)


__all__ = [
    "BatchEnvelope",
    "CancellationPort",
    "Checkpoint",
    "CheckpointPort",
    "ConnectorCatalog",
    "ExecutionPlan",
    "LocalApplicationService",
    "RunEvent",
    "RunPipeline",
    "RunRequest",
    "RunResult",
    "RunSnapshot",
    "RunStatus",
    "SecretResolver",
    "StageStatus",
    "ValidationResult",
    "enqueue_pipeline",
    "get_local_application",
]
