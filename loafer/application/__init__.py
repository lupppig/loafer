"""Versioned application boundary for Loafer clients."""

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
    "get_local_application",
]
