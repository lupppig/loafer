"""Runtime ports shared by the engine and application service."""

from __future__ import annotations

from typing import Protocol

from loafer.contracts import Checkpoint, RunEvent


class CancellationPort(Protocol):
    """Report whether a run should stop at the next safe boundary."""

    def is_cancelled(self, run_id: str) -> bool:
        """Return true when cancellation has been requested."""


class CheckpointPort(Protocol):
    """Load and commit durable batch checkpoints."""

    def load(self, run_id: str, partition_id: str) -> Checkpoint | None:
        """Return the latest committed checkpoint for a partition."""

    def save(self, checkpoint: Checkpoint) -> None:
        """Persist a checkpoint after its target effect is durable."""


class SecretResolver(Protocol):
    """Resolve a server-side secret reference without exposing it to clients."""

    def resolve(self, reference: str) -> str | None:
        """Return a secret value, or ``None`` when the reference is absent."""


class EventPublisher(Protocol):
    """Publish sanitized application events."""

    def publish(self, event: RunEvent) -> None:
        """Publish one monotonically sequenced run event."""


class ReviewPort(Protocol):
    """Approve or reject generated transform code before execution."""

    def approve_transform(self, generated_code: str) -> bool:
        """Return true only when the candidate may execute."""
