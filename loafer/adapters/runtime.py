"""Local adapters for application runtime ports."""

from __future__ import annotations

import os

from loafer.contracts import Checkpoint, RunEvent


class NeverCancelled:
    """Default cancellation adapter for synchronous local execution."""

    def is_cancelled(self, run_id: str) -> bool:
        del run_id
        return False


class NullCheckpointStore:
    """No-op checkpoint adapter until Phase 3 adds durable metadata."""

    def load(self, run_id: str, partition_id: str) -> Checkpoint | None:
        del run_id, partition_id
        return None

    def save(self, checkpoint: Checkpoint) -> None:
        del checkpoint


class EnvironmentSecretResolver:
    """Resolve local secret references from environment variables."""

    def resolve(self, reference: str) -> str | None:
        return os.environ.get(reference)


class NullEventPublisher:
    """Discard events for callers that consume the returned iterator."""

    def publish(self, event: RunEvent) -> None:
        del event


class InputReviewPort:
    """Portable stdin reviewer used by the local Python API."""

    def approve_transform(self, generated_code: str) -> bool:
        print("\nAI-generated transform code:\n")
        print(generated_code)
        try:
            answer = input("Execute this code? [y/N]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            return False
        return answer in {"y", "yes"}
