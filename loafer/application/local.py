"""Local composition root for the application service."""

from __future__ import annotations

from loafer.adapters.runtime import (
    EnvironmentSecretResolver,
    InputReviewPort,
    NeverCancelled,
    NullCheckpointStore,
    NullEventPublisher,
)
from loafer.application.service import LocalApplicationService, RunPipeline
from loafer.engine import ProviderFactory
from loafer.ports.runtime import ReviewPort


def get_local_application(
    *,
    reviewer: ReviewPort | None = None,
    provider_factory: ProviderFactory | None = None,
) -> LocalApplicationService:
    """Build the synchronous local application service."""
    return LocalApplicationService(
        RunPipeline(
            cancellation=NeverCancelled(),
            checkpoints=NullCheckpointStore(),
            secrets=EnvironmentSecretResolver(),
            events=NullEventPublisher(),
            reviewer=reviewer or InputReviewPort(),
            provider_factory=provider_factory,
        )
    )
