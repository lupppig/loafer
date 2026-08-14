"""Job transport seam for role-isolated worker pools.

The transport dispatches work; it never records what is true. Authoritative
run state stays in the metadata store, so a lost, duplicated, or replayed
message costs at most redundant effort, never correctness.

These protocols are synchronous because the engine, worker, and application
service are synchronous. An adapter over an async client owns its own event
loop rather than propagating one across the boundary.
"""

from __future__ import annotations

from typing import Protocol

from loafer.contracts import JobEnvelope


class DeliveredJob(Protocol):
    """One delivered job whose acknowledgement the caller drives explicitly."""

    envelope: JobEnvelope
    delivery_count: int

    def ack(self) -> None:
        """Confirm the run reached a durable terminal or retry-wait state."""

    def nak(self, delay_seconds: float) -> None:
        """Release the job for redelivery after a delay."""

    def term(self) -> None:
        """Stop redelivery permanently after the caller quarantined the run."""


class QueuePublisher(Protocol):
    """Publish opaque job identifiers onto a role's subject."""

    def publish(self, envelope: JobEnvelope, *, dedupe_key: str) -> None:
        """Publish one job, deduplicated by key at the transport."""

    def close(self) -> None:
        """Release transport resources owned by this publisher."""


class QueueConsumer(Protocol):
    """Fetch jobs for exactly one role under explicit acknowledgement."""

    def fetch(self, max_messages: int, timeout_seconds: float) -> list[DeliveredJob]:
        """Return up to max_messages jobs, or an empty list when none arrive."""

    def close(self) -> None:
        """Release transport resources owned by this consumer."""
