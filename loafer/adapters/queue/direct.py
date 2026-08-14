"""No-broker job transport backed directly by authoritative metadata.

Local and embedded installations run without NATS. Rather than special-casing
the worker, this adapter presents the runnable set as a queue: it peeks at
dispatchable runs and lets the caller claim them through the same fenced path
transport delivery uses. Losing a race therefore looks exactly like a
duplicate delivery, which is already a no-op acknowledgement.

Delivery counting is deliberately absent here. Without a broker there is no
redelivery record, so poison detection falls back to the run's own attempt
counter rather than a transport-level count.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from loafer.contracts import JobEnvelope
from loafer.core.roles import WorkerRole
from loafer.ports.metadata import MetadataStore


@dataclass
class DirectDeliveredJob:
    """One peeked run; acknowledgement is a no-op because nothing was reserved."""

    envelope: JobEnvelope
    delivery_count: int = 0
    _naked: list[str] = field(default_factory=list, repr=False)

    def ack(self) -> None:
        return None

    def nak(self, delay_seconds: float) -> None:
        del delay_seconds
        self._naked.append(self.envelope.run_id)

    def term(self) -> None:
        return None


class DirectQueuePublisher:
    """Discard publications; the run row is already the durable record."""

    def publish(self, envelope: JobEnvelope, *, dedupe_key: str) -> None:
        del envelope, dedupe_key

    def close(self) -> None:
        return None


class DirectQueueConsumer:
    """Present one role's runnable set as a queue over authoritative metadata."""

    def __init__(self, metadata: MetadataStore, role: WorkerRole) -> None:
        self._metadata = metadata
        self._role = role

    def fetch(self, max_messages: int, timeout_seconds: float) -> list[DirectDeliveredJob]:
        del timeout_seconds
        return [
            DirectDeliveredJob(envelope=envelope)
            for envelope in self._metadata.list_runnable(role=self._role, limit=max_messages)
        ]

    def close(self) -> None:
        return None


__all__ = [
    "DirectDeliveredJob",
    "DirectQueueConsumer",
    "DirectQueuePublisher",
]
