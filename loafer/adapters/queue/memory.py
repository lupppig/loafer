"""In-process job transport that reproduces at-least-once delivery.

This adapter exists so the delivery failure matrix — duplicate delivery, lost
acknowledgement, redelivery after an expired ack window, and poison
termination — runs deterministically without a broker. It models the
guarantees Loafer depends on rather than the ones a broker happens to provide,
so a test that passes here states something true about the protocol.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from loafer.contracts import JobEnvelope
from loafer.core.roles import WorkerRole


@dataclass
class _Record:
    envelope: JobEnvelope
    available_at: float
    delivery_count: int = 0
    inflight_until: float | None = None
    settled: bool = False


class MemoryQueueBroker:
    """Shared delivery state for in-process publishers and consumers."""

    def __init__(
        self,
        *,
        ack_wait_seconds: float = 30.0,
        clock: object = time.monotonic,
    ) -> None:
        self._ack_wait = ack_wait_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._records: dict[WorkerRole, list[_Record]] = {role: [] for role in WorkerRole}
        self._dedupe: set[str] = set()
        self.published: list[JobEnvelope] = []
        self.terminated: list[JobEnvelope] = []

    def now(self) -> float:
        return float(self._clock())  # type: ignore[operator]

    def publish(self, envelope: JobEnvelope, dedupe_key: str) -> None:
        with self._lock:
            if dedupe_key in self._dedupe:
                return
            self._dedupe.add(dedupe_key)
            self._records[envelope.role].append(_Record(envelope=envelope, available_at=self.now()))
            self.published.append(envelope)

    def redeliver(self, envelope: JobEnvelope) -> None:
        """Force a duplicate delivery, bypassing transport deduplication."""
        with self._lock:
            self._records[envelope.role].append(_Record(envelope=envelope, available_at=self.now()))

    def fetch(self, role: WorkerRole, max_messages: int) -> list[_Record]:
        with self._lock:
            now = self.now()
            ready: list[_Record] = []
            for record in self._records[role]:
                if len(ready) >= max_messages:
                    break
                if record.settled or record.available_at > now:
                    continue
                if record.inflight_until is not None and record.inflight_until > now:
                    continue
                record.delivery_count += 1
                record.inflight_until = now + self._ack_wait
                ready.append(record)
            return ready

    def ack(self, record: _Record) -> None:
        with self._lock:
            record.settled = True
            record.inflight_until = None

    def in_progress(self, record: _Record) -> None:
        with self._lock:
            if not record.settled:
                record.inflight_until = self.now() + self._ack_wait

    def nak(self, record: _Record, delay_seconds: float) -> None:
        with self._lock:
            record.inflight_until = None
            record.available_at = self.now() + delay_seconds

    def term(self, record: _Record) -> None:
        with self._lock:
            record.settled = True
            record.inflight_until = None
            self.terminated.append(record.envelope)

    def depth(self, role: WorkerRole) -> int:
        """Return how many records remain deliverable for a role."""
        with self._lock:
            return sum(1 for record in self._records[role] if not record.settled)


@dataclass
class MemoryDeliveredJob:
    """One in-process delivery with caller-driven acknowledgement."""

    envelope: JobEnvelope
    delivery_count: int
    _broker: MemoryQueueBroker = field(repr=False)
    _record: _Record = field(repr=False)

    def in_progress(self) -> None:
        self._broker.in_progress(self._record)

    def ack(self) -> None:
        self._broker.ack(self._record)

    def nak(self, delay_seconds: float) -> None:
        self._broker.nak(self._record, delay_seconds)

    def term(self) -> None:
        self._broker.term(self._record)


class MemoryQueuePublisher:
    """Publish job identities into a shared in-process broker."""

    def __init__(self, broker: MemoryQueueBroker) -> None:
        self._broker = broker

    def publish(self, envelope: JobEnvelope, *, dedupe_key: str) -> None:
        self._broker.publish(envelope, dedupe_key)

    def close(self) -> None:
        return None


class MemoryQueueConsumer:
    """Consume one role's job identities from a shared in-process broker."""

    def __init__(self, broker: MemoryQueueBroker, role: WorkerRole) -> None:
        self._broker = broker
        self._role = role

    def fetch(self, max_messages: int, timeout_seconds: float) -> list[MemoryDeliveredJob]:
        del timeout_seconds
        return [
            MemoryDeliveredJob(
                envelope=record.envelope,
                delivery_count=record.delivery_count,
                _broker=self._broker,
                _record=record,
            )
            for record in self._broker.fetch(self._role, max_messages)
        ]

    def close(self) -> None:
        return None


__all__ = [
    "MemoryDeliveredJob",
    "MemoryQueueBroker",
    "MemoryQueueConsumer",
    "MemoryQueuePublisher",
]
