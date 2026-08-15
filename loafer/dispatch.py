"""Transactional outbox relay and queue-driven worker orchestration."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta

from loafer.contracts import JobEnvelope
from loafer.core.roles import WorkerRole
from loafer.core.run_state import RunState
from loafer.exceptions import MetadataNotFoundError
from loafer.metadata import utc_now
from loafer.ports.metadata import MetadataStore
from loafer.ports.queue import QueueConsumer, QueuePublisher
from loafer.worker import DurableWorker


class OutboxRelay:
    """Publish leased run commands and settle them in authoritative metadata."""

    def __init__(
        self,
        metadata: MetadataStore,
        publisher: QueuePublisher,
        *,
        lease_for: timedelta = timedelta(seconds=30),
        retry_delay: timedelta = timedelta(seconds=5),
        batch_size: int = 100,
        owned_resources: tuple[object, ...] = (),
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self._metadata = metadata
        self._publisher = publisher
        self._lease_for = lease_for
        self._retry_delay = retry_delay
        self._batch_size = batch_size
        self._owned_resources = owned_resources
        self._shutdown = threading.Event()

    def run_once(self) -> int:
        """Publish one claimed page, returning the number successfully settled."""
        records = self._metadata.claim_outbox(
            limit=self._batch_size,
            lease_for=self._lease_for,
            event_types=("run.created",),
        )
        published = 0
        for record in records:
            try:
                payload = record.payload
                envelope = JobEnvelope(
                    run_id=str(payload["run_id"]),
                    workspace_id=str(payload["workspace_id"]),
                    role=WorkerRole(str(payload["role"])),
                )
                self._publisher.publish(envelope, dedupe_key=record.id)
            except Exception as exc:
                self._metadata.release_outbox(
                    record.id,
                    error=f"{type(exc).__name__}: {exc}",
                    retry_after=self._retry_delay,
                )
                continue
            self._metadata.mark_outbox_published(record.id, utc_now())
            published += 1
        return published

    def run_forever(self, poll_interval: float = 1.0) -> None:
        """Relay until interrupted, sleeping only while no publication succeeds."""
        try:
            while not self._shutdown.is_set():
                if self.run_once() == 0:
                    self._shutdown.wait(poll_interval)
        except (KeyboardInterrupt, SystemExit):
            return

    def request_shutdown(self) -> None:
        """Stop polling after the current claimed outbox page is settled."""
        self._shutdown.set()

    def close(self) -> None:
        self._publisher.close()
        for resource in self._owned_resources:
            close = getattr(resource, "close", None)
            if close is not None:
                close()


class QueuedWorker:
    """Execute role-scoped queue deliveries under metadata leases and fencing."""

    def __init__(
        self,
        worker: DurableWorker,
        metadata: MetadataStore,
        consumer: QueueConsumer,
        *,
        worker_id: str,
        role: WorkerRole = WorkerRole.ETL,
        lease_for: timedelta = timedelta(seconds=30),
        retry_delay: timedelta = timedelta(seconds=5),
        max_deliveries: int = 5,
        owned_resources: tuple[object, ...] = (),
    ) -> None:
        if max_deliveries < 1:
            raise ValueError("max_deliveries must be positive")
        self._worker = worker
        self._metadata = metadata
        self._consumer = consumer
        self._worker_id = worker_id
        self._role = role
        self._lease_for = lease_for
        self._retry_delay = retry_delay
        self._max_deliveries = max_deliveries
        self._owned_resources = owned_resources
        self._shutdown = threading.Event()

    def run_once(self, timeout_seconds: float = 1.0) -> str | None:
        """Process at most one delivery and settle it from durable run state."""
        deliveries = self._consumer.fetch(1, timeout_seconds)
        if not deliveries:
            return None
        delivery = deliveries[0]
        envelope = delivery.envelope
        try:
            run = self._metadata.get_run(envelope.run_id)
        except MetadataNotFoundError:
            delivery.term()
            return envelope.run_id

        # Treat routing metadata as an assertion, never as authorization.
        if run.workspace_id != envelope.workspace_id or run.role != self._role:
            delivery.term()
            return envelope.run_id

        limit = self._metadata.concurrency_limit(
            run.workspace_id,
            environment_id=run.environment_id,
        )
        if (
            limit is not None
            and self._metadata.running_count(
                run.workspace_id,
                environment_id=run.environment_id,
            )
            >= limit
        ):
            delivery.nak(self._retry_delay.total_seconds())
            return envelope.run_id

        lease = self._metadata.claim_run_by_id(envelope.run_id, self._worker_id, self._lease_for)
        if lease is None:
            current = self._metadata.get_run(envelope.run_id)
            if current.state is RunState.RETRY_WAIT:
                delivery.nak(self._retry_wait_seconds(current.retry_at))
            elif current.state is RunState.QUEUED:
                delivery.nak(self._retry_delay.total_seconds())
            elif current.state in {
                RunState.CLAIMED,
                RunState.RUNNING,
                RunState.CANCELLING,
            }:
                delivery.nak(self._retry_wait_seconds(current.lease_expires_at))
            else:
                delivery.ack()
            return envelope.run_id

        if delivery.delivery_count > self._max_deliveries:
            self._metadata.quarantine_run(
                lease,
                f"redelivered {delivery.delivery_count} times without durable completion",
            )
            delivery.term()
            return envelope.run_id

        try:
            self._worker.execute(lease, heartbeat_callback=delivery.in_progress)
        except Exception:
            if self._metadata.get_run(envelope.run_id).state in {
                RunState.SUCCEEDED,
                RunState.FAILED,
                RunState.CANCELLED,
            }:
                delivery.ack()
            raise
        state = self._metadata.get_run(envelope.run_id).state
        if state is RunState.RETRY_WAIT:
            current = self._metadata.get_run(envelope.run_id)
            delivery.nak(self._retry_wait_seconds(current.retry_at))
        elif state in {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}:
            delivery.ack()
        else:
            # No durable terminal/retry state means the acknowledgement is unsafe.
            delivery.nak(self._retry_delay.total_seconds())
        return envelope.run_id

    def _retry_wait_seconds(self, retry_at: datetime | None) -> float:
        if retry_at is None:
            return self._retry_delay.total_seconds()
        delay = (retry_at - utc_now()).total_seconds()
        return max(self._retry_delay.total_seconds(), delay)

    def run_forever(self, poll_interval: float = 1.0) -> None:
        try:
            while not self._shutdown.is_set():
                self.run_once(timeout_seconds=poll_interval)
        except (KeyboardInterrupt, SystemExit):
            return

    def request_shutdown(self) -> None:
        """Stop fetching new jobs while allowing the active job to finish."""
        self._shutdown.set()

    def close(self) -> None:
        self._consumer.close()
        self._worker.close()
        for resource in self._owned_resources:
            close = getattr(resource, "close", None)
            if close is not None:
                close()


__all__ = ["OutboxRelay", "QueuedWorker"]
