"""NATS JetStream job transport with durable pull consumers.

The engine, application service, and worker are synchronous, so the async NATS
client is confined here behind a dedicated event loop thread rather than
forcing asyncio across the port boundary.

Two JetStream settings carry the delivery guarantees Loafer relies on: a
work-queue stream with a duplicate window, so a relay that publishes the same
outbox row twice enqueues one job; and explicit acknowledgement with a bounded
``max_ack_pending``, which is what stops a saturated role from fetching work it
cannot start.
"""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Coroutine
from dataclasses import dataclass, field
from typing import Any

from loafer.contracts import JobEnvelope
from loafer.core.roles import WorkerRole, stream_name, stream_subjects
from loafer.exceptions import QueueError

_DEDUPE_WINDOW_SECONDS = 600
_CONNECT_TIMEOUT_SECONDS = 10.0


def _nats_modules() -> tuple[Any, Any, Any]:
    try:
        import nats
        from nats.js import api as js_api
        from nats.js import errors as js_errors
    except ImportError as exc:
        raise QueueError("JetStream queue requires 'nats-py'") from exc
    return nats, js_api, js_errors


class _LoopThread:
    """Own one event loop so synchronous callers never manage asyncio."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._serve,
            name="loafer-jetstream",
            daemon=True,
        )
        self._thread.start()

    def _serve(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def run(self, coroutine: Coroutine[Any, Any, Any], timeout: float) -> Any:
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout)
        except BaseException:
            # A timed-out call leaves its coroutine running on the loop.
            # Cancelling here keeps a failed connect or fetch from surviving
            # into loop teardown as an orphaned task.
            future.cancel()
            raise

    def close(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        pending = asyncio.all_tasks(self._loop)
        for task in pending:
            task.cancel()
        if pending:
            self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        self._loop.close()


class JetStreamTransport:
    """One JetStream connection shared by a process's publisher and consumers."""

    def __init__(
        self,
        servers: str | list[str],
        *,
        connect_timeout: float = _CONNECT_TIMEOUT_SECONDS,
    ) -> None:
        self._nats, self._api, self._errors = _nats_modules()
        self._servers = [servers] if isinstance(servers, str) else list(servers)
        self._timeout = connect_timeout
        self._loop = _LoopThread()
        try:
            self._connection = self._loop.run(self._connect(), connect_timeout)
            self._js = self._connection.jetstream()
            self._loop.run(self._ensure_stream(), connect_timeout)
        except QueueError:
            raise
        except Exception as exc:
            self._loop.close()
            raise QueueError(f"could not connect to NATS JetStream: {exc}") from exc

    async def _connect(self) -> Any:
        return await self._nats.connect(servers=self._servers)

    async def _ensure_stream(self) -> None:
        config = self._api.StreamConfig(
            name=stream_name(),
            subjects=list(stream_subjects()),
            retention=self._api.RetentionPolicy.WORK_QUEUE,
            duplicate_window=_DEDUPE_WINDOW_SECONDS,
        )
        try:
            await self._js.stream_info(stream_name())
        except self._errors.NotFoundError:
            await self._js.add_stream(config)

    def publisher(self) -> JetStreamPublisher:
        """Return a publisher bound to this connection."""
        return JetStreamPublisher(self)

    def consumer(
        self,
        role: WorkerRole,
        *,
        max_ack_pending: int,
        ack_wait_seconds: float = 60.0,
    ) -> JetStreamConsumer:
        """Return a durable pull consumer for exactly one role."""
        return JetStreamConsumer(
            self,
            role,
            max_ack_pending=max_ack_pending,
            ack_wait_seconds=ack_wait_seconds,
        )

    def close(self) -> None:
        """Drain the connection and stop the owned event loop."""
        try:
            self._loop.run(self._connection.drain(), self._timeout)
        except Exception:
            pass
        self._loop.close()

    def _run(self, coroutine: Coroutine[Any, Any, Any], timeout: float) -> Any:
        return self._loop.run(coroutine, timeout)


class JetStreamPublisher:
    """Publish job identities, deduplicated by the outbox row id."""

    def __init__(self, transport: JetStreamTransport) -> None:
        self._transport = transport
        self._timeout = _CONNECT_TIMEOUT_SECONDS

    def publish(self, envelope: JobEnvelope, *, dedupe_key: str) -> None:
        payload = json.dumps(
            envelope.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        try:
            self._transport._run(
                self._transport._js.publish(
                    envelope.role.subject,
                    payload,
                    headers={"Nats-Msg-Id": dedupe_key},
                ),
                self._timeout,
            )
        except Exception as exc:
            raise QueueError(f"could not publish job {envelope.run_id}: {exc}") from exc

    def close(self) -> None:
        return None


@dataclass
class JetStreamDeliveredJob:
    """One JetStream delivery with explicit, caller-driven acknowledgement."""

    envelope: JobEnvelope
    delivery_count: int
    _transport: JetStreamTransport = field(repr=False)
    _message: Any = field(repr=False)

    def ack(self) -> None:
        self._settle(self._message.ack(), "acknowledge")

    def nak(self, delay_seconds: float) -> None:
        self._settle(self._message.nak(delay=delay_seconds), "return")

    def term(self) -> None:
        self._settle(self._message.term(), "terminate")

    def _settle(self, coroutine: Coroutine[Any, Any, Any], action: str) -> None:
        try:
            self._transport._run(coroutine, _CONNECT_TIMEOUT_SECONDS)
        except Exception as exc:
            raise QueueError(f"could not {action} job {self.envelope.run_id}: {exc}") from exc


class JetStreamConsumer:
    """Fetch one role's job identities from a durable pull consumer."""

    def __init__(
        self,
        transport: JetStreamTransport,
        role: WorkerRole,
        *,
        max_ack_pending: int,
        ack_wait_seconds: float,
    ) -> None:
        self._transport = transport
        self._role = role
        api = transport._api
        config = api.ConsumerConfig(
            durable_name=role.durable_name,
            ack_policy=api.AckPolicy.EXPLICIT,
            ack_wait=ack_wait_seconds,
            max_ack_pending=max_ack_pending,
            filter_subject=role.subject,
        )
        try:
            self._subscription = transport._run(
                transport._js.pull_subscribe(
                    role.subject,
                    durable=role.durable_name,
                    config=config,
                ),
                _CONNECT_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            raise QueueError(f"could not subscribe to {role.subject}: {exc}") from exc

    def fetch(self, max_messages: int, timeout_seconds: float) -> list[JetStreamDeliveredJob]:
        try:
            messages = self._transport._run(
                self._subscription.fetch(batch=max_messages, timeout=timeout_seconds),
                timeout_seconds + _CONNECT_TIMEOUT_SECONDS,
            )
        except Exception:
            # An empty fetch window is the steady state for an idle pool, and
            # the client reports it as a timeout error rather than an empty
            # list. Treat any fetch failure as "no work": the next poll retries,
            # and nothing has been acknowledged.
            return []
        return [self._deliver(message) for message in messages]

    def close(self) -> None:
        try:
            self._transport._run(self._subscription.unsubscribe(), _CONNECT_TIMEOUT_SECONDS)
        except Exception:
            pass

    def _deliver(self, message: Any) -> JetStreamDeliveredJob:
        try:
            envelope = JobEnvelope.model_validate_json(message.data)
        except Exception as exc:
            # A payload this consumer cannot parse will never become parseable.
            # Terminate it rather than letting it cycle forever.
            self._transport._run(message.term(), _CONNECT_TIMEOUT_SECONDS)
            raise QueueError(f"unparseable job payload on {self._role.subject}: {exc}") from exc
        return JetStreamDeliveredJob(
            envelope=envelope,
            delivery_count=int(message.metadata.num_delivered),
            _transport=self._transport,
            _message=message,
        )


__all__ = [
    "JetStreamConsumer",
    "JetStreamDeliveredJob",
    "JetStreamPublisher",
    "JetStreamTransport",
]
