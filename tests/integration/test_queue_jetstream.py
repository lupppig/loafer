"""JetStream contract tests for the delivery guarantees dispatch depends on."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator

import pytest

from loafer.adapters.queue.jetstream import JetStreamTransport
from loafer.contracts import JobEnvelope
from loafer.core.roles import WorkerRole, stream_name, stream_subjects
from loafer.exceptions import QueueError

pytestmark = pytest.mark.integration

_ACK_WAIT_SECONDS = 2.0


@pytest.fixture()
def transport(nats_url: str) -> Iterator[JetStreamTransport]:
    connection = JetStreamTransport(nats_url)
    try:
        yield connection
    finally:
        connection.close()


def _drain(transport: JetStreamTransport, role: WorkerRole) -> None:
    consumer = transport.consumer(role, max_ack_pending=64, ack_wait_seconds=_ACK_WAIT_SECONDS)
    try:
        while True:
            delivered = consumer.fetch(64, 0.5)
            if not delivered:
                return
            for job in delivered:
                job.ack()
    finally:
        consumer.close()


@pytest.fixture()
def etl(transport: JetStreamTransport) -> Iterator[object]:
    _drain(transport, WorkerRole.ETL)
    consumer = transport.consumer(
        WorkerRole.ETL,
        max_ack_pending=64,
        ack_wait_seconds=_ACK_WAIT_SECONDS,
    )
    try:
        yield consumer
    finally:
        _drain(transport, WorkerRole.ETL)
        consumer.close()


def _envelope(role: WorkerRole = WorkerRole.ETL) -> JobEnvelope:
    return JobEnvelope(run_id=uuid.uuid4().hex[:12], workspace_id="workspace-1", role=role)


def test_duplicate_publication_of_one_outbox_row_enqueues_one_job(
    transport: JetStreamTransport,
    etl: object,
) -> None:
    publisher = transport.publisher()
    envelope = _envelope()
    dedupe_key = uuid.uuid4().hex

    publisher.publish(envelope, dedupe_key=dedupe_key)
    publisher.publish(envelope, dedupe_key=dedupe_key)

    delivered = etl.fetch(10, 2.0)  # type: ignore[attr-defined]

    assert [job.envelope for job in delivered] == [envelope]
    assert delivered[0].delivery_count == 1
    delivered[0].ack()


def test_an_unacknowledged_job_is_redelivered_with_a_higher_delivery_count(
    transport: JetStreamTransport,
    etl: object,
) -> None:
    envelope = _envelope()
    transport.publisher().publish(envelope, dedupe_key=uuid.uuid4().hex)

    first = etl.fetch(10, 2.0)  # type: ignore[attr-defined]
    assert [job.delivery_count for job in first] == [1]

    time.sleep(_ACK_WAIT_SECONDS + 1)
    redelivered = etl.fetch(10, 2.0)  # type: ignore[attr-defined]

    assert [job.envelope for job in redelivered] == [envelope]
    assert redelivered[0].delivery_count == 2

    redelivered[0].ack()
    time.sleep(_ACK_WAIT_SECONDS + 1)
    assert etl.fetch(10, 1.0) == []  # type: ignore[attr-defined]


def test_in_progress_ack_extends_the_server_ack_window(
    transport: JetStreamTransport,
    etl: object,
) -> None:
    transport.publisher().publish(_envelope(), dedupe_key=uuid.uuid4().hex)
    first = etl.fetch(1, 2.0)  # type: ignore[attr-defined]

    time.sleep(1)
    first[0].in_progress()
    time.sleep(1.5)

    assert etl.fetch(1, 0.5) == []  # type: ignore[attr-defined]
    time.sleep(_ACK_WAIT_SECONDS + 0.5)
    redelivered = etl.fetch(1, 2.0)  # type: ignore[attr-defined]
    assert redelivered[0].delivery_count == 2
    redelivered[0].ack()


def test_job_survives_transport_process_restart(nats_url: str) -> None:
    first = JetStreamTransport(nats_url)
    _drain(first, WorkerRole.ETL)
    envelope = _envelope()
    first.publisher().publish(envelope, dedupe_key=uuid.uuid4().hex)
    first.close()

    restarted = JetStreamTransport(nats_url)
    try:
        consumer = restarted.consumer(
            WorkerRole.ETL,
            max_ack_pending=1,
            ack_wait_seconds=_ACK_WAIT_SECONDS,
        )
        delivered = consumer.fetch(1, 2.0)
        assert [item.envelope for item in delivered] == [envelope]
        delivered[0].ack()
        consumer.close()
    finally:
        restarted.close()


def test_terminated_jobs_are_never_redelivered(
    transport: JetStreamTransport,
    etl: object,
) -> None:
    transport.publisher().publish(_envelope(), dedupe_key=uuid.uuid4().hex)

    etl.fetch(10, 2.0)[0].term()  # type: ignore[attr-defined]
    time.sleep(_ACK_WAIT_SECONDS + 1)

    assert etl.fetch(10, 1.0) == []  # type: ignore[attr-defined]


def test_roles_consume_from_isolated_subjects(
    transport: JetStreamTransport,
    etl: object,
) -> None:
    _drain(transport, WorkerRole.BROWSER)
    transport.publisher().publish(_envelope(WorkerRole.BROWSER), dedupe_key=uuid.uuid4().hex)

    assert etl.fetch(10, 1.0) == []  # type: ignore[attr-defined]

    browser = transport.consumer(
        WorkerRole.BROWSER,
        max_ack_pending=64,
        ack_wait_seconds=_ACK_WAIT_SECONDS,
    )
    try:
        delivered = browser.fetch(10, 2.0)
        assert [job.envelope.role for job in delivered] == [WorkerRole.BROWSER]
        delivered[0].ack()
    finally:
        browser.close()


def test_consumer_redeploy_reconciles_an_existing_durable(nats_url: str) -> None:
    """A durable outlives the process, so backpressure changes must reach it."""
    connection = JetStreamTransport(nats_url)
    try:
        first = connection.consumer(WorkerRole.ETL, max_ack_pending=8, ack_wait_seconds=2.0)
        first.close()
        second = connection.consumer(WorkerRole.ETL, max_ack_pending=32, ack_wait_seconds=9.0)
        try:
            info = connection._run(
                connection._js.consumer_info(stream_name(), WorkerRole.ETL.durable_name),
                10,
            )
            assert info.config.ack_wait == 9.0
            assert info.config.max_ack_pending == 32
            assert info.config.max_deliver == -1
        finally:
            second.close()
    finally:
        connection.close()


def test_stream_subject_drift_is_reconciled_on_connect(nats_url: str) -> None:
    """A role added after first deployment must become routable."""
    connection = JetStreamTransport(nats_url)
    try:
        api = connection._api
        connection._run(
            connection._js.update_stream(
                api.StreamConfig(
                    name=stream_name(),
                    subjects=[WorkerRole.ETL.subject],
                    retention=api.RetentionPolicy.WORK_QUEUE,
                    duplicate_window=600,
                )
            ),
            10,
        )
    finally:
        connection.close()

    reconnected = JetStreamTransport(nats_url)
    try:
        info = reconnected._run(reconnected._js.stream_info(stream_name()), 10)
        assert set(info.config.subjects) == set(stream_subjects())
    finally:
        reconnected.close()


def test_close_is_idempotent(nats_url: str) -> None:
    connection = JetStreamTransport(nats_url)

    connection.close()
    connection.close()

    assert connection._loop.is_closed()


def test_failure_after_connect_is_wrapped_and_tears_down(
    nats_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def explode(_self: JetStreamTransport) -> None:
        raise RuntimeError("stream setup exploded")

    monkeypatch.setattr(JetStreamTransport, "_ensure_stream", explode)

    with pytest.raises(QueueError, match="stream setup exploded"):
        JetStreamTransport(nats_url)


def test_published_payloads_carry_identifiers_only(
    transport: JetStreamTransport,
    etl: object,
) -> None:
    """Assert on the bytes that actually reached the stream, not on the model.

    The model-level key set is already covered by the unit suite. What only a
    live broker can show is that nothing else was serialized on the way out.
    """
    transport.publisher().publish(_envelope(), dedupe_key=uuid.uuid4().hex)

    delivered = etl.fetch(10, 2.0)  # type: ignore[attr-defined]

    assert json.loads(delivered[0].payload).keys() == {
        "contract_version",
        "run_id",
        "workspace_id",
        "role",
        "attempt",
    }
    delivered[0].ack()
