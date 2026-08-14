"""JetStream contract tests for the delivery guarantees dispatch depends on."""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator

import pytest

from loafer.adapters.queue.jetstream import JetStreamTransport
from loafer.contracts import JobEnvelope
from loafer.core.roles import WorkerRole

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


def test_published_payloads_carry_identifiers_only(transport: JetStreamTransport) -> None:
    envelope = _envelope()

    assert set(envelope.model_dump(mode="json")) == {
        "contract_version",
        "run_id",
        "workspace_id",
        "role",
        "attempt",
    }
