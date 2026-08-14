"""Dispatch primitives: opaque job identities, claiming, and outbox leasing."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from loafer.adapters.metadata import SqlMetadataStore
from loafer.adapters.queue.direct import DirectQueueConsumer, DirectQueuePublisher
from loafer.adapters.queue.memory import (
    MemoryQueueBroker,
    MemoryQueueConsumer,
    MemoryQueuePublisher,
)
from loafer.contracts import JobEnvelope
from loafer.core.roles import WorkerRole, stream_subjects
from loafer.core.run_state import RunState
from loafer.exceptions import StaleFenceError


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 14, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs: int) -> None:
        self.now += timedelta(**kwargs)


class SteppedClock:
    """Monotonic seconds under test control, for transport ack windows."""

    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@pytest.fixture()
def store(tmp_path: Path) -> tuple[SqlMetadataStore, Clock]:
    clock = Clock()
    metadata = SqlMetadataStore(f"sqlite:///{tmp_path / 'dispatch.db'}", clock=clock)
    metadata.migrate()
    try:
        yield metadata, clock
    finally:
        metadata.close()


def _version(metadata: SqlMetadataStore, workspace_id: str = "workspace-1") -> str:
    return metadata.register_pipeline_version(
        workspace_id=workspace_id,
        pipeline_key="customers",
        config_digest="a" * 64,
        config={"document": {"name": "customers"}},
    ).id


def _run(
    metadata: SqlMetadataStore,
    command_key: str = "manual-1",
    *,
    workspace_id: str = "workspace-1",
    role: WorkerRole = WorkerRole.ETL,
    environment_id: str | None = None,
) -> str:
    return metadata.create_run(
        workspace_id=workspace_id,
        pipeline_version_id=_version(metadata, workspace_id),
        command_key=command_key,
        role=role,
        environment_id=environment_id,
    ).id


def test_job_envelope_carries_identifiers_only() -> None:
    envelope = JobEnvelope(run_id="run-1", workspace_id="workspace-1", role=WorkerRole.ETL)

    assert set(envelope.model_dump(mode="json")) == {
        "contract_version",
        "run_id",
        "workspace_id",
        "role",
        "attempt",
    }
    assert JobEnvelope.model_validate_json(envelope.model_dump_json()) == envelope


def test_every_role_has_a_distinct_subject_covered_by_the_stream() -> None:
    subjects = [role.subject for role in WorkerRole]

    assert len(set(subjects)) == len(subjects)
    assert set(stream_subjects()) == set(subjects)


def test_outbox_row_carries_the_identifiers_the_relay_needs(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    run_id = _run(metadata)

    records = metadata.pending_outbox()

    assert [record.event_type for record in records] == ["run.created"]
    payload = records[0].payload
    assert payload["run_id"] == run_id
    assert payload["workspace_id"] == "workspace-1"
    assert payload["role"] == WorkerRole.ETL.value
    assert set(payload) == {"run_id", "sequence", "workspace_id", "role"}


def test_claim_by_id_is_a_no_op_for_a_run_already_under_a_live_lease(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    run_id = _run(metadata)

    first = metadata.claim_run_by_id(run_id, "worker-a", timedelta(seconds=30))
    assert first is not None

    duplicate = metadata.claim_run_by_id(run_id, "worker-b", timedelta(seconds=30))

    assert duplicate is None
    assert metadata.get_run(run_id).lease_owner == "worker-a"


def test_claim_by_id_steals_an_expired_lease_with_a_higher_fence(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, clock = store
    run_id = _run(metadata)
    stale = metadata.claim_run_by_id(run_id, "worker-a", timedelta(seconds=5))
    assert stale is not None

    clock.advance(seconds=6)
    current = metadata.claim_run_by_id(run_id, "worker-b", timedelta(seconds=30))

    assert current is not None
    assert current.fencing_token == stale.fencing_token + 1
    assert current.run.attempt == stale.run.attempt + 1
    with pytest.raises(StaleFenceError):
        metadata.transition_run(stale, RunState.SUCCEEDED)


def test_claim_by_id_refuses_a_terminal_run(store: tuple[SqlMetadataStore, Clock]) -> None:
    metadata, _clock = store
    run_id = _run(metadata)
    lease = metadata.claim_run_by_id(run_id, "worker-a", timedelta(seconds=30))
    assert lease is not None
    metadata.transition_run(lease, RunState.RUNNING)
    metadata.transition_run(lease, RunState.SUCCEEDED)

    assert metadata.claim_run_by_id(run_id, "worker-b", timedelta(seconds=30)) is None


def test_claiming_is_restricted_to_one_role(store: tuple[SqlMetadataStore, Clock]) -> None:
    metadata, _clock = store
    _run(metadata, "browser-job", role=WorkerRole.BROWSER)

    assert metadata.claim_run("etl-worker", timedelta(seconds=30), role=WorkerRole.ETL) is None
    browser = metadata.claim_run("browser-worker", timedelta(seconds=30), role=WorkerRole.BROWSER)
    assert browser is not None


def test_list_runnable_returns_role_scoped_identities(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    etl_run = _run(metadata, "etl-job", role=WorkerRole.ETL)
    _run(metadata, "browser-job", role=WorkerRole.BROWSER)

    envelopes = metadata.list_runnable(role=WorkerRole.ETL, limit=10)

    assert [envelope.run_id for envelope in envelopes] == [etl_run]
    assert envelopes[0].workspace_id == "workspace-1"
    assert envelopes[0].role is WorkerRole.ETL


def test_outbox_claim_leases_rows_so_a_second_relay_sees_nothing(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, clock = store
    _run(metadata)

    first = metadata.claim_outbox(limit=10, lease_for=timedelta(seconds=30))
    second = metadata.claim_outbox(limit=10, lease_for=timedelta(seconds=30))

    assert len(first) == 1
    assert second == []
    assert first[0].attempts == 1

    clock.advance(seconds=31)
    reclaimed = metadata.claim_outbox(limit=10, lease_for=timedelta(seconds=30))

    assert [record.id for record in reclaimed] == [first[0].id]
    assert reclaimed[0].attempts == 2


def test_publishing_settles_the_row_and_releasing_records_why_it_failed(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, clock = store
    _run(metadata)
    claimed = metadata.claim_outbox(limit=10)

    metadata.release_outbox(claimed[0].id, error="broker unreachable", retry_after=timedelta(0))
    released = metadata.claim_outbox(limit=10)

    assert released[0].last_error == "broker unreachable"

    metadata.mark_outbox_published(released[0].id, clock())

    assert metadata.claim_outbox(limit=10) == []
    assert metadata.pending_outbox() == []


def test_outbox_claim_can_be_filtered_by_role_and_event_type(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    _run(metadata, "browser-job", role=WorkerRole.BROWSER)

    assert metadata.claim_outbox(limit=10, role=WorkerRole.ETL) == []
    assert len(metadata.claim_outbox(limit=10, role=WorkerRole.BROWSER)) == 1
    assert metadata.claim_outbox(limit=10, event_types=("batch.committed",)) == []


def test_running_count_and_limits_are_scoped_to_tenant_and_environment(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    run_id = _run(metadata, "job-1", environment_id="env-1")
    _run(metadata, "job-2", workspace_id="workspace-2")

    assert metadata.running_count("workspace-1") == 0

    metadata.claim_run_by_id(run_id, "worker-a", timedelta(seconds=30))

    assert metadata.running_count("workspace-1") == 1
    assert metadata.running_count("workspace-1", environment_id="env-1") == 1
    assert metadata.running_count("workspace-1", environment_id="env-2") == 0
    assert metadata.running_count("workspace-2") == 0
    assert metadata.concurrency_limit("workspace-1") is None


def test_quarantine_ends_the_run_and_records_why(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    run_id = _run(metadata)
    lease = metadata.claim_run_by_id(run_id, "worker-a", timedelta(seconds=30))
    assert lease is not None

    quarantined = metadata.quarantine_run(lease, "redelivered 6 times without progress")

    assert quarantined.state is RunState.FAILED
    assert quarantined.error == {
        "type": "PoisonJob",
        "message": "redelivered 6 times without progress",
    }
    assert quarantined.lease_owner is None
    events = [event.event_type for event in metadata.list_events(run_id)]
    assert "run.quarantined" in events
    assert metadata.claim_run_by_id(run_id, "worker-b", timedelta(seconds=30)) is None


def test_quarantine_rejects_a_superseded_lease(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, clock = store
    run_id = _run(metadata)
    stale = metadata.claim_run_by_id(run_id, "worker-a", timedelta(seconds=5))
    assert stale is not None
    clock.advance(seconds=6)
    metadata.claim_run_by_id(run_id, "worker-b", timedelta(seconds=30))

    with pytest.raises(StaleFenceError):
        metadata.quarantine_run(stale, "poison")


def test_memory_transport_deduplicates_by_key_and_isolates_roles() -> None:
    broker = MemoryQueueBroker()
    publisher = MemoryQueuePublisher(broker)
    envelope = JobEnvelope(run_id="run-1", workspace_id="workspace-1")

    publisher.publish(envelope, dedupe_key="outbox-1")
    publisher.publish(envelope, dedupe_key="outbox-1")

    assert broker.depth(WorkerRole.ETL) == 1
    assert broker.depth(WorkerRole.BROWSER) == 0
    assert MemoryQueueConsumer(broker, WorkerRole.BROWSER).fetch(10, 0.0) == []


def test_memory_transport_redelivers_an_unacknowledged_job() -> None:
    clock = SteppedClock()
    broker = MemoryQueueBroker(ack_wait_seconds=30.0, clock=clock)
    MemoryQueuePublisher(broker).publish(
        JobEnvelope(run_id="run-1", workspace_id="workspace-1"),
        dedupe_key="outbox-1",
    )
    consumer = MemoryQueueConsumer(broker, WorkerRole.ETL)

    first = consumer.fetch(10, 0.0)
    assert [job.delivery_count for job in first] == [1]
    assert consumer.fetch(10, 0.0) == []

    clock.advance(31)
    redelivered = consumer.fetch(10, 0.0)

    assert [job.delivery_count for job in redelivered] == [2]
    assert redelivered[0].envelope == first[0].envelope

    redelivered[0].ack()
    clock.advance(31)
    assert consumer.fetch(10, 0.0) == []


def test_memory_transport_honours_nak_delay_and_termination() -> None:
    clock = SteppedClock()
    broker = MemoryQueueBroker(ack_wait_seconds=30.0, clock=clock)
    publisher = MemoryQueuePublisher(broker)
    consumer = MemoryQueueConsumer(broker, WorkerRole.ETL)
    publisher.publish(
        JobEnvelope(run_id="run-1", workspace_id="workspace-1"),
        dedupe_key="outbox-1",
    )

    consumer.fetch(10, 0.0)[0].nak(delay_seconds=10.0)
    assert consumer.fetch(10, 0.0) == []

    clock.advance(11)
    delivered = consumer.fetch(10, 0.0)
    assert len(delivered) == 1

    delivered[0].term()
    clock.advance(60)

    assert consumer.fetch(10, 0.0) == []
    assert [envelope.run_id for envelope in broker.terminated] == ["run-1"]


def test_direct_transport_presents_runnable_runs_without_claiming_them(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    run_id = _run(metadata)
    consumer = DirectQueueConsumer(metadata, WorkerRole.ETL)

    DirectQueuePublisher().publish(
        JobEnvelope(run_id=run_id, workspace_id="workspace-1"),
        dedupe_key="ignored",
    )
    delivered = consumer.fetch(10, 0.0)

    assert [job.envelope.run_id for job in delivered] == [run_id]
    assert metadata.get_run(run_id).state is RunState.QUEUED

    delivered[0].ack()
    assert metadata.get_run(run_id).state is RunState.QUEUED
