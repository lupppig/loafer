"""Dispatch primitives: opaque job identities, claiming, and outbox leasing."""

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from loafer.adapters import metadata_schema as schema
from loafer.adapters.metadata import SqlMetadataStore
from loafer.adapters.queue.direct import DirectQueueConsumer, DirectQueuePublisher
from loafer.adapters.queue.memory import (
    MemoryQueueBroker,
    MemoryQueueConsumer,
    MemoryQueuePublisher,
)
from loafer.contracts import JobEnvelope
from loafer.core.roles import WorkerRole, stream_subjects
from loafer.core.run_state import RetryCategory, RunState
from loafer.dispatch import OutboxRelay, QueuedWorker
from loafer.exceptions import MetadataError, QueueError, StaleFenceError
from loafer.metadata import utc_now
from loafer.worker import _LeaseKeeper


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


def test_concurrency_limit_ignores_an_environment_from_another_tenant(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, clock = store
    with metadata.engine.begin() as connection:
        for workspace_id, limit in (("workspace-1", 8), ("workspace-2", 2)):
            connection.execute(
                schema.workspaces.insert().values(
                    id=workspace_id,
                    organization_id="org-1",
                    slug=workspace_id,
                    name=workspace_id,
                    created_at=clock(),
                    max_concurrent_runs=limit,
                )
            )
        connection.execute(
            schema.environments.insert().values(
                id="env-of-workspace-2",
                workspace_id="workspace-2",
                slug="prod",
                name="Production",
                is_production=True,
                created_at=clock(),
                max_concurrent_runs=1,
            )
        )

    borrowed = metadata.concurrency_limit("workspace-1", environment_id="env-of-workspace-2")

    assert borrowed == 8
    assert metadata.concurrency_limit("workspace-2", environment_id="env-of-workspace-2") == 1


def test_claim_transaction_enforces_workspace_concurrency_limit(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, clock = store
    first_run = _run(metadata, "job-1")
    second_run = _run(metadata, "job-2")
    with metadata.engine.begin() as connection:
        connection.execute(
            schema.workspaces.insert().values(
                id="workspace-1",
                organization_id="org-1",
                slug="workspace-1",
                name="Workspace 1",
                created_at=clock(),
                max_concurrent_runs=1,
            )
        )

    assert metadata.claim_run_by_id(first_run, "worker-a", timedelta(seconds=30)) is not None
    assert metadata.claim_run_by_id(second_run, "worker-b", timedelta(seconds=30)) is None
    assert metadata.get_run(second_run).state is RunState.QUEUED


def test_workspace_limit_counts_active_runs_in_other_environments(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, clock = store
    first_run = _run(metadata, "job-1", environment_id="env-1")
    second_run = _run(metadata, "job-2", environment_id="env-2")
    with metadata.engine.begin() as connection:
        connection.execute(
            schema.workspaces.insert().values(
                id="workspace-1",
                organization_id="org-1",
                slug="workspace-1",
                name="Workspace 1",
                created_at=clock(),
                max_concurrent_runs=1,
            )
        )

    assert metadata.claim_run_by_id(first_run, "worker-a", timedelta(seconds=30)) is not None
    assert metadata.claim_run_by_id(second_run, "worker-b", timedelta(seconds=30)) is None
    assert metadata.get_run(second_run).state is RunState.QUEUED


def test_environment_limit_counts_only_active_runs_in_that_environment(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, clock = store
    first_run = _run(metadata, "job-1", environment_id="env-1")
    second_run = _run(metadata, "job-2", environment_id="env-1")
    with metadata.engine.begin() as connection:
        connection.execute(
            schema.workspaces.insert().values(
                id="workspace-1",
                organization_id="org-1",
                slug="workspace-1",
                name="Workspace 1",
                created_at=clock(),
                max_concurrent_runs=2,
            )
        )
        connection.execute(
            schema.environments.insert().values(
                id="env-1",
                workspace_id="workspace-1",
                slug="production",
                name="Production",
                is_production=True,
                created_at=clock(),
                max_concurrent_runs=1,
            )
        )

    assert metadata.claim_run_by_id(first_run, "worker-a", timedelta(seconds=30)) is not None
    assert metadata.claim_run_by_id(second_run, "worker-b", timedelta(seconds=30)) is None
    assert metadata.get_run(second_run).state is RunState.QUEUED


def test_expired_run_is_excluded_from_each_of_its_concurrency_counts(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, clock = store
    run_id = _run(metadata, environment_id="env-1")
    with metadata.engine.begin() as connection:
        connection.execute(
            schema.workspaces.insert().values(
                id="workspace-1",
                organization_id="org-1",
                slug="workspace-1",
                name="Workspace 1",
                created_at=clock(),
                max_concurrent_runs=1,
            )
        )
        connection.execute(
            schema.environments.insert().values(
                id="env-1",
                workspace_id="workspace-1",
                slug="production",
                name="Production",
                is_production=True,
                created_at=clock(),
                max_concurrent_runs=1,
            )
        )

    first = metadata.claim_run_by_id(run_id, "worker-a", timedelta(seconds=5))
    assert first is not None
    clock.advance(seconds=6)

    reclaimed = metadata.claim_run_by_id(run_id, "worker-b", timedelta(seconds=30))

    assert reclaimed is not None
    assert reclaimed.fencing_token == first.fencing_token + 1


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


def test_memory_transport_in_progress_extends_the_ack_window() -> None:
    clock = SteppedClock()
    broker = MemoryQueueBroker(ack_wait_seconds=30.0, clock=clock)
    MemoryQueuePublisher(broker).publish(
        JobEnvelope(run_id="run-1", workspace_id="workspace-1"),
        dedupe_key="outbox-1",
    )
    consumer = MemoryQueueConsumer(broker, WorkerRole.ETL)
    delivery = consumer.fetch(1, 0.0)[0]

    clock.advance(20)
    delivery.in_progress()
    clock.advance(20)

    assert consumer.fetch(1, 0.0) == []
    clock.advance(11)
    assert consumer.fetch(1, 0.0)[0].delivery_count == 2


def test_lease_keeper_renews_metadata_and_transport_ack(tmp_path: Path) -> None:
    metadata = SqlMetadataStore(f"sqlite:///{tmp_path / 'heartbeat.db'}")
    metadata.migrate()
    try:
        run_id = _run(metadata)
        lease = metadata.claim_run_by_id(run_id, "worker-1", timedelta(milliseconds=600))
        assert lease is not None
        acknowledged = threading.Event()

        with _LeaseKeeper(
            metadata,
            lease,
            timedelta(milliseconds=600),
            acknowledged.set,
        ) as keeper:
            assert acknowledged.wait(2)
            renewed = keeper.current()

        assert renewed.expires_at > lease.expires_at
        assert metadata.get_run(run_id).heartbeat_at is not None
    finally:
        metadata.close()


def test_lease_keeper_does_not_hold_its_lock_during_heartbeat(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    run_id = _run(metadata)
    lease = metadata.claim_run_by_id(run_id, "worker-1", timedelta(seconds=30))
    assert lease is not None
    heartbeat_started = threading.Event()
    finish_heartbeat = threading.Event()
    current_returned = threading.Event()
    observed: list[object] = []

    class BlockingMetadata:
        def heartbeat(self, snapshot: object, lease_for: timedelta) -> object:
            del lease_for
            heartbeat_started.set()
            assert finish_heartbeat.wait(2)
            return snapshot

    def read_current(keeper: _LeaseKeeper) -> None:
        observed.append(keeper.current())
        current_returned.set()

    with _LeaseKeeper(
        BlockingMetadata(),  # type: ignore[arg-type]
        lease,
        timedelta(milliseconds=300),
        None,
    ) as keeper:
        assert heartbeat_started.wait(1)
        reader = threading.Thread(target=read_current, args=(keeper,))
        reader.start()
        try:
            assert current_returned.wait(0.2)
        finally:
            finish_heartbeat.set()
            reader.join(timeout=1)

    assert observed == [lease]


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


class StubWorker:
    def __init__(self, metadata: SqlMetadataStore, outcome: RunState) -> None:
        self.metadata = metadata
        self.outcome = outcome
        self.executed: list[str] = []

    def execute(self, lease: object, *, heartbeat_callback: object = None) -> None:
        if callable(heartbeat_callback):
            heartbeat_callback()
        self.executed.append(lease.run.id)  # type: ignore[attr-defined]
        self.metadata.transition_run(lease, RunState.RUNNING)  # type: ignore[arg-type]
        if self.outcome is RunState.RETRY_WAIT:
            self.metadata.transition_run(
                lease,  # type: ignore[arg-type]
                RunState.RETRY_WAIT,
                retry_category=RetryCategory.INFRASTRUCTURE,
                retry_at=utc_now() + timedelta(seconds=5),
            )
        else:
            self.metadata.transition_run(lease, self.outcome)  # type: ignore[arg-type]

    def close(self) -> None:
        return None


def test_outbox_relay_publishes_and_settles_a_run_command(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    run_id = _run(metadata)
    broker = MemoryQueueBroker()

    assert OutboxRelay(metadata, MemoryQueuePublisher(broker)).run_once() == 1

    assert metadata.pending_outbox() == []
    delivered = MemoryQueueConsumer(broker, WorkerRole.ETL).fetch(1, 0.0)
    assert [item.envelope.run_id for item in delivered] == [run_id]


def test_outbox_relay_releases_a_failed_publication_for_retry(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    _run(metadata)

    class BrokenPublisher:
        def publish(self, envelope: JobEnvelope, *, dedupe_key: str) -> None:
            del envelope, dedupe_key
            raise RuntimeError("broker unavailable")

        def close(self) -> None:
            return None

    relay = OutboxRelay(metadata, BrokenPublisher(), retry_delay=timedelta(0))

    assert relay.run_once() == 0
    assert "broker unavailable" in (metadata.pending_outbox()[0].last_error or "")


def test_outbox_relay_republishes_stale_command_for_a_queued_run(
    store: tuple[SqlMetadataStore, Clock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata, clock = store
    run_id = _run(metadata)

    class RecordingPublisher:
        def __init__(self) -> None:
            self.published: list[JobEnvelope] = []
            self.fail_next = False

        def publish(self, envelope: JobEnvelope, *, dedupe_key: str) -> None:
            del dedupe_key
            if self.fail_next:
                self.fail_next = False
                raise RuntimeError("broker unavailable during recovery")
            self.published.append(envelope)

        def close(self) -> None:
            return None

    monkeypatch.setattr("loafer.dispatch.utc_now", clock)
    publisher = RecordingPublisher()
    relay = OutboxRelay(metadata, publisher, retry_delay=timedelta(0))

    assert relay.run_once() == 1
    clock.advance(days=6, seconds=1)
    publisher.fail_next = True

    assert metadata.get_run(run_id).state is RunState.QUEUED
    assert relay.run_once() == 0
    assert relay.run_once() == 1
    assert [item.run_id for item in publisher.published] == [run_id, run_id]


def test_outbox_relay_does_not_republish_a_stale_command_for_a_terminal_run(
    store: tuple[SqlMetadataStore, Clock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata, clock = store
    run_id = _run(metadata)
    broker = MemoryQueueBroker()
    monkeypatch.setattr("loafer.dispatch.utc_now", clock)
    relay = OutboxRelay(metadata, MemoryQueuePublisher(broker))

    assert relay.run_once() == 1
    lease = metadata.claim_run_by_id(run_id, "worker-a", timedelta(seconds=30))
    assert lease is not None
    metadata.transition_run(lease, RunState.RUNNING)
    metadata.transition_run(lease, RunState.SUCCEEDED)
    clock.advance(days=6, seconds=1)

    assert relay.run_once() == 0
    assert len(broker.published) == 1


@pytest.mark.parametrize("outcome", [RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED])
def test_queue_worker_acknowledges_only_after_a_durable_terminal_state(
    store: tuple[SqlMetadataStore, Clock], outcome: RunState
) -> None:
    metadata, _clock = store
    run_id = _run(metadata)
    broker = MemoryQueueBroker()
    publisher = MemoryQueuePublisher(broker)
    publisher.publish(JobEnvelope(run_id=run_id, workspace_id="workspace-1"), dedupe_key="job-1")
    worker = StubWorker(metadata, outcome)
    queued = QueuedWorker(
        worker,  # type: ignore[arg-type]
        metadata,
        MemoryQueueConsumer(broker, WorkerRole.ETL),
        worker_id="worker-1",
    )

    assert queued.run_once(0.0) == run_id
    assert worker.executed == [run_id]
    assert metadata.get_run(run_id).state is outcome
    assert broker.depth(WorkerRole.ETL) == 0


def test_queue_worker_naks_a_durable_retry_state(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    run_id = _run(metadata)
    transport_clock = SteppedClock()
    broker = MemoryQueueBroker(clock=transport_clock)
    MemoryQueuePublisher(broker).publish(
        JobEnvelope(run_id=run_id, workspace_id="workspace-1"), dedupe_key="job-1"
    )
    queued = QueuedWorker(
        StubWorker(metadata, RunState.RETRY_WAIT),  # type: ignore[arg-type]
        metadata,
        MemoryQueueConsumer(broker, WorkerRole.ETL),
        worker_id="worker-1",
        retry_delay=timedelta(seconds=5),
    )

    queued.run_once(0.0)
    assert broker.depth(WorkerRole.ETL) == 1
    assert MemoryQueueConsumer(broker, WorkerRole.ETL).fetch(1, 0.0) == []
    transport_clock.advance(6)
    assert len(MemoryQueueConsumer(broker, WorkerRole.ETL).fetch(1, 0.0)) == 1


def test_redelivery_before_metadata_retry_is_due_is_not_acknowledged(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    run_id = _run(metadata)
    transport_clock = SteppedClock()
    broker = MemoryQueueBroker(clock=transport_clock)
    MemoryQueuePublisher(broker).publish(
        JobEnvelope(run_id=run_id, workspace_id="workspace-1"), dedupe_key="job-1"
    )
    consumer = MemoryQueueConsumer(broker, WorkerRole.ETL)
    queued = QueuedWorker(
        StubWorker(metadata, RunState.RETRY_WAIT),  # type: ignore[arg-type]
        metadata,
        consumer,
        worker_id="worker-1",
        retry_delay=timedelta(seconds=5),
    )

    queued.run_once(0.0)
    transport_clock.advance(6)
    queued.run_once(0.0)

    assert broker.depth(WorkerRole.ETL) == 1
    assert metadata.get_run(run_id).state is RunState.RETRY_WAIT


def test_queue_worker_quarantines_a_poison_delivery(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    run_id = _run(metadata)
    broker = MemoryQueueBroker()
    envelope = JobEnvelope(run_id=run_id, workspace_id="workspace-1")
    MemoryQueuePublisher(broker).publish(envelope, dedupe_key="job-1")
    consumer = MemoryQueueConsumer(broker, WorkerRole.ETL)
    first = consumer.fetch(1, 0.0)[0]
    first.nak(0)
    worker = StubWorker(metadata, RunState.SUCCEEDED)
    queued = QueuedWorker(
        worker,  # type: ignore[arg-type]
        metadata,
        consumer,
        worker_id="worker-1",
        max_deliveries=1,
    )

    queued.run_once(0.0)

    assert worker.executed == []
    assert metadata.get_run(run_id).state is RunState.FAILED
    assert broker.depth(WorkerRole.ETL) == 0


def test_metadata_outage_leaves_delivery_recoverable_after_lease_expiry(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, metadata_clock = store
    run_id = _run(metadata)
    transport_clock = SteppedClock()
    broker = MemoryQueueBroker(ack_wait_seconds=30, clock=transport_clock)
    MemoryQueuePublisher(broker).publish(
        JobEnvelope(run_id=run_id, workspace_id="workspace-1"), dedupe_key="job-1"
    )

    class OutageWorker(StubWorker):
        def execute(self, lease: object, *, heartbeat_callback: object = None) -> None:
            del heartbeat_callback
            self.metadata.transition_run(lease, RunState.RUNNING)  # type: ignore[arg-type]
            raise MetadataError("database connection lost")

    failed = QueuedWorker(
        OutageWorker(metadata, RunState.SUCCEEDED),  # type: ignore[arg-type]
        metadata,
        MemoryQueueConsumer(broker, WorkerRole.ETL),
        worker_id="worker-a",
        lease_for=timedelta(seconds=30),
    )

    with pytest.raises(MetadataError, match="connection lost"):
        failed.run_once(0.0)

    transport_clock.advance(31)
    metadata_clock.advance(seconds=31)
    recovered_worker = StubWorker(metadata, RunState.SUCCEEDED)
    recovered = QueuedWorker(
        recovered_worker,  # type: ignore[arg-type]
        metadata,
        MemoryQueueConsumer(broker, WorkerRole.ETL),
        worker_id="worker-b",
    )
    recovered.run_once(0.0)

    assert recovered_worker.executed == [run_id]
    assert metadata.get_run(run_id).state is RunState.SUCCEEDED
    assert broker.depth(WorkerRole.ETL) == 0


def test_metadata_lookup_outage_never_terminates_the_delivery() -> None:
    broker = MemoryQueueBroker()
    MemoryQueuePublisher(broker).publish(
        JobEnvelope(run_id="run-1", workspace_id="workspace-1"), dedupe_key="job-1"
    )

    class UnavailableMetadata:
        def get_run(self, run_id: str) -> object:
            del run_id
            raise MetadataError("database unavailable")

    queued = QueuedWorker(
        StubWorker(None, RunState.SUCCEEDED),  # type: ignore[arg-type]
        UnavailableMetadata(),  # type: ignore[arg-type]
        MemoryQueueConsumer(broker, WorkerRole.ETL),
        worker_id="worker-1",
    )

    with pytest.raises(MetadataError, match="database unavailable"):
        queued.run_once(0.0)

    assert broker.depth(WorkerRole.ETL) == 1


def test_lost_ack_redelivery_does_not_reexecute_a_terminal_run(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    run_id = _run(metadata)
    transport_clock = SteppedClock()
    broker = MemoryQueueBroker(ack_wait_seconds=30, clock=transport_clock)
    MemoryQueuePublisher(broker).publish(
        JobEnvelope(run_id=run_id, workspace_id="workspace-1"), dedupe_key="job-1"
    )
    base_consumer = MemoryQueueConsumer(broker, WorkerRole.ETL)
    lost = [False]

    class LostAckDelivery:
        def __init__(self, delivered: object) -> None:
            self._delivered = delivered
            self.envelope = delivered.envelope  # type: ignore[attr-defined]
            self.delivery_count = delivered.delivery_count  # type: ignore[attr-defined]

        def in_progress(self) -> None:
            self._delivered.in_progress()  # type: ignore[attr-defined]

        def ack(self) -> None:
            if not lost[0]:
                lost[0] = True
                raise QueueError("ack response lost")
            self._delivered.ack()  # type: ignore[attr-defined]

        def nak(self, delay_seconds: float) -> None:
            self._delivered.nak(delay_seconds)  # type: ignore[attr-defined]

        def term(self) -> None:
            self._delivered.term()  # type: ignore[attr-defined]

    class LostAckConsumer:
        def fetch(self, max_messages: int, timeout_seconds: float) -> list[LostAckDelivery]:
            return [
                LostAckDelivery(item) for item in base_consumer.fetch(max_messages, timeout_seconds)
            ]

        def close(self) -> None:
            return None

    worker = StubWorker(metadata, RunState.SUCCEEDED)
    queued = QueuedWorker(
        worker,  # type: ignore[arg-type]
        metadata,
        LostAckConsumer(),
        worker_id="worker-1",
    )

    with pytest.raises(QueueError, match="ack response lost"):
        queued.run_once(0.0)
    transport_clock.advance(31)
    queued.run_once(0.0)

    assert worker.executed == [run_id]
    assert broker.depth(WorkerRole.ETL) == 0


def test_graceful_shutdown_drains_active_job_without_claiming_another(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    first_run = _run(metadata, "job-1")
    second_run = _run(metadata, "job-2")
    broker = MemoryQueueBroker()
    publisher = MemoryQueuePublisher(broker)
    for index, run_id in enumerate((first_run, second_run), start=1):
        publisher.publish(
            JobEnvelope(run_id=run_id, workspace_id="workspace-1"),
            dedupe_key=f"job-{index}",
        )
    started = threading.Event()
    release = threading.Event()

    class BlockingWorker(StubWorker):
        def execute(self, lease: object, *, heartbeat_callback: object = None) -> None:
            del heartbeat_callback
            self.executed.append(lease.run.id)  # type: ignore[attr-defined]
            self.metadata.transition_run(lease, RunState.RUNNING)  # type: ignore[arg-type]
            started.set()
            assert release.wait(2)
            self.metadata.transition_run(lease, RunState.SUCCEEDED)  # type: ignore[arg-type]

    worker = BlockingWorker(metadata, RunState.SUCCEEDED)
    queued = QueuedWorker(
        worker,  # type: ignore[arg-type]
        metadata,
        MemoryQueueConsumer(broker, WorkerRole.ETL),
        worker_id="worker-1",
    )
    thread = threading.Thread(target=queued.run_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    assert started.wait(2)

    queued.request_shutdown()
    release.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert worker.executed == [first_run]
    assert metadata.get_run(first_run).state is RunState.SUCCEEDED
    assert metadata.get_run(second_run).state is RunState.QUEUED


def test_browser_backlog_cannot_starve_etl_worker(
    store: tuple[SqlMetadataStore, Clock],
) -> None:
    metadata, _clock = store
    broker = MemoryQueueBroker()
    publisher = MemoryQueuePublisher(broker)
    for index in range(20):
        browser_run = _run(metadata, f"browser-{index}", role=WorkerRole.BROWSER)
        publisher.publish(
            JobEnvelope(
                run_id=browser_run,
                workspace_id="workspace-1",
                role=WorkerRole.BROWSER,
            ),
            dedupe_key=f"browser-{index}",
        )
    etl_run = _run(metadata, "etl", role=WorkerRole.ETL)
    publisher.publish(JobEnvelope(run_id=etl_run, workspace_id="workspace-1"), dedupe_key="etl")
    worker = StubWorker(metadata, RunState.SUCCEEDED)
    queued = QueuedWorker(
        worker,  # type: ignore[arg-type]
        metadata,
        MemoryQueueConsumer(broker, WorkerRole.ETL),
        worker_id="etl-worker",
    )

    queued.run_once(0.0)

    assert worker.executed == [etl_run]
    assert broker.depth(WorkerRole.BROWSER) == 20
