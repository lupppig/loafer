"""PostgreSQL contract tests for authoritative durable metadata."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import inspect

from loafer.adapters.metadata import SqlMetadataStore
from loafer.core.run_state import RunState
from loafer.exceptions import StaleFenceError

pytestmark = pytest.mark.integration


@pytest.fixture()
def metadata(postgres_url: str) -> SqlMetadataStore:
    store = SqlMetadataStore(postgres_url)
    store.migrate(0)
    store.migrate()
    try:
        yield store
    finally:
        store.migrate(0)
        store.close()


def test_postgres_empty_schema_and_previous_schema_upgrade(
    postgres_url: str,
) -> None:
    store = SqlMetadataStore(postgres_url)
    try:
        store.migrate(0)
        assert store.migrate(1) == 1
        version = store.register_pipeline_version(
            workspace_id="pg-workspace",
            pipeline_key="customers",
            config_digest="a" * 64,
            config={"document": {"name": "customers"}},
        )

        assert store.migrate() == 2
        assert store.get_pipeline_version(version.id).config_digest == "a" * 64
        assert "loafer_outbox" in inspect(store.engine).get_table_names()

        assert store.migrate(1) == 1
        assert "loafer_outbox" not in inspect(store.engine).get_table_names()
        assert store.migrate() == 2
    finally:
        store.migrate(0)
        store.close()


def test_postgres_claims_are_fenced_and_events_are_monotonic(
    metadata: SqlMetadataStore,
) -> None:
    version = metadata.register_pipeline_version(
        workspace_id="pg-workspace",
        pipeline_key="orders",
        config_digest="b" * 64,
        config={"document": {"name": "orders"}},
    )
    metadata.create_run(
        workspace_id="pg-workspace",
        pipeline_version_id=version.id,
        command_key="request-1",
        run_id="pg-run-1",
    )
    lease = metadata.claim_run("worker-a", timedelta(seconds=30))
    assert lease is not None
    metadata.transition_run(lease, RunState.RUNNING)
    metadata.append_event(lease, "worker.progress", {"rows": 10})
    metadata.transition_run(lease, RunState.SUCCEEDED)

    events = metadata.list_events("pg-run-1")
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))

    with pytest.raises(StaleFenceError):
        metadata.append_event(lease, "worker.late", {})
