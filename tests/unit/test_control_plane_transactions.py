"""Atomicity tests for control-plane mutations and their audit records."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from loafer.adapters.metadata import SqlMetadataStore
from loafer.control_plane.domain import AuthContext
from loafer.control_plane.repository import ControlPlaneRepository
from loafer.control_plane.service import ControlPlaneService
from loafer.exceptions import MetadataError


def test_audit_failure_rolls_back_control_plane_mutations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = SqlMetadataStore(f"sqlite:///{tmp_path / 'atomic-audit.db'}")
    store.migrate()
    repository = ControlPlaneRepository(store)
    workspace = repository.bootstrap_workspace(
        organization_id="org-a",
        subject_id="owner",
        slug="primary",
        name="Primary",
    )
    pipeline = repository.register_pipeline(
        workspace_id=workspace["id"],
        pipeline_key="orders",
        document={"name": "orders"},
    )
    service = ControlPlaneService(repository)
    auth = AuthContext("owner")
    now = datetime(2026, 1, 1, tzinfo=UTC)

    def fail_audit(**_kwargs: object) -> None:
        raise MetadataError("audit insert failed")

    monkeypatch.setattr(repository, "audit", fail_audit)

    mutations = (
        lambda: service.create_run(
            auth,
            workspace["id"],
            pipeline_version_id=pipeline["id"],
            idempotency_key="create-run",
            request_id="request-run",
        ),
        lambda: service.create_connection(
            auth,
            workspace["id"],
            environment_id=None,
            name="warehouse",
            connector_type="postgres",
            secret_reference="vault:warehouse",
            metadata={},
            request_id="request-connection",
        ),
        lambda: service.upsert_schedule(
            auth,
            workspace["id"],
            schedule_id="daily-orders",
            pipeline_version_id=pipeline["id"],
            trigger_kind="cron",
            trigger_spec="0 0 * * *",
            timezone="UTC",
            enabled=True,
            next_run_at=now,
            request_id="request-schedule",
        ),
    )

    try:
        for mutate in mutations:
            with pytest.raises(MetadataError, match="audit insert failed"):
                mutate()

        assert repository.list_runs(workspace["id"]) == []
        assert repository.list_connections(workspace["id"]) == []
        assert repository.list_schedules(workspace["id"]) == []
    finally:
        store.close()
