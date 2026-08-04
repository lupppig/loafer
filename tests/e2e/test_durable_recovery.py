"""Single-node crash recovery at the durable batch commit boundary."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from loafer.adapters.metadata import SqlMetadataStore
from loafer.adapters.object_storage import FilesystemObjectStorage
from loafer.adapters.runtime import (
    DurableBatchRecovery,
    EnvironmentSecretResolver,
    MetadataCancellation,
    NullCheckpointStore,
    NullEventPublisher,
)
from loafer.application.service import RunPipeline
from loafer.config import load_config
from loafer.contracts import RunRequest
from loafer.core.run_state import RunState
from loafer.worker import DurableWorker, RejectUnapprovedTransform


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


class CrashAfterCommit:
    def __init__(self, recovery: DurableBatchRecovery, crash_after: int) -> None:
        self._recovery = recovery
        self._crash_after = crash_after
        self._commits = 0

    def restore(self, run_id: str, partition_id: str) -> Any:
        return self._recovery.restore(run_id, partition_id)

    def read_rows(self, commit: Any) -> Any:
        return self._recovery.read_rows(commit)

    def commit(self, envelope: Any, rows: list[dict[str, Any]]) -> Any:
        checkpoint = self._recovery.commit(envelope, rows)
        self._commits += 1
        if self._commits == self._crash_after:
            raise SystemExit("simulated worker kill after durable commit")
        return checkpoint


def _config(tmp_path: Path) -> tuple[Path, Path]:
    source = tmp_path / "input.csv"
    source.write_text(
        "id,name\n" + "".join(f"{index},name-{index}\n" for index in range(1, 6)),
        encoding="utf-8",
    )
    transform = tmp_path / "transform.py"
    transform.write_text(
        "def transform(data):\n    return [{**row, 'name': row['name'].upper()} for row in data]\n",
        encoding="utf-8",
    )
    output = tmp_path / "output.json"
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        "\n".join(
            [
                "name: durable-recovery",
                "mode: etl",
                "source:",
                "  type: csv",
                f"  path: {source}",
                "target:",
                "  type: json",
                f"  path: {output}",
                "transform:",
                "  type: custom",
                f"  path: {transform}",
                "execution:",
                "  transform_class: row_local",
                "  schema_drift: fail",
                "chunk_size: 2",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return config, output


@pytest.mark.parametrize("crash_after", [1, 2, 3])
def test_reclaimed_worker_resumes_from_each_durable_batch(
    tmp_path: Path,
    crash_after: int,
) -> None:
    config_path, output = _config(tmp_path)
    clock = Clock()
    metadata = SqlMetadataStore(f"sqlite:///{tmp_path / 'metadata.db'}", clock=clock)
    metadata.migrate()
    objects = FilesystemObjectStorage(tmp_path / "objects")
    config_document = load_config(config_path).model_dump(mode="json")
    digest = hashlib.sha256(json.dumps(config_document, sort_keys=True).encode()).hexdigest()
    version = metadata.register_pipeline_version(
        workspace_id="local",
        pipeline_key="durable-recovery",
        config_digest=digest,
        config={"document": config_document},
    )
    metadata.create_run(
        workspace_id="local",
        pipeline_version_id=version.id,
        command_key="manual:test",
        run_id="recovery-run",
    )
    stale_lease = metadata.claim_run("worker-a", timedelta(seconds=5))
    assert stale_lease is not None
    metadata.transition_run(stale_lease, RunState.RUNNING)
    crashing_recovery = CrashAfterCommit(
        DurableBatchRecovery(metadata, objects, stale_lease),
        crash_after,
    )
    first_attempt = RunPipeline(
        cancellation=MetadataCancellation(metadata),
        checkpoints=NullCheckpointStore(),
        secrets=EnvironmentSecretResolver(),
        events=NullEventPublisher(),
        reviewer=RejectUnapprovedTransform(),
        recovery=crashing_recovery,
    )

    with pytest.raises(SystemExit, match="simulated worker kill"):
        list(first_attempt.stream(RunRequest(config_path=str(config_path), run_id="recovery-run")))
    assert not output.exists()
    checkpoint = metadata.latest_checkpoint("recovery-run", "default")
    assert checkpoint is not None
    assert checkpoint.batch_id == f"batch-{crash_after:08d}"

    clock.now += timedelta(seconds=6)
    current_lease = metadata.claim_run("worker-b", timedelta(seconds=30))
    assert current_lease is not None
    DurableWorker(metadata, objects, worker_id="worker-b").execute(current_lease)

    assert metadata.get_run("recovery-run").state is RunState.SUCCEEDED
    published = json.loads(output.read_text(encoding="utf-8"))
    assert [row["id"] for row in published] == ["1", "2", "3", "4", "5"]
    assert [row["name"] for row in published] == [
        "NAME-1",
        "NAME-2",
        "NAME-3",
        "NAME-4",
        "NAME-5",
    ]
    assert len(metadata.list_batch_commits("recovery-run", "default")) == 3
    sequences = [event.sequence for event in metadata.list_events("recovery-run")]
    assert sequences == list(range(1, len(sequences) + 1))
    metadata.close()
