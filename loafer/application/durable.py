"""Composition helpers for the durable local and PostgreSQL profiles."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path

from loafer.adapters.metadata import SqlMetadataStore
from loafer.adapters.object_storage import FilesystemObjectStorage
from loafer.config import load_config
from loafer.metadata import PipelineVersion, RunRecord
from loafer.worker import DurableWorker

_LOAFER_DIR = Path.home() / ".loafer"
_METADATA_PATH = _LOAFER_DIR / "metadata.db"
_OBJECTS_PATH = _LOAFER_DIR / "objects"


def default_metadata_url() -> str:
    """Return PostgreSQL when configured, otherwise restricted local SQLite."""
    configured = os.environ.get("LOAFER_METADATA_URL")
    if configured:
        return configured
    _LOAFER_DIR.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{_METADATA_PATH}"


def get_metadata_store(url: str | None = None) -> SqlMetadataStore:
    """Build and migrate the configured authoritative metadata adapter."""
    store = SqlMetadataStore(url or default_metadata_url())
    store.migrate()
    return store


def get_object_storage(root: str | Path | None = None) -> FilesystemObjectStorage:
    """Build the embedded filesystem object-store adapter."""
    configured = root or os.environ.get("LOAFER_OBJECTS_PATH") or _OBJECTS_PATH
    return FilesystemObjectStorage(configured)


def enqueue_pipeline(
    config_path: str | Path,
    *,
    command_key: str,
    workspace_id: str = "local",
    run_id: str | None = None,
    metadata_url: str | None = None,
) -> RunRecord:
    """Persist an immutable config version and idempotent run command."""
    version = register_pipeline_config(
        config_path,
        workspace_id=workspace_id,
        metadata_url=metadata_url,
    )
    return enqueue_registered_version(
        version.id,
        command_key=command_key,
        workspace_id=workspace_id,
        run_id=run_id,
        metadata_url=metadata_url,
    )


def register_pipeline_config(
    config_path: str | Path,
    *,
    workspace_id: str = "local",
    metadata_url: str | None = None,
) -> PipelineVersion:
    """Snapshot a resolved pipeline config as an immutable version."""
    resolved = Path(config_path).resolve()
    config = load_config(resolved)
    document = config.model_dump(mode="json")
    rendered = json.dumps(document, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(rendered.encode()).hexdigest()
    store = get_metadata_store(metadata_url)
    try:
        return store.register_pipeline_version(
            workspace_id=workspace_id,
            pipeline_key=config.name or resolved.stem,
            config_digest=digest,
            config={"document": document, "source_path": str(resolved)},
        )
    finally:
        store.close()


def enqueue_registered_version(
    pipeline_version_id: str,
    *,
    command_key: str,
    workspace_id: str = "local",
    run_id: str | None = None,
    metadata_url: str | None = None,
) -> RunRecord:
    """Create an idempotent run for an already immutable version."""
    store = get_metadata_store(metadata_url)
    try:
        return store.create_run(
            workspace_id=workspace_id,
            pipeline_version_id=pipeline_version_id,
            command_key=command_key,
            run_id=run_id or uuid.uuid4().hex[:12],
        )
    finally:
        store.close()


def get_durable_worker(
    *,
    worker_id: str,
    metadata_url: str | None = None,
    object_root: str | Path | None = None,
) -> DurableWorker:
    """Compose a worker process; callers own its long-running lifecycle."""
    return DurableWorker(
        get_metadata_store(metadata_url),
        get_object_storage(object_root),
        worker_id=worker_id,
    )
