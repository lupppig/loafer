"""Local adapters for application runtime ports."""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from loafer.contracts import Checkpoint, RunEvent
from loafer.metadata import BatchCommit, RunLease
from loafer.ports.metadata import MetadataStore
from loafer.ports.object_storage import ObjectStoragePort


class NeverCancelled:
    """Default cancellation adapter for synchronous local execution."""

    def is_cancelled(self, run_id: str) -> bool:
        del run_id
        return False


class NullCheckpointStore:
    """No-op checkpoint adapter until Phase 3 adds durable metadata."""

    def load(self, run_id: str, partition_id: str) -> Checkpoint | None:
        del run_id, partition_id
        return None

    def save(self, checkpoint: Checkpoint) -> None:
        del checkpoint


class EnvironmentSecretResolver:
    """Resolve local secret references from environment variables."""

    def resolve(self, reference: str) -> str | None:
        return os.environ.get(reference)


class ScopedSecretResolver:
    """Bound one resolver to a run's allow-list and a short access window."""

    def __init__(
        self,
        resolver: Any,
        allowed_references: set[str] | frozenset[str],
        *,
        ttl_seconds: float = 300.0,
        clock: Any = time.monotonic,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("secret access TTL must be positive")
        self._resolver = resolver
        self._allowed = frozenset(allowed_references)
        self._expires_at = float(clock()) + ttl_seconds
        self._clock = clock

    def resolve(self, reference: str) -> str | None:
        if reference not in self._allowed:
            return None
        if float(self._clock()) >= self._expires_at:
            return None
        return self._resolver.resolve(reference)


class NullEventPublisher:
    """Discard events for callers that consume the returned iterator."""

    def publish(self, event: RunEvent) -> None:
        del event


class InputReviewPort:
    """Portable stdin reviewer used by the local Python API."""

    def approve_transform(self, generated_code: str) -> bool:
        print("\nAI-generated transform code:\n")
        print(generated_code)
        try:
            answer = input("Execute this code? [y/N]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            return False
        return answer in {"y", "yes"}


class MetadataCancellation:
    """Read cooperative cancellation from the authoritative run record."""

    def __init__(self, metadata: MetadataStore) -> None:
        self._metadata = metadata

    def is_cancelled(self, run_id: str) -> bool:
        return self._metadata.cancellation_requested(run_id)


class DurableBatchRecovery:
    """Stage transformed batches as immutable objects before checkpointing."""

    def __init__(
        self,
        metadata: MetadataStore,
        objects: ObjectStoragePort,
        lease: RunLease,
    ) -> None:
        self._metadata = metadata
        self._objects = objects
        self._lease = lease

    def restore(self, run_id: str, partition_id: str) -> list[BatchCommit]:
        return self._metadata.list_batch_commits(run_id, partition_id)

    def read_rows(self, commit: BatchCommit) -> list[dict[str, Any]]:
        payload = self._objects.read(commit.artifact.uri)
        checksum = hashlib.sha256(payload).hexdigest()
        if checksum != commit.artifact.checksum:
            raise ValueError(f"recovery artifact checksum mismatch for {commit.envelope.batch_id}")
        rows = []
        for line in payload.splitlines():
            value = json.loads(line, object_hook=_row_object_hook)
            if not isinstance(value, dict):
                raise ValueError("recovery artifact contains a non-object row")
            rows.append(value)
        return rows

    def commit(self, envelope: Any, rows: list[dict[str, Any]]) -> Checkpoint:
        payload = b"".join(
            json.dumps(
                row,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                default=_row_json_default,
            ).encode("utf-8")
            + b"\n"
            for row in rows
        )
        artifact = self._objects.put(
            (
                f"runs/{envelope.run_id}/partitions/{envelope.partition_id}/"
                f"batches/{envelope.batch_id}.jsonl"
            ),
            payload,
            kind="temporary_output",
            run_id=envelope.run_id,
            metadata={
                "batch_id": envelope.batch_id,
                "rows": len(rows),
                "format": "loafer-jsonl-v1",
            },
        )
        checkpoint = Checkpoint(
            checkpoint_id=hashlib.sha256(
                (f"{envelope.run_id}\0{envelope.partition_id}\0{envelope.batch_id}").encode()
            ).hexdigest()[:32],
            run_id=envelope.run_id,
            partition_id=envelope.partition_id,
            batch_id=envelope.batch_id,
            source_position=envelope.source_position_end,
            committed_at=artifact.created_at,
        )
        return self._metadata.commit_batch(
            self._lease,
            envelope,
            checkpoint,
            artifact,
        ).checkpoint


def _row_json_default(value: Any) -> dict[str, str]:
    if isinstance(value, datetime):
        return {"__loafer_type__": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"__loafer_type__": "date", "value": value.isoformat()}
    if isinstance(value, Decimal):
        return {"__loafer_type__": "decimal", "value": str(value)}
    if isinstance(value, UUID):
        return {"__loafer_type__": "uuid", "value": str(value)}
    if isinstance(value, bytes):
        return {"__loafer_type__": "bytes", "value": value.hex()}
    raise TypeError(f"unsupported recovery value: {type(value).__name__}")


def _row_object_hook(value: dict[str, Any]) -> Any:
    marker = value.get("__loafer_type__")
    raw = value.get("value")
    if marker == "datetime":
        return datetime.fromisoformat(raw)
    if marker == "date":
        return date.fromisoformat(raw)
    if marker == "decimal":
        return Decimal(raw)
    if marker == "uuid":
        return UUID(raw)
    if marker == "bytes":
        return bytes.fromhex(raw)
    return value
