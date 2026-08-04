"""Object-storage seam for durable artifacts and temporary output."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from loafer.metadata import StoredArtifact


class ObjectStoragePort(Protocol):
    """Store immutable binary objects behind stable logical keys."""

    def put(
        self,
        key: str,
        content: bytes | Iterable[bytes],
        *,
        kind: str,
        run_id: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> StoredArtifact:
        """Atomically store content and return its immutable descriptor."""

    def read(self, uri: str) -> bytes:
        """Read an object by the descriptor URI returned from ``put``."""

    def delete(self, uri: str) -> None:
        """Delete an object when retention policy permits it."""

    def exists(self, uri: str) -> bool:
        """Return whether an object currently exists."""
