"""Local and in-memory object-storage adapters."""

from __future__ import annotations

import hashlib
import os
import tempfile
import uuid
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlparse

from loafer.exceptions import MetadataError
from loafer.metadata import StoredArtifact, utc_now


class FilesystemObjectStorage:
    """Atomic local object storage for the embedded single-node profile."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root).resolve()
        self._root.mkdir(parents=True, exist_ok=True)

    def put(
        self,
        key: str,
        content: bytes | Iterable[bytes],
        *,
        kind: str,
        run_id: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> StoredArtifact:
        destination = self._path_for_key(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        size = 0
        handle = tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=destination.parent,
            delete=False,
        )
        temporary = Path(handle.name)
        try:
            chunks = (content,) if isinstance(content, bytes) else content
            for chunk in chunks:
                if not isinstance(chunk, bytes):
                    raise TypeError("object storage content chunks must be bytes")
                handle.write(chunk)
                digest.update(chunk)
                size += len(chunk)
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()
            os.replace(temporary, destination)
        except Exception:
            handle.close()
            temporary.unlink(missing_ok=True)
            raise
        checksum = digest.hexdigest()
        return StoredArtifact(
            id=hashlib.sha256(f"{destination.as_uri()}\0{checksum}".encode()).hexdigest()[:32],
            run_id=run_id,
            kind=kind,
            uri=destination.as_uri(),
            checksum=checksum,
            size_bytes=size,
            metadata=dict(metadata or {}),
            created_at=utc_now(),
        )

    def read(self, uri: str) -> bytes:
        return self._path_for_uri(uri).read_bytes()

    def delete(self, uri: str) -> None:
        self._path_for_uri(uri).unlink(missing_ok=True)

    def exists(self, uri: str) -> bool:
        return self._path_for_uri(uri).is_file()

    def _path_for_key(self, key: str) -> Path:
        logical = PurePosixPath(key)
        if logical.is_absolute() or ".." in logical.parts or not logical.parts:
            raise MetadataError(f"unsafe object key: {key}")
        path = (self._root / Path(*logical.parts)).resolve()
        if not path.is_relative_to(self._root):
            raise MetadataError(f"unsafe object key: {key}")
        return path

    def _path_for_uri(self, uri: str) -> Path:
        parsed = urlparse(uri)
        if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
            raise MetadataError(f"unsupported local object URI: {uri}")
        path = Path(unquote(parsed.path)).resolve()
        if not path.is_relative_to(self._root):
            raise MetadataError("object URI escapes configured storage root")
        return path


class MemoryObjectStorage:
    """Deterministic object-storage adapter for interface-level tests."""

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    def put(
        self,
        key: str,
        content: bytes | Iterable[bytes],
        *,
        kind: str,
        run_id: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> StoredArtifact:
        chunks = (content,) if isinstance(content, bytes) else content
        payload = b"".join(chunks)
        checksum = hashlib.sha256(payload).hexdigest()
        uri = f"memory://{key}"
        self._objects[uri] = payload
        return StoredArtifact(
            id=uuid.uuid5(uuid.NAMESPACE_URL, f"{uri}:{checksum}").hex,
            run_id=run_id,
            kind=kind,
            uri=uri,
            checksum=checksum,
            size_bytes=len(payload),
            metadata=dict(metadata or {}),
            created_at=utc_now(),
        )

    def read(self, uri: str) -> bytes:
        try:
            return self._objects[uri]
        except KeyError as exc:
            raise FileNotFoundError(uri) from exc

    def delete(self, uri: str) -> None:
        self._objects.pop(uri, None)

    def exists(self, uri: str) -> bool:
        return uri in self._objects
