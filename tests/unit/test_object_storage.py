from __future__ import annotations

from pathlib import Path

import pytest

from loafer.adapters.object_storage import FilesystemObjectStorage, MemoryObjectStorage
from loafer.exceptions import MetadataError


@pytest.mark.parametrize("adapter", ["filesystem", "memory"])
def test_object_storage_contract(adapter: str, tmp_path: Path) -> None:
    storage = (
        FilesystemObjectStorage(tmp_path / "objects")
        if adapter == "filesystem"
        else MemoryObjectStorage()
    )
    artifact = storage.put(
        "runs/run-1/logs/worker.log",
        [b"first\n", b"second\n"],
        kind="log",
        run_id="run-1",
    )

    assert storage.exists(artifact.uri)
    assert storage.read(artifact.uri) == b"first\nsecond\n"
    assert artifact.size_bytes == 13
    assert len(artifact.checksum) == 64

    storage.delete(artifact.uri)
    assert not storage.exists(artifact.uri)


def test_filesystem_storage_rejects_path_escape(tmp_path: Path) -> None:
    storage = FilesystemObjectStorage(tmp_path / "objects")

    with pytest.raises(MetadataError, match="unsafe object key"):
        storage.put("../secret", b"nope", kind="artifact")
