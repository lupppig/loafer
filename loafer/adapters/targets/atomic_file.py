"""Atomic local-file publication primitives for target adapters."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import TextIO

from loafer.exceptions import LoadError


def open_temporary_text(
    destination: Path,
    *,
    newline: str | None = None,
) -> tuple[TextIO, Path]:
    """Open a hidden temporary file beside *destination*.

    Keeping both paths on the same filesystem makes the final rename atomic.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline=newline,
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
        delete=False,
    )
    return handle, Path(handle.name)


def sync_and_close(handle: TextIO) -> None:
    """Flush file data to the operating system and close the handle."""
    handle.flush()
    os.fsync(handle.fileno())
    handle.close()


def publish_temporary_file(
    temporary_path: Path,
    destination: Path,
    *,
    write_mode: str,
) -> None:
    """Publish a complete temporary file without exposing partial output."""
    try:
        if write_mode == "error":
            # A hard link is an atomic create-if-absent operation. Because the
            # temporary file is in the same directory it is on the same
            # filesystem, and concurrent publishers cannot overwrite a winner.
            os.link(temporary_path, destination)
            try:
                temporary_path.unlink()
            except OSError:
                # Publication succeeded. A hidden cleanup artifact is safer
                # than reporting failure after the final file became visible.
                pass
        else:
            os.replace(temporary_path, destination)
    except FileExistsError as exc:
        raise LoadError(f"Output file already exists: {destination}") from exc
    except OSError as exc:
        raise LoadError(f"Failed to publish output file {destination}: {exc}") from exc


def discard_temporary_file(handle: TextIO | None, temporary_path: Path | None) -> None:
    """Close and remove an unpublished temporary file."""
    if handle is not None and not handle.closed:
        handle.close()
    if temporary_path is not None:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
