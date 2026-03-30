"""Connector abstract base classes.

``SourceConnector`` and ``TargetConnector`` define the streaming-first
interface that every data connector must implement.  Connectors know
nothing about agents, LangGraph, or LLMs.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator


class SourceConnector(ABC):
    """Read data from an external source in a streaming fashion."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection. Raise ``ConnectorError`` on failure."""

    @abstractmethod
    def disconnect(self) -> None:
        """Release connection resources."""

    @abstractmethod
    def stream(self, chunk_size: int) -> Iterator[list[dict[str, Any]]]:
        """Yield chunks of rows.  Must be a generator — never buffer the full dataset."""

    def read_all(self) -> list[dict[str, Any]]:
        """Convenience: collect all chunks into a single list (small datasets only)."""
        return [row for chunk in self.stream(chunk_size=1000) for row in chunk]

    @abstractmethod
    def count(self) -> int | None:
        """Row count if cheap to compute, else ``None``."""

    # -- context manager -----------------------------------------------------

    def __enter__(self) -> SourceConnector:
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.disconnect()


class TargetConnector(ABC):
    """Write data to an external target in a streaming fashion."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection."""

    @abstractmethod
    def disconnect(self) -> None:
        """Release connection resources."""

    @abstractmethod
    def write_chunk(self, chunk: list[dict[str, Any]]) -> int:
        """Write a chunk. Return number of rows written."""

    @abstractmethod
    def finalize(self) -> None:
        """Flush buffers, close files, commit transactions."""

    # -- context manager -----------------------------------------------------

    def __enter__(self) -> TargetConnector:
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.finalize()
        self.disconnect()
