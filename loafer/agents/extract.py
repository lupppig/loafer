"""Extract Agent — pure function over PipelineState.

Resolves the correct SourceConnector, streams or reads all data, builds
the schema sample, and sets streaming flags.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from loafer.connectors.registry import get_source_connector
from loafer.exceptions import ExtractionError
from loafer.llm.schema import build_schema_sample

if TYPE_CHECKING:
    from collections.abc import Iterator

    from loafer.connectors.base import SourceConnector
    from loafer.graph.state import PipelineState


class _PeekableStream:
    """Stream wrapper that holds a peeked first chunk.

    The first chunk is consumed by peek() for schema sampling and
    replayed when the stream is iterated by the transform agent.
    """

    def __init__(self, source: Iterator[list[dict[str, Any]]]) -> None:
        self._source = source
        self._first_chunk: list[dict[str, Any]] | None = None
        self._peeked = False

    def peek(self) -> list[dict[str, Any]]:
        if not self._peeked:
            try:
                self._first_chunk = next(self._source)
            except StopIteration:
                self._first_chunk = []
            self._peeked = True
        return self._first_chunk or []

    def __iter__(self) -> Iterator[list[dict[str, Any]]]:
        if self._peeked and self._first_chunk:
            yield self._first_chunk
        for chunk in self._source:
            yield chunk


def extract_agent(state: PipelineState) -> PipelineState:
    """Extract data from the configured source.

    Returns the updated PipelineState with raw_data or stream_iterator
    populated, schema_sample built, and streaming flags set.
    """
    start = time.monotonic()

    source_config = state["source_config"]
    connector: SourceConnector = get_source_connector(source_config)

    try:
        connector.connect()
    except Exception as exc:
        connector.disconnect()
        raise ExtractionError(f"Failed to connect to source: {exc}") from exc

    try:
        count: int | None = connector.count()
        threshold: int = state.get("streaming_threshold", 10_000)

        is_streaming = count is None or count > threshold
        state["is_streaming"] = is_streaming

        if is_streaming:
            raw_iter: Iterator[list[dict[str, Any]]] = connector.stream(
                state.get("chunk_size", 500)
            )
            peekable = _PeekableStream(raw_iter)
            peekable_stream = _counting_stream(peekable, state)
            state["stream_iterator"] = peekable_stream
            state["rows_extracted"] = count if count is not None else 0

            first_chunk = peekable.peek()
            state["schema_sample"] = build_schema_sample(
                first_chunk,
                max_sample_rows=5,
            )
        else:
            raw_data: list[dict[str, Any]] = connector.read_all()
            state["raw_data"] = raw_data
            state["rows_extracted"] = len(raw_data)

            state["schema_sample"] = build_schema_sample(
                raw_data[:5],
                max_sample_rows=5,
            )

        if state.get("rows_extracted", 0) == 0:
            state.setdefault("warnings", []).append("Source returned 0 rows")

    except Exception:
        connector.disconnect()
        raise
    finally:
        if not is_streaming:
            connector.disconnect()
        else:
            state["_source_connector"] = connector

    state["duration_ms"]["extract"] = (time.monotonic() - start) * 1000
    return state


def _counting_stream(
    stream_iter: Iterator[list[dict[str, Any]]],
    state: PipelineState,
) -> Iterator[list[dict[str, Any]]]:
    """Wrap a stream iterator to count total rows as they're consumed."""
    total = 0
    for chunk in stream_iter:
        total += len(chunk)
        yield chunk
    state["rows_extracted"] = total
