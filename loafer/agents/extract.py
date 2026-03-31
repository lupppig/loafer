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
            stream_iter: Iterator[list[dict[str, Any]]] = connector.stream(
                state.get("chunk_size", 500)
            )
            state["stream_iterator"] = stream_iter
            state["rows_extracted"] = count if count is not None else 0

            peek_first_chunk(state, connector)
        else:
            raw_data: list[dict[str, Any]] = connector.read_all()
            state["raw_data"] = raw_data
            state["rows_extracted"] = len(raw_data)

        state["schema_sample"] = build_schema_sample(
            _get_sample_data(state),
            max_sample_rows=5,
        )

        if state.get("rows_extracted", 0) == 0:
            state.setdefault("warnings", []).append("Source returned 0 rows")

    except Exception:
        connector.disconnect()
        raise
    finally:
        connector.disconnect()

    state["duration_ms"]["extract"] = (time.monotonic() - start) * 1000
    return state


def peek_first_chunk(state: PipelineState, connector: SourceConnector) -> None:
    """Peek the first chunk from the stream for schema sampling.

    The chunk is stored in state so the next agent can prepend it
    before consuming the rest of the stream.
    """
    stream_iter: Iterator[list[dict[str, Any]]] | None = state.get("stream_iterator")
    if stream_iter is None:
        return

    try:
        first_chunk: list[dict[str, Any]] = next(stream_iter)
        state["_first_chunk"] = first_chunk
    except StopIteration:
        state["_first_chunk"] = []


def _get_sample_data(state: PipelineState) -> list[dict[str, Any]]:
    """Get data suitable for schema sampling."""
    if not state.get("is_streaming"):
        return state.get("raw_data", [])

    first_chunk: list[dict[str, Any]] | None = state.get("_first_chunk")
    if first_chunk:
        return first_chunk

    return []
