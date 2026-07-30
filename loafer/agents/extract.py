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

    from loafer.graph.state import PipelineState
    from loafer.ports.connector import SourceConnector

_INCREMENTAL_PUSHDOWN_SOURCES = {"postgres", "mysql", "sqlite", "rest_api"}


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
        yield from self._source

    def __next__(self) -> list[dict[str, Any]]:
        return next(self._source)


def extract_agent(state: PipelineState) -> PipelineState:
    """Extract data from the configured source.

    Returns the updated PipelineState with raw_data or stream_iterator
    populated, schema_sample built, and streaming flags set.
    """
    start = time.monotonic()

    source_config = state["source_config"]
    incremental = state.get("incremental_config")
    cursor_column: str | None = getattr(incremental, "column", None)
    source_type = (
        source_config.get("type")
        if isinstance(source_config, dict)
        else getattr(source_config, "type", None)
    )
    client_side_incremental = (
        incremental is not None and source_type not in _INCREMENTAL_PUSHDOWN_SOURCES
    )

    connector: SourceConnector
    if incremental is not None and not client_side_incremental:
        param = getattr(incremental, "param", None) or cursor_column
        connector = get_source_connector(
            source_config,
            incremental_column=cursor_column,
            incremental_param=param,
            cursor_value=state.get("cursor_value"),
        )
    else:
        connector = get_source_connector(source_config)

    if client_side_incremental:
        state.setdefault("warnings", []).append(
            f"Incremental filtering for source type '{source_type}' is applied client-side "
            "and scans the source"
        )

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

        state["new_cursor"] = state.get("cursor_value")

        if is_streaming:
            raw_iter: Iterator[list[dict[str, Any]]] = connector.stream(
                state.get("chunk_size", 500)
            )
            if client_side_incremental and cursor_column is not None:
                raw_iter = _filter_incremental_stream(
                    raw_iter,
                    cursor_column,
                    state.get("cursor_value"),
                )
            peekable = _PeekableStream(raw_iter)
            peekable_stream = _counting_stream(
                peekable,
                state,
                cursor_column,
                connector,
            )
            state["stream_iterator"] = peekable_stream
            state["rows_extracted"] = (
                count if count is not None and not client_side_incremental else 0
            )

            first_chunk = peekable.peek()
            state["schema_sample"] = build_schema_sample(
                first_chunk,
                max_sample_rows=5,
            )
        else:
            raw_data: list[dict[str, Any]] = connector.read_all()
            if client_side_incremental and cursor_column is not None:
                from loafer.core.incremental import filter_rows_after_cursor

                raw_data = filter_rows_after_cursor(
                    raw_data,
                    cursor_column,
                    state.get("cursor_value"),
                )
            state["raw_data"] = raw_data
            state["rows_extracted"] = len(raw_data)

            if cursor_column is not None:
                from loafer.core.incremental import max_cursor

                state["new_cursor"] = max_cursor(raw_data, cursor_column, state.get("cursor_value"))

            state["schema_sample"] = build_schema_sample(
                raw_data,
                max_sample_rows=5,
            )

        # Only warn here when the true count is known. In streaming mode the
        # count is often None until the stream is drained (Postgres always),
        # so rows_extracted is a placeholder 0 at this point — warning now
        # would fire a false "Source returned 0 rows" on every successful
        # streaming extract (BUG-5). The deferred warning is emitted from
        # _counting_stream once the real total is known.
        if not is_streaming and state.get("rows_extracted", 0) == 0:
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
    cursor_column: str | None = None,
    connector: SourceConnector | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """Wrap a stream iterator to count rows and track the max cursor as consumed."""
    total = 0
    for chunk in stream_iter:
        total += len(chunk)
        if cursor_column is not None:
            from loafer.core.incremental import max_cursor

            state["new_cursor"] = max_cursor(chunk, cursor_column, state.get("new_cursor"))
        yield chunk
    state["rows_extracted"] = total
    # The true count is only known now that the stream is drained; emit the
    # deferred 0-row warning here so it reflects reality instead of the
    # placeholder count set at extract time (BUG-5).
    if total == 0:
        state.setdefault("warnings", []).append("Source returned 0 rows")
    if connector is not None:
        state.setdefault("warnings", []).extend(connector.diagnostics())


def _filter_incremental_stream(
    stream_iter: Iterator[list[dict[str, Any]]],
    column: str,
    cursor: Any,
) -> Iterator[list[dict[str, Any]]]:
    """Filter non-pushdown source chunks without materializing the full source."""
    from loafer.core.incremental import filter_rows_after_cursor

    for chunk in stream_iter:
        filtered = filter_rows_after_cursor(chunk, column, cursor)
        if filtered:
            yield filtered
