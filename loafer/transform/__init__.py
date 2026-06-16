"""Transform runner interface and dispatch.

Every transform mode (ai, custom, sql) implements this ABC.  The
Transform Agent delegates to the correct runner — it never contains
mode-specific logic itself.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from loafer.exceptions import TransformError
from loafer.graph.state import PipelineState


class TransformRunner(ABC):
    """Base class for all transform execution modes."""

    @abstractmethod
    def run(self, state: PipelineState) -> PipelineState:
        """Execute the transform and return the updated state."""


def materialize_input_rows(state: PipelineState) -> list[dict[str, Any]]:
    """Return the rows to transform, draining the stream in streaming mode.

    In streaming mode the extract agent leaves ``raw_data`` empty and puts a
    chunked iterator in ``stream_iterator``; any runner that needs the full
    dataset in memory (custom and AI transforms) must drain it here. The true
    row count is recorded back into ``rows_extracted``.

    A stream can only be consumed once, so callers must invoke this exactly
    once at the start of a run and reuse the returned list.
    """
    if not state.get("is_streaming", False):
        return list(state.get("raw_data", []))

    stream_iter = state.get("stream_iterator")
    if stream_iter is None:
        raise TransformError("stream_iterator is None in streaming mode")

    rows: list[dict[str, Any]] = []
    for chunk in stream_iter:
        rows.extend(chunk)
    state["rows_extracted"] = len(rows)
    return rows


__all__ = ["TransformRunner", "materialize_input_rows"]
