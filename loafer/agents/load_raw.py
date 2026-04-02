"""Load Raw Agent — ELT-only pure function over PipelineState.

Loads raw extracted data into the target database as a staging table.
Sets raw_table_name in state for the transform-in-target agent.
"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any

from loafer.connectors.registry import get_target_connector
from loafer.exceptions import LoadError

if TYPE_CHECKING:
    from loafer.connectors.base import TargetConnector
    from loafer.graph.state import PipelineState


def load_raw_agent(state: PipelineState) -> PipelineState:
    """Load raw data into target as a staging table.

    Returns the updated PipelineState with raw_table_name set.
    """
    start = time.monotonic()

    target_config = state["target_config"]
    connector: TargetConnector = get_target_connector(target_config)

    try:
        connector.connect()
    except Exception as exc:
        connector.disconnect()
        raise LoadError(f"Failed to connect to target for raw load: {exc}") from exc

    try:
        staging_table = _build_staging_table_name(target_config)
        state["raw_table_name"] = staging_table

        is_streaming: bool = state.get("is_streaming", False)
        chunk_size: int = state.get("chunk_size", 500)
        total_loaded: int = 0

        if is_streaming:
            total_loaded = _write_stream_raw(connector, state, chunk_size)
        else:
            raw_data: list[dict[str, Any]] = state.get("raw_data", [])
            for i in range(0, len(raw_data), chunk_size):
                chunk = raw_data[i : i + chunk_size]
                written = connector.write_chunk(chunk)
                total_loaded += written

        connector.finalize()

    except LoadError:
        raise
    except Exception as exc:
        raise LoadError(f"Raw load failed after writing {total_loaded} rows: {exc}") from exc
    finally:
        connector.disconnect()

    state["rows_extracted"] = total_loaded
    state["duration_ms"]["load_raw"] = (time.monotonic() - start) * 1000
    return state


def _build_staging_table_name(target_config: Any) -> str:
    """Generate a unique staging table name."""
    target_type = target_config.type if hasattr(target_config, "type") else "unknown"
    unique_id = uuid.uuid4().hex[:8]
    return f"loafer_raw_{target_type}_{unique_id}"


def _write_stream_raw(
    connector: TargetConnector,
    state: PipelineState,
    chunk_size: int,
) -> int:
    """Consume the stream iterator and write each chunk to the target as raw data."""
    total_loaded = 0

    first_chunk = state.get("_first_chunk")
    if isinstance(first_chunk, list):
        written = connector.write_chunk(first_chunk)
        total_loaded += written

    stream_iter = state.get("stream_iterator")
    if stream_iter is None:
        return total_loaded

    for chunk in stream_iter:
        written = connector.write_chunk(chunk)
        total_loaded += written

    return total_loaded
