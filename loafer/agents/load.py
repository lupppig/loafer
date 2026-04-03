"""Load Agent — pure function over PipelineState.

Resolves the correct TargetConnector, writes transformed data in chunks,
and finalizes.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from loafer.connectors.registry import get_target_connector
from loafer.exceptions import LoadError

if TYPE_CHECKING:
    from loafer.connectors.base import TargetConnector
    from loafer.graph.state import PipelineState


def load_agent(state: PipelineState) -> PipelineState:
    """Load transformed data into the configured target.

    Returns the updated PipelineState with rows_loaded set.
    """
    start = time.monotonic()

    target_config = state["target_config"]
    connector: TargetConnector = get_target_connector(target_config)

    try:
        connector.connect()
    except Exception as exc:
        connector.disconnect()
        raise LoadError(f"Failed to connect to target: {exc}") from exc

    try:
        chunk_size: int = state.get("chunk_size", 500)
        total_loaded: int = 0

        is_streaming: bool = state.get("is_streaming", False)
        transformed_data: list[dict[str, Any]] = state.get("transformed_data", [])

        if is_streaming and not transformed_data:
            total_loaded = _write_stream(connector, state, chunk_size)
        else:
            for i in range(0, len(transformed_data), chunk_size):
                chunk = transformed_data[i : i + chunk_size]
                written = connector.write_chunk(chunk)
                total_loaded += written

        connector.finalize()

    except LoadError:
        raise
    except Exception as exc:
        raise LoadError(f"Load failed after writing {total_loaded} rows: {exc}") from exc
    finally:
        connector.disconnect()

    state["rows_loaded"] = total_loaded
    state["duration_ms"]["load"] = (time.monotonic() - start) * 1000

    if total_loaded == 0:
        state.setdefault("warnings", []).append(
            "No rows were written to target (transform may have filtered all data)"
        )

    return state


def _write_stream(
    connector: TargetConnector,
    state: PipelineState,
    chunk_size: int,
) -> int:
    """Write transformed data to the target.

    In streaming mode the transform agent already consumed the full stream
    (including _first_chunk) and stored results in transformed_data, so
    we write from there.
    """
    total_loaded = 0
    transformed_data: list[dict[str, Any]] = state.get("transformed_data", [])
    for i in range(0, len(transformed_data), chunk_size):
        chunk = transformed_data[i : i + chunk_size]
        written = connector.write_chunk(chunk)
        total_loaded += written
    return total_loaded
