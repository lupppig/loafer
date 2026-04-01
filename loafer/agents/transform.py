"""Transform Agent — pure function over PipelineState.

Routes to the correct TransformRunner (ai, custom, sql) based on
transform_config.type.  The agent itself contains no mode-specific logic.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loafer.exceptions import TransformError

if TYPE_CHECKING:
    from loafer.graph.state import PipelineState
    from loafer.transform import TransformRunner


def transform_agent(state: PipelineState) -> PipelineState:
    """Execute the configured transform.

    Routes to the correct runner based on transform_config.type.
    Returns the updated PipelineState with transformed_data populated.
    """

    transform_config = state.get("transform_config")
    transform_type: str = transform_config.type if transform_config else "ai"

    runner: TransformRunner = _resolve_runner(transform_type)
    return runner.run(state)


def _resolve_runner(transform_type: str) -> TransformRunner:
    """Instantiate the correct TransformRunner for the given type."""
    from loafer.transform.ai_runner import AiTransformRunner
    from loafer.transform.custom_runner import CustomTransformRunner
    from loafer.transform.sql_runner import SqlTransformRunner

    match transform_type:
        case "ai":
            return AiTransformRunner()
        case "custom":
            return CustomTransformRunner()
        case "sql":
            return SqlTransformRunner()
        case _:
            raise TransformError(
                f"Unknown transform type: '{transform_type}'. Expected one of: ai, custom, sql"
            )
