"""Transform runner interface and dispatch.

Every transform mode (ai, custom, sql) implements this ABC.  The
Transform Agent delegates to the correct runner — it never contains
mode-specific logic itself.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from loafer.graph.state import PipelineState


class TransformRunner(ABC):
    """Base class for all transform execution modes."""

    @abstractmethod
    def run(self, state: PipelineState) -> PipelineState:
        """Execute the transform and return the updated state."""


__all__ = ["TransformRunner"]
