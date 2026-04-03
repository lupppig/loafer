"""Provider-agnostic LLM interface.

Re-exported from ``loafer.ports.llm`` for backward compatibility.
"""

from loafer.ports.llm import ELTSQLResult, LLMProvider, TransformPromptResult

__all__ = ["ELTSQLResult", "LLMProvider", "TransformPromptResult"]
