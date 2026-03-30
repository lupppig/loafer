"""Provider-agnostic LLM interface.

All LLM interactions in Loafer go through ``LLMProvider``.  Concrete
implementations (Gemini, Claude, OpenAI, …) live in separate modules.
Agent code never imports a specific provider — it receives one via
the pipeline state / dependency injection.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class TransformPromptResult:
    """Result of an ETL transform function generation request."""

    code: str
    """The generated Python function as a string."""

    raw_response: str
    """Full LLM response for debugging."""

    token_usage: dict[str, int] = field(default_factory=dict)
    """Token counts: prompt_tokens, completion_tokens, total_tokens."""


@dataclass(frozen=True, slots=True)
class ELTSQLResult:
    """Result of an ELT SQL generation request."""

    sql: str
    """The generated SQL SELECT statement."""

    raw_response: str
    """Full LLM response for debugging."""

    token_usage: dict[str, int] = field(default_factory=dict)
    """Token counts: prompt_tokens, completion_tokens, total_tokens."""


class LLMProvider(ABC):
    """Abstract interface every LLM backend must implement."""

    @abstractmethod
    def generate_transform_function(
        self,
        schema_sample: dict[str, object],
        instruction: str,
        previous_error: str | None = None,
        previous_code: str | None = None,
    ) -> TransformPromptResult:
        """Generate a Python ``transform(data)`` function."""

    @abstractmethod
    def generate_elt_sql(
        self,
        target_schema: dict[str, object],
        raw_table_name: str,
        instruction: str,
        previous_error: str | None = None,
    ) -> ELTSQLResult:
        """Generate a SQL SELECT for ELT in-target transformation."""
