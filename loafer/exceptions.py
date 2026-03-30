"""Loafer exception hierarchy.

Every exception inherits from LoaferError so callers can catch the entire tree.
Leaf exceptions carry specific context for actionable error messages.
"""


class LoaferError(Exception):
    """Base exception for all Loafer errors."""


class ConfigError(LoaferError):
    """Invalid or missing configuration."""


class ConnectorError(LoaferError):
    """Base for all connector-related failures."""


class ExtractionError(ConnectorError):
    """Source connector failed to read data."""


class LoadError(ConnectorError):
    """Target connector failed to write data."""


class ValidationError(LoaferError):
    """Data validation failed."""


class TransformError(LoaferError):
    """Transform execution failed."""


class LLMError(LoaferError):
    """Base for LLM provider failures."""


class LLMRateLimitError(LLMError):
    """LLM provider rate-limited the request."""


class LLMInvalidOutputError(LLMError):
    """LLM returned unparseable or empty output."""


class SchedulerError(LoaferError):
    """Job scheduling failure."""


class PipelineError(LoaferError):
    """Pipeline orchestration failure."""
