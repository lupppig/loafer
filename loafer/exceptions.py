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


class LLMAuthError(LLMError):
    """LLM provider rejected the API key (invalid, expired, or unauthorized).

    Non-retryable: retrying with the same bad key only wastes time and quota.
    """


class LLMInvalidOutputError(LLMError):
    """LLM returned unparseable or empty output."""


class SchedulerError(LoaferError):
    """Job scheduling failure."""


class PipelineError(LoaferError):
    """Pipeline orchestration failure."""


class QueueError(LoaferError):
    """Job transport publication or consumption failed."""


class MetadataError(LoaferError):
    """Durable metadata operation failed."""


class InvalidStateTransitionError(MetadataError):
    """A durable run, stage, or batch transition is impossible."""


class StaleFenceError(MetadataError):
    """A worker attempted to write with an expired or superseded lease."""


class IdempotencyConflictError(MetadataError):
    """An idempotency key was reused for different immutable input."""
