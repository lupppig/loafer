"""Structured logging configuration for Loafer.

Uses structlog for JSON-formatted, machine-parseable logs with
consistent context (run_id, agent, etc.) across the entire pipeline.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def configure_logging(verbose: bool = False, json: bool = False) -> None:
    """Configure structlog for the entire application.

    Args:
        verbose: Enable DEBUG level. Defaults to INFO.
        json: Use JSON output. Defaults to human-readable console format.
    """
    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    if json:
        structlog.configure(
            processors=[
                *shared_processors,
                structlog.processors.dict_tracebacks,
                structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(level),
            cache_logger_on_first_use=True,
        )
    else:
        structlog.configure(
            processors=[
                *shared_processors,
                structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a structured logger bound to the given name."""
    return structlog.get_logger(name)  # type: ignore[no-any-return]
