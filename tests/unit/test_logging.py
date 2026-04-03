"""Tests for logging module."""

from __future__ import annotations

import pytest

from loafer.logging import configure_logging, get_logger


class TestConfigureLogging:
    def test_default_config_does_not_raise(self) -> None:
        configure_logging()

    def test_verbose_config_does_not_raise(self) -> None:
        configure_logging(verbose=True)

    def test_json_config_does_not_raise(self) -> None:
        configure_logging(verbose=False, json=True)


class TestGetLogger:
    def test_returns_logger(self) -> None:
        configure_logging()
        logger = get_logger("test")
        assert logger is not None

    def test_logger_can_log(self) -> None:
        configure_logging()
        logger = get_logger("test")
        logger.info("test message")  # should not raise
