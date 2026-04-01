"""Tests for daemon management."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest


class TestDaemonHelpers:
    """Tests for daemon utility functions."""

    def test_ensure_dir_creates_directory(self, tmp_path: Path, monkeypatch: Any) -> None:
        """_ensure_dir should create the .loafer directory."""
        from loafer import daemon

        test_dir = tmp_path / ".loafer"
        monkeypatch.setattr(daemon, "_LOAFER_DIR", test_dir)
        monkeypatch.setattr(daemon, "_PID_FILE", test_dir / "scheduler.pid")
        monkeypatch.setattr(daemon, "_LOG_FILE", test_dir / "scheduler.log")

        daemon._ensure_dir()
        assert test_dir.exists()

    def test_read_pid_returns_int(self, tmp_path: Path, monkeypatch: Any) -> None:
        """_read_pid should return the PID from the file."""
        from loafer import daemon

        pid_file = tmp_path / "scheduler.pid"
        pid_file.write_text("12345")
        monkeypatch.setattr(daemon, "_PID_FILE", pid_file)

        result = daemon._read_pid()
        assert result == 12345

    def test_read_pid_returns_none_for_invalid_content(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        """_read_pid should return None for invalid content."""
        from loafer import daemon

        pid_file = tmp_path / "scheduler.pid"
        pid_file.write_text("not_a_number")
        monkeypatch.setattr(daemon, "_PID_FILE", pid_file)

        result = daemon._read_pid()
        assert result is None

    def test_read_pid_returns_none_for_missing_file(self, tmp_path: Path, monkeypatch: Any) -> None:
        """_read_pid should return None if file doesn't exist."""
        from loafer import daemon

        pid_file = tmp_path / "scheduler.pid"
        monkeypatch.setattr(daemon, "_PID_FILE", pid_file)

        result = daemon._read_pid()
        assert result is None

    def test_process_alive_returns_true_for_current_process(self) -> None:
        """_process_alive should return True for the current process."""
        from loafer import daemon

        assert daemon._process_alive(os.getpid()) is True

    def test_process_alive_returns_false_for_nonexistent_pid(self) -> None:
        """_process_alive should return False for a non-existent PID."""
        from loafer import daemon

        assert daemon._process_alive(999999) is False

    def test_get_log_path(self) -> None:
        """get_log_path should return the expected log file path."""
        from loafer.daemon import get_log_path, _LOG_FILE

        assert get_log_path() == _LOG_FILE

    def test_stop_daemon_no_pid_file(self, tmp_path: Path, monkeypatch: Any) -> None:
        """stop_daemon should return False when no PID file exists."""
        from loafer import daemon

        pid_file = tmp_path / "scheduler.pid"
        monkeypatch.setattr(daemon, "_PID_FILE", pid_file)

        result = daemon.stop_daemon()
        assert result is False

    def test_stop_daemon_stale_pid_file(self, tmp_path: Path, monkeypatch: Any) -> None:
        """stop_daemon should clean up stale PID files."""
        from loafer import daemon

        pid_file = tmp_path / "scheduler.pid"
        pid_file.write_text("999999")
        monkeypatch.setattr(daemon, "_PID_FILE", pid_file)

        result = daemon.stop_daemon()
        assert result is True
        assert not pid_file.exists()

    def test_get_daemon_status_not_running(self, tmp_path: Path, monkeypatch: Any) -> None:
        """get_daemon_status should return False when not running."""
        from loafer import daemon

        pid_file = tmp_path / "scheduler.pid"
        log_file = tmp_path / "scheduler.log"
        monkeypatch.setattr(daemon, "_PID_FILE", pid_file)
        monkeypatch.setattr(daemon, "_LOG_FILE", log_file)

        running, pid, log = daemon.get_daemon_status()
        assert running is False
        assert pid is None
        assert log == log_file
