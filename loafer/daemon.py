"""Background daemon management for the Loafer scheduler.

Handles PID file creation, process spawning, and log file management
for running the scheduler as a background service.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from pathlib import Path

_LOAFER_DIR = Path.home() / ".loafer"
_PID_FILE = _LOAFER_DIR / "scheduler.pid"
_LOG_FILE = _LOAFER_DIR / "scheduler.log"


def _ensure_dir() -> None:
    """Create the .loafer directory if it doesn't exist."""
    _LOAFER_DIR.mkdir(parents=True, exist_ok=True)


def start_daemon() -> tuple[int, Path]:
    """Start the scheduler as a background daemon.

    Returns:
        (pid, log_path) — the daemon's PID and log file path.
    """
    _ensure_dir()

    # Check if already running
    if _is_running():
        pid = _read_pid()
        raise RuntimeError(f"Scheduler already running (PID {pid})")

    # Spawn ourselves with --daemon flag
    cmd = [sys.executable, "-m", "loafer.cli", "_daemon_entry"]
    log_file = open(_LOG_FILE, "a")

    process = subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=log_file,
        start_new_session=True,
        cwd=str(Path.cwd()),
    )

    # Write PID file
    _PID_FILE.write_text(str(process.pid))

    return process.pid, _LOG_FILE


def stop_daemon() -> bool:
    """Stop the background scheduler daemon.

    Returns:
        True if a process was stopped, False if none was running.
    """
    if not _PID_FILE.exists():
        return False

    pid = _read_pid()
    if pid is None:
        _PID_FILE.unlink(missing_ok=True)
        return False

    try:
        os.kill(pid, signal.SIGTERM)
        # Wait briefly for graceful shutdown
        import time

        for _ in range(10):
            try:
                os.kill(pid, 0)  # Check if still alive
                time.sleep(0.1)
            except OSError:
                break
        else:
            # Force kill if still alive
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
    except OSError:
        pass  # Process already dead

    _PID_FILE.unlink(missing_ok=True)
    return True


def get_daemon_status() -> tuple[bool, int | None, Path]:
    """Check if the daemon is running.

    Returns:
        (is_running, pid, log_path)
    """
    if not _PID_FILE.exists():
        return False, None, _LOG_FILE

    pid = _read_pid()
    if pid is None:
        _PID_FILE.unlink(missing_ok=True)
        return False, None, _LOG_FILE

    if _process_alive(pid):
        return True, pid, _LOG_FILE

    # Stale PID file
    _PID_FILE.unlink(missing_ok=True)
    return False, None, _LOG_FILE


def _is_running() -> bool:
    """Check if the daemon is currently running."""
    if not _PID_FILE.exists():
        return False
    pid = _read_pid()
    if pid is None:
        return False
    return _process_alive(pid)


def _read_pid() -> int | None:
    """Read the PID from the PID file."""
    try:
        return int(_PID_FILE.read_text().strip())
    except (ValueError, OSError):
        return None


def _process_alive(pid: int) -> bool:
    """Check if a process with the given PID is alive."""
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def get_log_path() -> Path:
    """Return the path to the scheduler log file."""
    return _LOG_FILE


def tail_log(path: Path, lines: int = 50) -> None:
    """Print the last N lines of the log file."""
    from rich.console import Console

    console = Console()
    if not path.exists():
        console.print("[dim]No log file found.[/dim]")
        return

    content = path.read_text()
    all_lines = content.splitlines()
    for line in all_lines[-lines:]:
        console.print(f"  [dim]{line}[/dim]")
