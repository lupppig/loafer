#!/usr/bin/env python3
"""Run a deterministic CSV → custom transform → CSV volume benchmark.

The harness measures the complete Loafer subprocess and its worker children,
enforces RSS and wall-time limits, and verifies exact row count and SHA-256
equality. It is Linux-only because process-tree RSS is sampled from /proc.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.metadata
import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

_MIB = 1024 * 1024
_SAMPLE_INTERVAL_SECONDS = 0.1


@dataclass(frozen=True)
class MonitoredProcess:
    returncode: int
    wall_seconds: float
    peak_rss_bytes: int
    cpu_user_seconds: float
    cpu_system_seconds: float
    termination_reason: str | None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _csv_data_row_count(path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def _generate_input(path: Path, rows: int) -> float:
    started = time.monotonic()
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "value", "bucket", "active"])
        for row_id in range(rows):
            writer.writerow(
                [
                    row_id,
                    f"value_{row_id:012d}",
                    row_id % 1000,
                    "true" if row_id % 2 == 0 else "false",
                ]
            )
    return time.monotonic() - started


def _process_group_rss_bytes(process_group: int) -> int:
    """Return summed resident bytes for processes in a Linux process group."""
    total_pages = 0
    page_size = os.sysconf("SC_PAGE_SIZE")

    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text(encoding="utf-8")
            fields = stat[stat.rfind(")") + 2 :].split()
            # fields[0] is state (field 3 in procfs), fields[2] is pgrp
            # (field 5), and fields[21] is RSS pages (field 24).
            if int(fields[2]) == process_group:
                total_pages += int(fields[21])
        except (
            FileNotFoundError,
            ProcessLookupError,
            IndexError,
            PermissionError,
            ValueError,
        ):
            continue

    return total_pages * page_size


def _terminate_process_group(process: subprocess.Popen[Any]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=5)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()


def _run_monitored(
    command: list[str],
    *,
    cwd: Path,
    stdout_path: Path,
    stderr_path: Path,
    timeout_seconds: int,
    rss_limit_bytes: int,
) -> MonitoredProcess:
    started = time.monotonic()
    times_before = os.times()
    peak_rss_bytes = 0
    termination_reason: str | None = None

    environment = os.environ.copy()
    environment["PYTHONHASHSEED"] = "0"

    with (
        stdout_path.open("w", encoding="utf-8") as stdout,
        stderr_path.open("w", encoding="utf-8") as stderr,
    ):
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=environment,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )

        while process.poll() is None:
            rss_bytes = _process_group_rss_bytes(process.pid)
            peak_rss_bytes = max(peak_rss_bytes, rss_bytes)
            elapsed = time.monotonic() - started

            if rss_bytes > rss_limit_bytes:
                termination_reason = "rss_limit_exceeded"
                _terminate_process_group(process)
                break
            if elapsed > timeout_seconds:
                termination_reason = "timeout"
                _terminate_process_group(process)
                break
            time.sleep(_SAMPLE_INTERVAL_SECONDS)

        returncode = process.wait()
        peak_rss_bytes = max(peak_rss_bytes, _process_group_rss_bytes(process.pid))

    times_after = os.times()
    return MonitoredProcess(
        returncode=returncode,
        wall_seconds=time.monotonic() - started,
        peak_rss_bytes=peak_rss_bytes,
        cpu_user_seconds=max(0.0, times_after.children_user - times_before.children_user),
        cpu_system_seconds=max(0.0, times_after.children_system - times_before.children_system),
        termination_reason=termination_reason,
    )


def _preflight(work_directory: Path, rows: int) -> None:
    if not Path("/proc").is_dir():
        raise RuntimeError("full-pipeline RSS benchmarking requires Linux /proc")
    if rows <= 0:
        raise ValueError("--rows must be positive")

    # Two CSV files plus temporary publication and safety headroom. The fixed
    # benchmark row is currently under 64 bytes, so 192 bytes/row is a
    # deliberately conservative working-space estimate.
    required_bytes = rows * 192 + 256 * _MIB
    free_bytes = shutil.disk_usage(work_directory).free
    if free_bytes < required_bytes:
        raise RuntimeError(
            f"insufficient free disk: need approximately {required_bytes / _MIB:.0f} MiB, "
            f"have {free_bytes / _MIB:.0f} MiB"
        )


def _git_revision(repository: Path) -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _git_worktree_dirty(repository: Path) -> bool | None:
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=repository,
        capture_output=True,
        text=True,
        check=False,
    )
    return bool(result.stdout) if result.returncode == 0 else None


def _tail(path: Path, max_characters: int = 4000) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-max_characters:]


def _verified_throughput(rows: int, wall_seconds: float, *, correct: bool) -> float | None:
    """Return throughput only when all requested rows were verified."""
    if not correct or wall_seconds <= 0:
        return None
    return round(rows / wall_seconds, 3)


def run_benchmark(
    *,
    repository: Path,
    work_directory: Path,
    rows: int,
    chunk_size: int,
    timeout_seconds: int,
    rss_limit_mb: int,
    sandbox_memory_mb: int,
) -> dict[str, Any]:
    """Execute one benchmark and return a machine-readable report."""
    work_directory.mkdir(parents=True, exist_ok=True)
    _preflight(work_directory, rows)

    input_path = work_directory / "input.csv"
    output_path = work_directory / "output.csv"
    transform_path = work_directory / "identity.py"
    config_path = work_directory / "pipeline.yaml"
    stdout_path = work_directory / "pipeline.stdout.log"
    stderr_path = work_directory / "pipeline.stderr.log"

    generation_seconds = _generate_input(input_path, rows)
    transform_path.write_text("def transform(data):\n    return data\n", encoding="utf-8")
    config_path.write_text(
        "\n".join(
            [
                "name: full_pipeline_volume_benchmark",
                "mode: etl",
                "source:",
                "  type: csv",
                f"  path: {json.dumps(str(input_path))}",
                "target:",
                "  type: csv",
                f"  path: {json.dumps(str(output_path))}",
                "  write_mode: overwrite",
                "transform:",
                "  type: custom",
                f"  path: {json.dumps(str(transform_path))}",
                "execution:",
                "  transform_class: row_local",
                "  schema_drift: fail",
                f"chunk_size: {chunk_size}",
                "streaming_threshold: 1",
                "sandbox:",
                f"  timeout: {timeout_seconds}",
                f"  max_memory_mb: {sandbox_memory_mb}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    command = [
        sys.executable,
        "-m",
        "loafer",
        "run",
        str(config_path),
        "--quiet",
        "--yes",
    ]
    process = _run_monitored(
        command,
        cwd=repository,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        timeout_seconds=timeout_seconds,
        rss_limit_bytes=rss_limit_mb * _MIB,
    )

    input_sha256 = _sha256(input_path)
    output_exists = output_path.exists()
    output_rows = _csv_data_row_count(output_path) if output_exists else None
    output_sha256 = _sha256(output_path) if output_exists else None
    temporary_outputs = sorted(path.name for path in work_directory.glob(".output.csv.*.tmp"))
    correct = (
        process.returncode == 0
        and process.termination_reason is None
        and output_rows == rows
        and output_sha256 == input_sha256
        and not temporary_outputs
    )

    if correct:
        status = "succeeded"
    elif process.termination_reason is not None:
        status = process.termination_reason
    elif process.returncode != 0:
        status = "pipeline_failed"
    else:
        status = "incorrect_output"

    try:
        loafer_version = importlib.metadata.version("loafer-etl")
    except importlib.metadata.PackageNotFoundError:
        loafer_version = "uninstalled"

    report: dict[str, Any] = {
        "schema_version": 1,
        "status": status,
        "correct": correct,
        "rows_requested": rows,
        "rows_output": output_rows,
        "chunk_size": chunk_size,
        "transform_class": "custom_identity_row_local",
        "generation_seconds": round(generation_seconds, 6),
        "wall_seconds": round(process.wall_seconds, 6),
        "throughput_rows_per_second": _verified_throughput(
            rows,
            process.wall_seconds,
            correct=correct,
        ),
        "peak_process_tree_rss_bytes": process.peak_rss_bytes,
        "peak_process_tree_rss_mb": round(process.peak_rss_bytes / _MIB, 3),
        "rss_limit_mb": rss_limit_mb,
        "timeout_seconds": timeout_seconds,
        "sandbox_memory_mb": sandbox_memory_mb,
        "cpu_user_seconds": round(process.cpu_user_seconds, 6),
        "cpu_system_seconds": round(process.cpu_system_seconds, 6),
        "returncode": process.returncode,
        "termination_reason": process.termination_reason,
        "output_published": output_exists,
        "input_bytes": input_path.stat().st_size,
        "output_bytes": output_path.stat().st_size if output_exists else None,
        "input_sha256": input_sha256,
        "output_sha256": output_sha256,
        "temporary_outputs": temporary_outputs,
        "stderr_tail": _tail(stderr_path),
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "loafer_version": loafer_version,
            "git_revision": _git_revision(repository),
            "git_worktree_dirty": _git_worktree_dirty(repository),
        },
        "process": asdict(process),
    }
    return report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows", type=int, required=True, help="Number of deterministic input rows"
    )
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--rss-limit-mb", type=int, default=2048)
    parser.add_argument("--sandbox-memory-mb", type=int, default=1536)
    parser.add_argument(
        "--work-directory",
        type=Path,
        help="Keep inputs, outputs, logs, and config in this directory",
    )
    parser.add_argument("--report", type=Path, help="Write the JSON report to this path")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    repository = Path(__file__).resolve().parents[1]

    temporary_directory: tempfile.TemporaryDirectory[str] | None = None
    if args.work_directory is None:
        temporary_directory = tempfile.TemporaryDirectory(prefix="loafer-benchmark-")
        work_directory = Path(temporary_directory.name)
    else:
        work_directory = args.work_directory.resolve()

    try:
        report = run_benchmark(
            repository=repository,
            work_directory=work_directory,
            rows=args.rows,
            chunk_size=args.chunk_size,
            timeout_seconds=args.timeout_seconds,
            rss_limit_mb=args.rss_limit_mb,
            sandbox_memory_mb=args.sandbox_memory_mb,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        report = {
            "schema_version": 1,
            "status": "preflight_failed",
            "correct": False,
            "error": str(exc),
            "rows_requested": args.rows,
        }

    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(f"{rendered}\n", encoding="utf-8")

    if temporary_directory is not None:
        temporary_directory.cleanup()
    return 0 if report.get("correct") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
