"""Execute validated transform code in an isolated, resource-limited worker.

On POSIX the code runs in a spawned subprocess with CPU and address-space
rlimits and a hard wall-clock kill, so a runaway or hostile transform becomes a
killed worker with a clear error rather than a host hang or compromise. On
platforms without ``resource`` (Windows) it falls back to an in-process thread
with a best-effort timeout and a one-time warning that limits are not enforced.

The AST denylist in ``loafer.transform.code_validator`` runs *before* this; the
sandbox is the enforcement layer behind it.
"""

from __future__ import annotations

import contextlib
import pickle
import subprocess
import sys
from typing import Any

from loafer.exceptions import TransformError
from loafer.transform._safe_exec import build_safe_globals

try:
    import resource as _resource
except ImportError:  # pragma: no cover - Windows
    _resource = None  # type: ignore[assignment]

_POSIX = _resource is not None
_warned_no_limits = False


def _run_transform(code: str, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """exec the code in a restricted namespace and call transform(data)."""
    g = build_safe_globals()
    exec(code, g)
    fn = g.get("transform")
    if fn is None:
        raise TransformError("Code does not define a `transform` function")
    result = fn(data)
    if not isinstance(result, list):
        raise TransformError(f"Transform must return list[dict], got {type(result).__name__}")
    return result


def _apply_resource_limits(timeout: int, max_memory_mb: int) -> None:
    """Apply POSIX memory and CPU limits inside the sandbox worker."""
    if _resource is not None:
        mem_bytes = max_memory_mb * 1024 * 1024
        try:
            _resource.setrlimit(_resource.RLIMIT_AS, (mem_bytes, mem_bytes))
            _resource.setrlimit(_resource.RLIMIT_CPU, (timeout, timeout + 1))
        except (ValueError, OSError):
            pass


def _worker_main() -> None:
    """Read one request from stdin and write one result to stdout.

    This module entrypoint is intentionally independent of the caller's
    ``__main__`` module. Unlike ``multiprocessing`` with the spawn start method,
    it never re-imports and re-executes an unguarded user script.
    """
    try:
        request = pickle.loads(sys.stdin.buffer.read())
        code, data, timeout, max_memory_mb = request
        _apply_resource_limits(timeout, max_memory_mb)
        # Keep transform print() calls away from the binary result protocol.
        with contextlib.redirect_stdout(sys.stderr):
            result = _run_transform(code, data)
        response: tuple[str, Any] = ("ok", result)
    except BaseException as exc:
        response = ("err", f"{type(exc).__name__}: {exc}")

    try:
        sys.stdout.buffer.write(pickle.dumps(response, protocol=pickle.HIGHEST_PROTOCOL))
        sys.stdout.buffer.flush()
    except BaseException:
        raise SystemExit(2) from None


def run_sandboxed(
    code: str,
    data: list[dict[str, Any]],
    *,
    timeout: int = 60,
    max_memory_mb: int = 512,
) -> list[dict[str, Any]]:
    """Run ``transform(data)`` from *code* under resource limits.

    Raises ``TransformError`` on timeout, resource-limit kill, or any error
    inside the transform.
    """
    if not _POSIX:
        return _run_in_thread(code, data, timeout=timeout)

    try:
        request = pickle.dumps(
            (code, data, timeout, max_memory_mb),
            protocol=pickle.HIGHEST_PROTOCOL,
        )
    except (pickle.PickleError, TypeError, ValueError) as exc:
        raise TransformError(f"Transform input could not be serialized: {exc}") from exc

    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "loafer.core.sandbox"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            # Transform print() calls are redirected to stderr in the worker.
            # Discard them so hostile code cannot exhaust parent memory by
            # making communicate() buffer an unbounded log stream.
            stderr=subprocess.DEVNULL,
        )
    except OSError as exc:
        raise TransformError(f"Could not start transform worker: {exc}") from exc

    try:
        stdout, _ = proc.communicate(input=request, timeout=timeout + 2)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        raise TransformError(f"Transform exceeded {timeout}s timeout and was terminated") from None

    if proc.returncode != 0:
        # Killed by the kernel before reporting — almost always a limit breach.
        raise TransformError(
            f"Transform worker was killed (exit {proc.returncode}) — likely exceeded "
            f"the {max_memory_mb}MB memory or {timeout}s CPU limit"
        )

    try:
        status, payload = pickle.loads(stdout)
    except (pickle.PickleError, EOFError, TypeError, ValueError) as exc:
        raise TransformError("Transform worker returned an invalid response") from exc

    if status == "err":
        raise TransformError(f"Transform execution failed: {payload}")
    return payload  # type: ignore[no-any-return]


def _run_in_thread(
    code: str,
    data: list[dict[str, Any]],
    *,
    timeout: int,
) -> list[dict[str, Any]]:
    """Windows fallback: in-process thread with a best-effort join timeout."""
    global _warned_no_limits
    if not _warned_no_limits:
        import warnings

        warnings.warn(
            "Transform sandbox: resource limits are not enforced on this platform; "
            "running in-process with a best-effort timeout only.",
            stacklevel=2,
        )
        _warned_no_limits = True

    import threading

    box: dict[str, Any] = {}

    def _target() -> None:
        try:
            box["result"] = _run_transform(code, data)
        except BaseException as exc:
            box["error"] = f"{type(exc).__name__}: {exc}"

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise TransformError(f"Transform exceeded {timeout}s timeout")
    if "error" in box:
        raise TransformError(f"Transform execution failed: {box['error']}")
    return box.get("result", [])


if __name__ == "__main__":  # pragma: no cover - exercised through the parent process
    _worker_main()
