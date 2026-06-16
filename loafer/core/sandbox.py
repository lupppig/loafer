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

import multiprocessing
import queue as _queue
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


def _child_entry(
    code: str,
    data: list[dict[str, Any]],
    timeout: int,
    max_memory_mb: int,
    out: Any,
) -> None:
    """Subprocess entry: apply rlimits, run the transform, report via the queue."""
    if _resource is not None:
        mem_bytes = max_memory_mb * 1024 * 1024
        try:
            _resource.setrlimit(_resource.RLIMIT_AS, (mem_bytes, mem_bytes))
            _resource.setrlimit(_resource.RLIMIT_CPU, (timeout, timeout + 1))
        except (ValueError, OSError):
            pass

    try:
        result = _run_transform(code, data)
        out.put(("ok", result))
    except BaseException as exc:
        out.put(("err", f"{type(exc).__name__}: {exc}"))


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

    ctx = multiprocessing.get_context("spawn")
    out: Any = ctx.Queue()
    proc = ctx.Process(
        target=_child_entry,
        args=(code, data, timeout, max_memory_mb, out),
    )
    proc.start()

    try:
        status, payload = out.get(timeout=timeout + 2)
    except _queue.Empty:
        proc.kill()
        proc.join()
        raise TransformError(f"Transform exceeded {timeout}s timeout and was terminated") from None
    finally:
        proc.join(timeout=1)
        if proc.is_alive():
            proc.kill()
            proc.join()

    if proc.exitcode not in (0, None):
        # Killed by the kernel before reporting — almost always a limit breach.
        raise TransformError(
            f"Transform worker was killed (exit {proc.exitcode}) — likely exceeded "
            f"the {max_memory_mb}MB memory or {timeout}s CPU limit"
        )

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
