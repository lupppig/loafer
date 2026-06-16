"""Restricted builtins and globals for executing transform code.

Shared by the custom and AI transform runners and the sandbox child process.
The allowlist is deliberately tiny — no ``__import__``, ``open``, ``eval``,
``exec``, or filesystem/network access. ``__import__`` is intentionally absent:
the modules a transform may use are injected directly below, so handing the
sandbox a live ``__import__`` would only add an escape primitive.
"""

from __future__ import annotations

from types import ModuleType
from typing import Any

# Builtins a transform is allowed to reference.
SAFE_BUILTINS: dict[str, Any] = {
    "len": len,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "list": list,
    "dict": dict,
    "set": set,
    "tuple": tuple,
    "range": range,
    "enumerate": enumerate,
    "zip": zip,
    "map": map,
    "filter": filter,
    "sorted": sorted,
    "reversed": reversed,
    "sum": sum,
    "min": min,
    "max": max,
    "abs": abs,
    "round": round,
    "isinstance": isinstance,
    "type": type,
    "None": None,
    "True": True,
    "False": False,
    "print": print,
}

# Modules injected into the transform namespace by name.
_ALLOWED_MODULES = ("re", "json", "datetime", "math", "decimal", "uuid", "itertools")


def build_safe_globals() -> dict[str, Any]:
    """Build the restricted globals dict used to exec transform code."""
    safe_globals: dict[str, Any] = {"__builtins__": SAFE_BUILTINS}
    for mod_name in _ALLOWED_MODULES:
        try:
            mod: ModuleType = __import__(mod_name)
            safe_globals[mod_name] = mod
        except ImportError:
            pass
    return safe_globals
