"""Static safety analysis for LLM-generated Python transform functions.

Runs *before* execution — unsafe code is rejected without ever being
``exec``-ed.
"""

from __future__ import annotations

import ast

# Imports / identifiers that are never allowed in generated code.
_BLOCKED_IMPORTS: frozenset[str] = frozenset(
    {
        "os",
        "sys",
        "subprocess",
        "socket",
        "urllib",
        "requests",
        "httpx",
        "shutil",
        "pathlib",
        "signal",
        "ctypes",
    }
)

_BLOCKED_NAMES: frozenset[str] = frozenset(
    {
        "__import__",
        "eval",
        "exec",
        "open",
        "__builtins__",
        "compile",
        "globals",
        "locals",
        "breakpoint",
    }
)

_MAX_LINES = 200


def validate_transform_function(code: str) -> tuple[bool, str | None]:
    """Validate a generated transform function for safety and correctness.

    Returns ``(True, None)`` when the code is acceptable, or
    ``(False, reason)`` when it must be rejected.
    """
    # -- length check --------------------------------------------------------
    lines = code.strip().splitlines()
    if len(lines) > _MAX_LINES:
        return False, f"code is {len(lines)} lines (limit is {_MAX_LINES})"

    # -- syntax check --------------------------------------------------------
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        return False, f"syntax error: {exc}"

    # -- find `transform` function -------------------------------------------
    transform_func: ast.FunctionDef | ast.AsyncFunctionDef | None = None
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "transform":
            transform_func = node
            break

    if transform_func is None:
        return False, "`transform` function not defined"

    # -- check signature (exactly one parameter) -----------------------------
    args = transform_func.args
    param_count = len(args.args) + len(args.posonlyargs) + len(args.kwonlyargs)
    if args.vararg:
        param_count += 1
    if args.kwarg:
        param_count += 1
    if param_count != 1:
        return False, (f"`transform` must have exactly 1 parameter, got {param_count}")

    # -- walk the full AST for blocked patterns ------------------------------
    for node in ast.walk(tree):
        # import foo / import os
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".")[0]
                if top in _BLOCKED_IMPORTS:
                    return False, f"blocked import: `{alias.name}`"

        # from foo import bar
        if isinstance(node, ast.ImportFrom):
            module = (node.module or "").split(".")[0]
            if module in _BLOCKED_IMPORTS:
                return False, f"blocked import: `{node.module}`"

        # Calls to blocked names: eval(...), exec(...), open(...)
        if isinstance(node, ast.Call):
            func = node.func
            name: str | None = None
            if isinstance(func, ast.Name):
                name = func.id
            elif isinstance(func, ast.Attribute):
                name = func.attr

            if name and name in _BLOCKED_NAMES:
                return False, f"blocked call: `{name}`"

        # Name references to blocked identifiers (e.g. __builtins__)
        if isinstance(node, ast.Name) and node.id in _BLOCKED_NAMES:
            return False, f"blocked identifier: `{node.id}`"

    return True, None
