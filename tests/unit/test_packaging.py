"""Packaging metadata guards.

Regression tests for BUG-1: the CLI imports ``click`` directly
(``loafer/cli.py``) but ``click`` was not declared in
``[project].dependencies``. It only worked because an older ``typer``
pulled it in transitively; on a clean install the import crashed every
command. These tests assert that every third-party module imported by the
package is backed by a declared dependency.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PYPROJECT = _PROJECT_ROOT / "pyproject.toml"


def _declared_dependencies() -> list[str]:
    data = tomllib.loads(_PYPROJECT.read_text())
    return data["project"]["dependencies"]


def _dep_names() -> set[str]:
    """Return the bare distribution names (no version specifiers)."""
    names: set[str] = set()
    for spec in _declared_dependencies():
        # Strip version/extras markers: "click>=8.0" -> "click".
        name = spec.split(";")[0].strip()
        for sep in ("<", ">", "=", "!", "~", "["):
            name = name.split(sep)[0]
        names.add(name.strip().lower().replace("_", "-"))
    return names


def test_click_is_declared() -> None:
    """``click`` must be declared because ``cli.py`` imports it directly."""
    assert "click" in _dep_names(), (
        "loafer/cli.py imports click directly; it must be declared in "
        "pyproject.toml [project].dependencies, not pulled in transitively."
    )


def test_cli_module_imports_declared_packages() -> None:
    """Every direct ``import``/``from`` in cli.py resolves to a declared dep.

    Guards against re-introducing an undeclared transitive dependency in the
    CLI entrypoint (the module loaded first on every invocation).
    """
    import ast

    # Distribution name -> top-level import name where they differ.
    import_to_dist = {
        "yaml": "pyyaml",
        "dotenv": "python-dotenv",
        "google": "google-genai",
    }
    declared = _dep_names()

    source = (_PROJECT_ROOT / "loafer" / "cli.py").read_text()
    tree = ast.parse(source)

    top_level_imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                top_level_imports.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            top_level_imports.add(node.module.split(".")[0])

    import sys

    stdlib = set(sys.stdlib_module_names)
    for mod in top_level_imports:
        if mod in stdlib or mod == "loafer":
            continue
        dist = import_to_dist.get(mod, mod).lower().replace("_", "-")
        assert dist in declared, (
            f"cli.py imports '{mod}' (distribution '{dist}') which is not "
            f"declared in pyproject.toml dependencies."
        )
