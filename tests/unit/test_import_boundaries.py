"""Architecture tests for the Phase 1 engine/client boundary."""

from __future__ import annotations

import ast
from pathlib import Path

_REPOSITORY = Path(__file__).resolve().parents[2]
_LOAFER = _REPOSITORY / "loafer"
_ENGINE_PATHS = (
    _LOAFER / "engine.py",
    _LOAFER / "config.py",
    _LOAFER / "contracts.py",
    _LOAFER / "core",
    _LOAFER / "graph",
    _LOAFER / "agents",
    _LOAFER / "transform",
    _LOAFER / "ports",
)
_BANNED_EXTERNAL = {"typer", "rich", "fastapi", "starlette", "flask", "django"}
_BANNED_INTERNAL = {
    "loafer.cli",
    "loafer.scheduler",
    "loafer.daemon",
    "loafer.application.service",
    "loafer.application.local",
}


def _python_files(path: Path) -> list[Path]:
    return [path] if path.is_file() else sorted(path.rglob("*.py"))


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_engine_does_not_import_client_or_web_frameworks() -> None:
    violations: list[str] = []
    for root in _ENGINE_PATHS:
        for path in _python_files(root):
            for module in _imports(path):
                top_level = module.split(".", 1)[0]
                if top_level in _BANNED_EXTERNAL or any(
                    module == banned or module.startswith(f"{banned}.")
                    for banned in _BANNED_INTERNAL
                ):
                    violations.append(f"{path.relative_to(_REPOSITORY)} imports {module}")

    assert violations == []


def test_application_service_has_no_cli_rendering_dependency() -> None:
    modules = set()
    for path in (_LOAFER / "application").rglob("*.py"):
        modules.update(_imports(path))

    assert not ({module.split(".", 1)[0] for module in modules} & _BANNED_EXTERNAL)

    for client in (_LOAFER / "cli.py", _LOAFER / "scheduler.py"):
        client_imports = _imports(client)
        assert "loafer.application" in client_imports
        assert "loafer.runner" not in client_imports
