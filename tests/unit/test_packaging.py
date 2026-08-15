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
_DOCKERFILE = _PROJECT_ROOT / "docker" / "Dockerfile"
_COMPOSE = _PROJECT_ROOT / "docker" / "docker-compose.yml"
_SMOKE_TEST = _PROJECT_ROOT / "scripts" / "smoke_test.sh"


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


def test_control_plane_daemon_script_is_declared() -> None:
    data = tomllib.loads(_PYPROJECT.read_text())

    assert data["project"]["scripts"]["loaferd"] == "loafer.control_plane.daemon:main"


def test_production_image_uses_locked_non_root_runtime() -> None:
    dockerfile = _DOCKERFILE.read_text()

    assert "uv sync --locked --no-dev --no-editable --no-cache" in dockerfile
    assert "uv pip install" not in dockerfile
    assert "pip uninstall --yes setuptools wheel pip" in dockerfile
    assert "USER 10001:10001" in dockerfile
    assert dockerfile.count("python:3.11.15-slim-trixie@sha256:") == 2
    assert "ghcr.io/astral-sh/uv:0.11.16@sha256:" in dockerfile


def test_release_smoke_covers_the_control_plane_entry_point() -> None:
    smoke_test = _SMOKE_TEST.read_text()

    assert "loaferd --help" in smoke_test
    assert '"http://127.0.0.1:19443/healthz"' in smoke_test
    assert '"$LOAFER" metadata migrate' in smoke_test


def test_compose_starts_services_only_after_explicit_migration() -> None:
    compose = _COMPOSE.read_text()

    assert 'command: ["metadata", "migrate"]' in compose
    assert 'profiles: ["platform", "daemon", "web"]' in compose
    assert 'profiles: ["platform", "scheduler"]' in compose
    assert 'profiles: ["platform", "worker"]' in compose
    assert 'profiles: ["platform", "web"]' in compose
    assert compose.count("condition: service_completed_successfully") == 11
    assert 'command: ["relay"]' in compose
    assert 'command: ["worker", "--role", "document"]' in compose
    assert 'command: ["worker", "--role", "browser"]' in compose
    assert "LOAFER_NATS_URL: nats://nats:4222" in compose
    assert "nocopy: true" in compose
    assert "storage-init:" in compose
    assert "      - CHOWN" in compose
    assert '"127.0.0.1:${LOAFERD_PORT:-9443}:9443"' in compose
    assert "read_only: true" in compose
    assert "no-new-privileges:true" in compose
    assert "/root/.loafer" not in compose


def test_web_service_is_hardened_and_loopback_bound() -> None:
    compose = _COMPOSE.read_text()
    web = compose.split("  web:", maxsplit=1)[1].split("\n  scheduler:", maxsplit=1)[0]

    # The browser boundary holds session secrets, so it inherits the same
    # runtime restrictions as the Python services despite using its own image.
    assert 'user: "10001:10001"' in web
    assert "read_only: true" in web
    assert "no-new-privileges:true" in web
    assert "cap_drop:" in web
    assert "init: true" in web

    # Published to loopback only; an operator-supplied proxy terminates TLS.
    assert '"127.0.0.1:${LOAFER_WEB_PORT:-3000}:3000"' in web

    # The private hop to loaferd is opt-in and explicit, never inferred.
    assert 'LOAFERD_TRUST_INTERNAL_NETWORK: "1"' in web
    assert "LOAFERD_URL: ${LOAFERD_INTERNAL_URL:-http://loaferd:9443}" in web


def test_web_image_builds_from_the_lockfile_as_non_root() -> None:
    dockerfile = (_PROJECT_ROOT / "docker" / "Dockerfile.web").read_text()

    assert "npm ci" in dockerfile
    assert "npm install" not in dockerfile
    assert "npm uninstall --global npm" in dockerfile
    assert "USER 10001:10001" in dockerfile
    # Both stages pin the same digest, matching the Python image's convention.
    assert dockerfile.count("node:22.16.0-bookworm-slim@sha256:") == 2


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
