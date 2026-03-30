"""Loafer CLI — Typer entrypoint.

All user-facing output uses rich.console.Console. Never use print().
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import typer
from rich.console import Console

if TYPE_CHECKING:
    from pathlib import Path

app = typer.Typer(
    name="loafer",
    help="AI-assisted ETL and ELT pipelines from the command line.",
    no_args_is_help=True,
)

console = Console()
err_console = Console(stderr=True)

_config_arg = typer.Argument(..., help="Path to pipeline YAML config")


@app.command()
def run(
    config_file: Path = _config_arg,
    dry_run: bool = typer.Option(False, "--dry-run", help="Run without loading to target"),
    verbose: bool = typer.Option(False, "--verbose", help="Print agent details"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip destructive confirmation prompts"),
) -> None:
    """Run an ETL or ELT pipeline."""
    raise NotImplementedError("pipeline runner not yet implemented")


@app.command()
def validate(
    config_file: Path = _config_arg,
) -> None:
    """Validate a pipeline config without running it."""
    raise NotImplementedError("config validation command not yet implemented")


@app.command()
def connectors() -> None:
    """List available source and target connectors."""
    raise NotImplementedError("connectors listing not yet implemented")
