"""Loafer CLI — Typer entrypoint.

All user-facing output uses rich.console.Console. Never use print().
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import typer
from rich.console import Console
from rich.table import Table

from loafer.exceptions import PipelineError
from loafer.runner import list_connectors, run_pipeline, validate_config

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
    if not config_file.exists():
        err_console.print(f"[red]Config file not found: {config_file}[/red]")
        raise typer.Exit(1)

    try:
        state = run_pipeline(
            config_path=config_file,
            dry_run=dry_run,
            verbose=verbose,
            yes=yes,
        )
    except PipelineError as exc:
        err_console.print(f"[red]Pipeline failed: {exc}[/red]")
        raise typer.Exit(1) from exc

    if dry_run:
        console.print("\n[dry-run] Pipeline completed without loading to target")
        console.print(f"  Rows transformed: {len(state.get('transformed_data', []))}")
    else:
        console.print("\n[green]Pipeline completed successfully[/green]")
        console.print(f"  Rows extracted: {state.get('rows_extracted', 0)}")
        console.print(f"  Rows loaded:    {state.get('rows_loaded', 0)}")

    if state.get("warnings"):
        console.print(f"\n[yellow]Warnings ({len(state['warnings'])}):[/yellow]")
        for w in state["warnings"]:
            console.print(f"  - {w}")

    duration = state.get("duration_ms", {}).get("total", 0)
    console.print(f"\nTotal time: {duration / 1000:.1f}s")


@app.command()
def validate(
    config_file: Path = _config_arg,
) -> None:
    """Validate a pipeline config without running it."""
    if not config_file.exists():
        err_console.print(f"[red]Config file not found: {config_file}[/red]")
        raise typer.Exit(1)

    try:
        config = validate_config(config_file)
    except PipelineError as exc:
        err_console.print(f"[red]Config validation failed: {exc}[/red]")
        raise typer.Exit(1) from exc

    console.print("[green]Config is valid[/green]\n")

    table = Table(title="Pipeline Configuration")
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Mode", config.mode)
    table.add_row("Source", config.source.type)
    table.add_row("Target", config.target.type)
    table.add_row("Transform", config.transform.type)
    table.add_row("Chunk size", str(config.chunk_size))
    table.add_row("Streaming threshold", str(config.streaming_threshold))
    table.add_row("Validation strict", str(config.validation.strict))
    table.add_row("LLM provider", config.llm.provider)
    table.add_row("LLM model", config.llm.model)

    console.print(table)


@app.command()
def connectors() -> None:
    """List available source and target connectors."""
    result = list_connectors()

    console.print("[bold]Available Connectors[/bold]\n")

    source_table = Table(title="Sources")
    source_table.add_column("Type", style="cyan")
    for source_type in result["sources"]:
        source_table.add_row(source_type)
    console.print(source_table)

    target_table = Table(title="Targets")
    target_table.add_column("Type", style="cyan")
    for target_type in result["targets"]:
        target_table.add_row(target_type)
    console.print(target_table)
