"""Loafer CLI — Typer entrypoint.

All user-facing output uses rich.console.Console. Never use print().
"""

from __future__ import annotations

import signal
import sys
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

from loafer.exceptions import PipelineError, SchedulerError
from loafer.runner import list_connectors, run_pipeline, validate_config

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


@app.command()
def schedule(
    config_file: Path = _config_arg,
    cron: str | None = typer.Option(None, "--cron", help="Cron expression (e.g. '0 9 * * *')"),
    interval: str | None = typer.Option(
        None, "--interval", help="Interval (e.g. '1h', '30m', '1d')"
    ),
    job_id: str | None = typer.Option(None, "--id", help="Job ID (auto-generated if omitted)"),
    replace: bool = typer.Option(False, "--replace", help="Replace existing job with same ID"),
) -> None:
    """Schedule a pipeline to run on a cron or interval trigger."""
    if not cron and not interval:
        err_console.print("[red]Either --cron or --interval is required[/red]")
        raise typer.Exit(1)

    if not config_file.exists():
        err_console.print(f"[red]Config file not found: {config_file}[/red]")
        raise typer.Exit(1)

    from loafer.config import load_config
    from loafer.scheduler import PipelineScheduler

    try:
        config = load_config(config_file)
        pipeline_name = config.name or config_file.stem
    except Exception:
        pipeline_name = config_file.stem

    scheduler = PipelineScheduler()
    try:
        schedule_id = scheduler.add_schedule(
            config_path=str(config_file),
            schedule_id=job_id,
            cron=cron,
            interval=interval,
            replace=replace,
            name=pipeline_name,
        )
    except SchedulerError as exc:
        err_console.print(f"[red]Schedule failed: {exc}[/red]")
        raise typer.Exit(1) from exc

    trigger_desc = f"cron: {cron}" if cron else f"interval: {interval}"
    console.print(f"[green]Scheduled pipeline[/green] {schedule_id}")
    console.print(f"  Name:    {pipeline_name}")
    console.print(f"  Config:  {config_file}")
    console.print(f"  Trigger: {trigger_desc}")
    console.print(f"\nRun [bold]loafer start[/bold] to begin executing scheduled jobs")


@app.command()
def unschedule(
    job_id: str = typer.Argument(..., help="Job ID to remove"),
) -> None:
    """Remove a scheduled pipeline job."""
    from loafer.scheduler import PipelineScheduler

    scheduler = PipelineScheduler()
    try:
        scheduler.remove_schedule(job_id)
    except SchedulerError as exc:
        err_console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1) from exc

    console.print(f"[green]Removed job[/green] {job_id}")


@app.command()
def list_schedules() -> None:
    """List all scheduled pipeline jobs."""
    from loafer.scheduler import PipelineScheduler

    scheduler = PipelineScheduler()
    jobs = scheduler.list_schedules()

    if not jobs:
        console.print("[dim]No scheduled jobs[/dim]")
        return

    table = Table(title="Scheduled Jobs")
    table.add_column("ID", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Config", style="green")
    table.add_column("Trigger", style="yellow")
    table.add_column("Next Run", style="magenta")
    table.add_column("Paused", style="red")

    for job in jobs:
        table.add_row(
            job["id"],
            job["name"] or "—",
            job["config_path"],
            job["trigger"],
            job["next_run"] or "—",
            "yes" if job["paused"] else "no",
        )

    console.print(table)


@app.command()
def start() -> None:
    """Start the scheduler and run scheduled jobs in the foreground."""
    from loafer.scheduler import PipelineScheduler

    scheduler = PipelineScheduler()
    scheduler.start()

    console.print("[green]Scheduler started[/green]")
    console.print("Press Ctrl+C to stop\n")

    jobs = scheduler.list_schedules()
    if jobs:
        console.print(f"Active schedules: {len(jobs)}")
        for job in jobs:
            name = f" ({job['name']})" if job["name"] else ""
            console.print(f"  {job['id']}{name}: {job['config_path']} ({job['trigger']})")
    else:
        console.print("[yellow]No scheduled jobs. Use 'loafer schedule' to add one.[/yellow]")

    def _shutdown(signum: int, frame: Any) -> None:
        console.print("\n[yellow]Stopping scheduler...[/yellow]")
        scheduler.stop()
        console.print("[green]Scheduler stopped[/green]")
        raise typer.Exit(0)

    import signal

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        import time

        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.stop()
        console.print("[green]Scheduler stopped[/green]")
