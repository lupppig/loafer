"""Loafer CLI — Typer entrypoint.

Commands: run, validate, connectors, schedule, unschedule, list-schedules,
start, status, stop, logs, init.

All user-facing output uses rich.console.Console. Never use print().
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import click
import httpx
import keyring
import typer
import yaml
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.rule import Rule
from rich.spinner import Spinner
from rich.syntax import Syntax
from rich.table import Table

from loafer.application import RunRequest, get_local_application
from loafer.exceptions import LLMError, PipelineError, SchedulerError
from loafer.llm.models import DEFAULT_GEMINI_MODEL, default_model_for, provider_for_model

app = typer.Typer(
    name="loafer",
    help="AI-assisted ETL and ELT pipelines from the command line.",
    no_args_is_help=True,
)
metadata_app = typer.Typer(help="Manage the authoritative metadata schema.", no_args_is_help=True)
app.add_typer(metadata_app, name="metadata")

console = Console()
err_console = Console(stderr=True)

_config_arg = typer.Argument(..., help="Path to pipeline YAML config")


class RichReviewPort:
    """Render generated code and collect approval in the CLI client."""

    def approve_transform(self, generated_code: str) -> bool:
        console.print()
        console.print(
            Panel(
                "[yellow]AI-generated transform code is ready for review.[/yellow]\n"
                "Review the code below. Type 'y' to execute or 'n' to skip it.",
                title="[bold yellow]⚠ Human Review Required[/bold yellow]",
            )
        )
        console.print(Syntax(generated_code, "python", theme="monokai", line_numbers=True))
        try:
            answer = input("Execute this code? [y/N]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[dim]No input received. Skipping AI transform.[/dim]")
            return False
        return answer in {"y", "yes"}


def _resolve_version() -> str:
    """Return the installed package version, or a dev fallback."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("loafer-etl")
    except PackageNotFoundError:  # pragma: no cover - source checkout without install
        return "unknown"


def _version_callback(value: bool) -> None:
    if value:
        console.print(f"loafer {_resolve_version()}")
        raise typer.Exit(0)


@app.callback()
def _main(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        help="Show the loafer version and exit.",
        callback=_version_callback,
        is_eager=True,
    ),
) -> None:
    """AI-assisted ETL and ELT pipelines from the command line."""


@metadata_app.command("migrate")
def metadata_migrate_command(
    metadata_url: str | None = typer.Option(
        None,
        "--metadata-url",
        envvar="LOAFER_METADATA_URL",
        help="SQLite or PostgreSQL metadata URL; defaults to the embedded profile.",
    ),
) -> None:
    """Apply checked-in metadata migrations before starting Loafer services."""
    from loafer.application.durable import get_metadata_store

    store = get_metadata_store(metadata_url)
    try:
        before = store.current_schema_version()
        after = store.migrate()
    except Exception as exc:
        err_console.print(f"[red]Metadata migration failed:[/red] {exc}")
        raise typer.Exit(1) from exc
    finally:
        store.close()

    if before == after:
        console.print(f"[green]✓ Metadata schema already at version {after}[/green]")
    else:
        console.print(f"[green]✓ Migrated metadata schema from version {before} to {after}[/green]")


@app.command("enqueue")
def enqueue_command(
    config: Path = _config_arg,
    command_key: str | None = typer.Option(
        None,
        "--command-key",
        help="Idempotency key; repeated use returns the same durable run.",
    ),
    local: bool = typer.Option(
        False,
        "--local",
        help="Use the embedded metadata profile explicitly instead of loaferd.",
    ),
    api_url: str | None = typer.Option(None, "--api-url", envvar="LOAFER_API_URL"),
    auth_url: str | None = typer.Option(None, "--auth-url", envvar="LOAFER_AUTH_URL"),
    workspace_id: str | None = typer.Option(None, "--workspace", envvar="LOAFER_WORKSPACE_ID"),
) -> None:
    """Create a durable run through loaferd, or explicitly use local mode."""
    from loafer.application.durable import enqueue_pipeline

    key = command_key or f"manual:{uuid.uuid4().hex}"
    if not local:
        direct_token = os.environ.get("LOAFER_ACCESS_TOKEN")
        if not api_url or not workspace_id or (not auth_url and not direct_token):
            err_console.print(
                "[red]Remote mode requires LOAFER_API_URL, LOAFER_WORKSPACE_ID, and either "
                "LOAFER_AUTH_URL or LOAFER_ACCESS_TOKEN.[/red]\n"
                "Use [bold]--local[/bold] only for explicit embedded compatibility mode."
            )
            raise typer.Exit(2)
        try:
            document = yaml.safe_load(config.read_text(encoding="utf-8"))
            if not isinstance(document, dict):
                raise ValueError("pipeline config must be a YAML object")
            from loafer.control_plane.client import HTTPSControlPlaneClient

            token = _exchange_control_token(auth_url)
            with HTTPSControlPlaneClient(api_url, token) as client:
                version = client.register_pipeline(
                    workspace_id,
                    str(document.get("name") or config.stem),
                    document,
                )
                run = client.create_run(
                    workspace_id,
                    version["id"],
                    idempotency_key=key,
                )
        except Exception as exc:
            err_console.print(f"[red]Could not enqueue pipeline through loaferd:[/red] {exc}")
            raise typer.Exit(1) from exc
        console.print(f"[green]✓ Enqueued run through loaferd[/green] {run['id']}")
        return
    try:
        run = enqueue_pipeline(config, command_key=key)
    except Exception as exc:
        err_console.print(f"[red]Could not enqueue pipeline:[/red] {exc}")
        raise typer.Exit(1) from exc
    console.print(f"[green]✓ Enqueued run[/green] {run.id}")


@app.command("login")
def login_command(
    auth_url: str = typer.Option(..., "--auth-url", envvar="LOAFER_AUTH_URL"),
) -> None:
    """Sign the CLI in with Better Auth's device-authorization flow."""
    _require_https_url(auth_url, "authentication URL")
    try:
        with httpx.Client(base_url=auth_url.rstrip("/"), timeout=30) as client:
            response = client.post(
                "/api/auth/device/code",
                json={"client_id": "loafer-cli", "scope": "openid profile email"},
            )
            response.raise_for_status()
            device = response.json()
            console.print(
                f"Open [link={device['verification_uri_complete']}]"
                f"{device['verification_uri_complete']}[/link]\n"
                f"Enter code: [bold]{device['user_code']}[/bold]"
            )
            deadline = time.monotonic() + int(device["expires_in"])
            interval = max(1, int(device["interval"]))
            while time.monotonic() < deadline:
                time.sleep(interval)
                token_response = client.post(
                    "/api/auth/device/token",
                    json={
                        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                        "device_code": device["device_code"],
                        "client_id": "loafer-cli",
                    },
                )
                body = token_response.json()
                if token_response.is_success:
                    keyring.set_password("loafer", auth_url, body["access_token"])
                    console.print("[green]✓ CLI authenticated[/green]")
                    return
                if body.get("error") == "slow_down":
                    interval += 5
                elif body.get("error") not in {"authorization_pending", "slow_down"}:
                    raise RuntimeError(body.get("error_description", "device login failed"))
    except Exception as exc:
        err_console.print(f"[red]CLI login failed:[/red] {exc}")
        raise typer.Exit(1) from exc
    err_console.print("[red]Device authorization expired before approval.[/red]")
    raise typer.Exit(1)


@app.command("logout")
def logout_command(
    auth_url: str = typer.Option(..., "--auth-url", envvar="LOAFER_AUTH_URL"),
) -> None:
    """Remove the CLI session credential from the operating-system keyring."""
    try:
        keyring.delete_password("loafer", auth_url)
    except keyring.errors.PasswordDeleteError:
        pass
    console.print("[green]✓ CLI credential removed[/green]")


def _exchange_control_token(auth_url: str | None) -> str:
    """Exchange the keyring-held device session for a short-lived loaferd JWT."""
    direct = os.environ.get("LOAFER_ACCESS_TOKEN")
    if direct:
        return direct
    if auth_url is None:
        raise RuntimeError("LOAFER_AUTH_URL is required for CLI session exchange")
    _require_https_url(auth_url, "authentication URL")
    session_token = keyring.get_password("loafer", auth_url)
    if not session_token:
        raise RuntimeError("CLI is not authenticated; run `loafer login`")
    response = httpx.get(
        f"{auth_url.rstrip('/')}/api/auth/token",
        headers={"Authorization": f"Bearer {session_token}"},
        timeout=30,
    )
    response.raise_for_status()
    token = response.json().get("token")
    if not isinstance(token, str) or not token:
        raise RuntimeError("authentication server did not return a control-plane token")
    return token


def _require_https_url(value: str, label: str) -> None:
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError(f"{label} must be an absolute HTTPS URL")


def _run_with_graceful_shutdown(service: Any) -> None:
    """Translate process termination into stop-claiming-and-drain semantics."""
    previous = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}

    def request_shutdown(_signum: int, _frame: Any) -> None:
        service.request_shutdown()

    try:
        for signum in previous:
            signal.signal(signum, request_shutdown)
        service.run_forever()
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


@app.command("worker")
def worker_command(
    once: bool = typer.Option(False, "--once", help="Process at most one runnable job."),
    worker_id: str | None = typer.Option(None, "--id", help="Stable worker identity."),
    role: str = typer.Option("etl", "--role", help="Worker pool: etl, document, or browser."),
) -> None:
    """Run the durable data-plane worker separately from the scheduler."""
    from loafer.application.durable import get_durable_worker
    from loafer.core.roles import WorkerRole

    try:
        worker_role = WorkerRole(role)
    except ValueError as exc:
        raise typer.BadParameter("must be etl, document, or browser", param_hint="--role") from exc
    if worker_role is WorkerRole.SCHEDULER:
        raise typer.BadParameter(
            "scheduler uses `loafer start`, not a worker pool", param_hint="--role"
        )
    identity = worker_id or f"{worker_role.value}-{socket.gethostname()}-{os.getpid()}"
    worker = get_durable_worker(worker_id=identity, role=worker_role)
    try:
        if once:
            run_id = worker.run_once()
            if run_id is None:
                console.print("[dim]No runnable jobs.[/dim]")
            else:
                console.print(f"[green]✓ Processed run[/green] {run_id}")
            return
        console.print(f"[green]Worker started[/green] {identity}")
        _run_with_graceful_shutdown(worker)
    finally:
        worker.close()


@app.command("relay")
def relay_command(
    once: bool = typer.Option(False, "--once", help="Publish at most one outbox page."),
) -> None:
    """Relay durable run commands from PostgreSQL to NATS JetStream."""
    from loafer.application.durable import get_outbox_relay

    relay = get_outbox_relay()
    try:
        if once:
            published = relay.run_once()
            console.print(f"[green]✓ Published[/green] {published} job(s)")
            return
        console.print("[green]Outbox relay started[/green]")
        _run_with_graceful_shutdown(relay)
    finally:
        relay.close()


# Animated stage loaders

_STAGE_SPINNERS = {
    "extract": ("dots", "cyan"),
    "validate": ("dots2", "blue"),
    "transform": ("dots3", "magenta"),
    "load": ("dots4", "green"),
    "load_raw": ("dots5", "green"),
    "transform_in_target": ("dots6", "magenta"),
}

_STAGE_MESSAGES = {
    "extract": [
        "Connecting to source…",
        "Reading data…",
        "Parsing rows…",
        "Extracting records…",
    ],
    "validate": [
        "Checking schema…",
        "Validating null rates…",
        "Verifying data types…",
        "Running quality checks…",
    ],
    "transform": [
        "Preparing transformation…",
        "Generating code…",
        "Processing data…",
        "Applying transforms…",
    ],
    "load": [
        "Opening target…",
        "Writing rows…",
        "Flushing to disk…",
        "Finalizing load…",
    ],
    "load_raw": [
        "Creating raw table…",
        "Loading raw data…",
        "Writing to target…",
        "Finalizing load…",
    ],
    "transform_in_target": [
        "Analyzing schema…",
        "Generating SQL…",
        "Executing query…",
        "Transforming in target…",
    ],
}


class StageAnimator:
    """Manages animated spinner feedback for a single pipeline stage."""

    def __init__(self, stage: str, label: str) -> None:
        self.stage = stage
        self.label = label
        spinner_name, spinner_color = _STAGE_SPINNERS.get(stage, ("dots", "white"))
        self.spinner = Spinner(
            spinner_name, text=f"[{spinner_color}]{label}[/]", style=spinner_color
        )
        self.messages = _STAGE_MESSAGES.get(stage, ["Working…"])
        self._msg_idx = 0
        self._start = time.monotonic()
        self._live: Live | None = None
        self._done = False

    def start(self) -> None:
        self._live = Live(self.spinner, refresh_per_second=12, console=console, transient=True)
        self._live.start()
        self._start = time.monotonic()

    def pulse(self) -> None:
        if self._done or self._live is None:
            return
        elapsed = time.monotonic() - self._start
        msg = self.messages[self._msg_idx % len(self.messages)]
        self._msg_idx += 1
        _, color = _STAGE_SPINNERS.get(self.stage, ("dots", "white"))
        self.spinner.update(
            text=f"[{color}]{msg}[/] [{color}][{elapsed:.1f}s][/]",
        )

    def finish(self, status: str, row_info: str = "") -> None:
        if self._live is None:
            return
        self._done = True
        elapsed = time.monotonic() - self._start
        self._live.stop()
        self._live = None

        if status == "done":
            icon = "[green]✓[/green]"
            console.print(f"  {icon}  [green]{self.label}[/green]  [{elapsed:.1f}s]  {row_info}")
        elif status == "failed":
            icon = "[red]✗[/red]"
            console.print(f"  {icon}  [red]{self.label}[/red]  [{elapsed:.1f}s]")
        elif status == "skipped":
            icon = "[dim]⊘[/dim]"
            console.print(f"  {icon}  [dim]{self.label}[/dim]")


# Human-readable error formatting


def _format_user_error(error: Exception, stage: str | None = None) -> str:
    """Convert raw exceptions into human-readable messages."""
    msg = str(error)

    # Pydantic validation errors — strip the noise
    if "validation error" in msg.lower() and "pydantic" in msg.lower():
        return _parse_pydantic_error(msg)

    # LLM API errors
    if "404" in msg or "not_found" in msg.lower() or "not found" in msg.lower():
        if "model" in msg.lower() or "gemini" in msg.lower() or "claude" in msg.lower():
            return _parse_model_not_found(msg)
        if "api" in msg.lower() or "endpoint" in msg.lower():
            return (
                "The API endpoint could not be reached.\n"
                "  • Check your internet connection\n"
                "  • Verify the API key is valid and has not expired\n"
                "  • Make sure the model name is correct for your provider"
            )

    # Rate limit / quota errors
    if "429" in msg or "rate" in msg.lower() or "quota" in msg.lower():
        return (
            "Gemini free tier allows 15 requests per minute — you've hit the limit.\n"
            "  • The pipeline will automatically retry with backoff (up to ~8 minutes)\n"
            "  • If it keeps failing, wait 1 minute and try again\n"
            "  • Check your usage at https://aistudio.google.com/\n"
            "  • Consider upgrading to Gemini paid for higher limits"
        )

    # Authentication errors
    if "401" in msg or "unauthorized" in msg.lower() or "authentication" in msg.lower():
        return (
            "Authentication failed — your API key is invalid or expired.\n"
            "  • Check that your API key is correct\n"
            "  • Make sure it hasn't expired or been revoked\n"
            '  • Set it with: export GEMINI_API_KEY="your-key" (or your provider\'s var)'
        )

    # Permission errors
    if "403" in msg or "permission" in msg.lower() or "forbidden" in msg.lower():
        return (
            "Access denied — your API key doesn't have permission for this operation.\n"
            "  • Check your API key has the required scopes\n"
            "  • Verify your account is in good standing"
        )

    # LLM transform failures (after retries)
    if "transform failed" in msg.lower() and "attempt" in msg.lower():
        # Extract the inner error
        lower_msg = msg.lower()
        if "last error:" in lower_msg:
            idx = lower_msg.find("last error:")
            inner = msg[idx + len("last error:") :].strip()
            return f"AI transformation failed after 3 retries.\n\n  {inner}"
        return (
            "AI transformation failed after 3 retries.\n"
            "  • Check your transform instruction is clear and specific\n"
            "  • Make sure your API key has sufficient quota\n"
            "  • Try with a smaller dataset to isolate the issue"
        )

    # File not found
    if "not found" in msg.lower() or "no such file" in msg.lower():
        return (
            f"File not found.\n"
            f"  • Check the file path in your config is correct\n"
            f"  • Paths are resolved relative to the config file's directory\n"
            f"  • Details: {msg}"
        )

    # Connection errors
    if "connection" in msg.lower() or "refused" in msg.lower():
        return (
            "Could not connect to the database or service.\n"
            "  • Make sure the service is running\n"
            "  • Check the connection URL is correct\n"
            "  • Verify your network / firewall settings"
        )

    # Transform sandbox limits (timeout / memory kill)
    lower = msg.lower()
    if "transform" in lower and ("timeout" in lower or "memory" in lower or "killed" in lower):
        return (
            "The transform exceeded its sandbox limits and was terminated.\n"
            "  • The code may have an infinite loop or use too much memory\n"
            "  • Raise the limits in your config:\n"
            "      sandbox:\n"
            "        timeout: 120\n"
            "        max_memory_mb: 1024"
        )

    # Timeout
    if "timeout" in msg.lower() or "timed out" in msg.lower():
        return (
            "The operation timed out.\n"
            "  • The source might be slow or the dataset is very large\n"
            "  • Try increasing the timeout in your config\n"
            "  • Check your network connection"
        )

    # Dependency missing
    if "module" in msg.lower() and "not found" in msg.lower():
        module = msg.split("'")[1] if "'" in msg else "unknown"
        return (
            f"Missing dependency: {module}\n"
            f"  • Install it with: uv add {module}\n"
            f"  • Or: pip install {module}"
        )

    # Generic — include stage context
    prefix = f"[{stage}] " if stage else ""
    return f"{prefix}{msg}"


def _parse_pydantic_error(raw: str) -> str:
    """Extract the actual validation issues from a Pydantic error dump."""
    lines = []
    in_errors = False
    current_field = None
    for line in raw.split("\n"):
        stripped = line.strip()
        if "validation error" in stripped.lower():
            in_errors = True
            lines.append("[bold red]Configuration Error[/bold red]")
            lines.append("")
            continue
        if in_errors:
            if stripped.startswith("For further"):
                continue
            # Field path lines like: source.excel.path
            if stripped and not stripped.startswith("type=") and "." in stripped[:30]:
                current_field = stripped
                continue
            if stripped.startswith("Value error,"):
                detail = stripped.replace("Value error,", "").strip()
                if current_field:
                    lines.append(f"  [yellow]•[/yellow] {current_field}: {detail}")
                else:
                    lines.append(f"  [yellow]•[/yellow] {detail}")
            elif stripped.startswith("Input should be") or stripped.startswith("Field required"):
                if current_field:
                    lines.append(f"  [yellow]•[/yellow] {current_field}: {stripped}")
                else:
                    lines.append(f"  [yellow]•[/yellow] {stripped}")
            elif stripped.startswith("type="):
                pass
            elif stripped and current_field:
                lines.append(f"  [yellow]•[/yellow] {current_field}: {stripped}")
    if not lines:
        return raw
    lines.append("")
    lines.append("[dim]Tip: Paths in your config are relative to the config file's location.[/dim]")
    return "\n".join(lines)


def _parse_model_not_found(raw: str) -> str:
    """Extract model name from a 404 model-not-found error."""
    model = "unknown"
    for part in raw.split():
        if part.startswith(("gemini", "claude", "gpt", "qwen")):
            model = part.strip("',.")
            break
    provider = provider_for_model(model)
    if provider:
        recommended = default_model_for(provider)
        return (
            f"Model [bold]{model}[/bold] was not found.\n"
            f"\n"
            f"  It may have been renamed, retired, or may not be enabled for your account.\n"
            f"  Current Loafer default for {provider}: [bold]{recommended}[/bold]\n"
            f"\n"
            f"  Update your config:\n"
            f"    llm:\n"
            f"      provider: {provider}\n"
            f"      model: {recommended}"
        )
    return (
        f"Model [bold]{model}[/bold] was not found.\n"
        f"  • Check the model name is correct for your provider\n"
        f"  • Visit your provider's docs for available models"
    )


# Display helpers

_STAGE_LABELS: dict[str, str] = {
    "extract": "Extracting from source",
    "validate": "Validating data",
    "transform": "Transforming data",
    "load": "Loading to target",
    "load_raw": "Loading raw data to target",
    "transform_in_target": "Generating and executing SQL",
}

_STATUS_ICONS: dict[str, str] = {
    "done": "[green]✓[/green]",
    "failed": "[red]✗[/red]",
    "skipped": "[dim]⊘[/dim]",
    "running": "[yellow]⠋[/yellow]",
}


def _get_stage_label(node_name: str, state: Any) -> str:
    """Build a human-readable label for a pipeline stage."""
    base = _STAGE_LABELS.get(node_name, node_name.capitalize())

    if node_name == "extract":
        source_type = state.get("source_type")
        if source_type:
            base = f"Extracting from {str(source_type).upper()}"
    elif node_name == "transform":
        transform_type = state.get("transform_type")
        if transform_type:
            base = f"Transforming data ({transform_type})"
    elif node_name in ("load", "load_raw"):
        target_type = state.get("target_type")
        if target_type:
            base = f"Loading to {str(target_type).upper()}"

    return base


def _get_row_info(node_name: str, state: Any) -> str:
    """Extract row count info for a completed stage."""
    if node_name == "extract":
        n = state.get("rows_extracted", 0)
        return f"{n} row{'s' if n != 1 else ''}" if n else "—"
    if node_name == "validate":
        n = state.get("rows_extracted", 0)
        passed = state.get("validation_passed", False)
        return f"{n} passed" if passed else "failed"
    if node_name == "transform":
        src = state.get("rows_extracted", 0)
        dst = state.get("rows_transformed", 0)
        if src and dst and src != dst:
            return f"{src} → {dst}"
        return f"{dst} row{'s' if dst != 1 else ''}" if dst else "—"
    if node_name in ("load", "load_raw"):
        n = state.get("rows_loaded", 0)
        return f"{n} row{'s' if n != 1 else ''}" if n else "—"
    if node_name == "transform_in_target":
        n = state.get("rows_loaded", 0)
        return f"{n} row{'s' if n != 1 else ''}" if n else "—"
    return ""


def _get_duration(node_name: str, state: Any) -> str:
    """Get duration for a stage in ms."""
    dm = state.get("duration_ms", {})
    ms = dm.get(node_name, 0)
    if ms < 1000:
        return f"{ms:.0f}ms"
    return f"{ms / 1000:.1f}s"


def _print_progress_bar(node_name: str, label: str, status: str, row_info: str) -> None:
    """Print a single progress line for a stage."""
    icon = _STATUS_ICONS.get(status, " ")
    console.print(f"  {icon}  {label:<35} {row_info}")


def _add_step_breakdown_rows(table: Any, state: dict[str, Any]) -> None:
    """Add one indented row per step of a multi-step transform pipeline."""
    step_results = state.get("step_results") or []
    if not step_results:
        return

    for i, step in enumerate(step_results):
        step_name = step["name"] if isinstance(step, dict) else step.name
        step_type = step["type"] if isinstance(step, dict) else step.type
        step_success = step["success"] if isinstance(step, dict) else step.success
        rows_in = step["rows_in"] if isinstance(step, dict) else step.rows_in
        rows_out = step["rows_out"] if isinstance(step, dict) else step.rows_out
        duration_ms = step["duration_ms"] if isinstance(step, dict) else step.duration_ms
        token_usage = step.get("token_usage") if isinstance(step, dict) else step.token_usage
        connector = "└─" if i == len(step_results) - 1 else "├─"
        icon = _STATUS_ICONS.get("done" if step_success else "failed", " ")
        detail = f"({step_type})"
        if token_usage and token_usage.get("total_tokens"):
            detail = f"({step_type}, {token_usage['total_tokens']:,} tok)"
        table.add_row(
            f"  {connector} {step_name}",
            icon,
            f"{rows_in:,} → {rows_out:,}",
            f"{duration_ms / 1000:.1f}s  [dim]{detail}[/dim]",
        )


def _print_summary_table(state: dict[str, Any], mode: str, failed_stage: str | None = None) -> None:
    """Print the detailed pipeline summary table."""
    console.print()
    console.print(Rule("[bold]Pipeline Summary[/bold]", style="dim"))

    if mode == "etl":
        stages = ["extract", "validate", "transform", "load"]
    else:
        stages = ["extract", "load_raw", "transform_in_target"]

    table = Table(show_header=True, box=None, padding=(0, 1))
    table.add_column("Stage", style="cyan", width=18)
    table.add_column("Status", width=8)
    table.add_column("Rows", style="green")
    table.add_column("Duration", style="yellow", justify="right")

    for stage in stages:
        status_val = "done"
        if failed_stage and stage == failed_stage:
            status_val = "failed"
        elif failed_stage and failed_stage in stages:
            idx_failed = stages.index(failed_stage)
            idx_stage = stages.index(stage)
            if idx_stage > idx_failed:
                status_val = "skipped"

        label = _STAGE_LABELS.get(stage, stage.capitalize())
        row_info = _get_row_info(stage, state)
        duration = _get_duration(stage, state)
        icon = _STATUS_ICONS.get(status_val, " ")

        table.add_row(label, icon, row_info, duration)

        if stage == "transform":
            _add_step_breakdown_rows(table, state)

    console.print(table)

    total_ms = state.get("duration_ms", {}).get("total", 0)
    token_usage = state.get("token_usage", {})
    extras = [f"Total: {total_ms / 1000:.1f}s"]
    if token_usage:
        total_tokens = token_usage.get("total_tokens", 0)
        extras.append(f"Tokens: {total_tokens:,}")
    if state.get("batches_completed"):
        extras.append(f"Batches: {state['batches_completed']:,}")
    if state.get("rows_rejected"):
        extras.append(f"Rejected: {state['rows_rejected']:,}")
    console.print(f"\n[dim]{'  |  '.join(extras)}[/dim]")

    warnings = state.get("warnings", [])
    if warnings:
        console.print()
        console.print(Rule(f"[yellow]Warnings ({len(warnings)})[/yellow]", style="yellow"))
        for w in warnings:
            console.print(f"  [yellow]⚠[/yellow] {w}")


def _print_error_panel(
    state: dict[str, Any],
    error: Exception,
    verbose: bool,
    failed_stage: str | None = None,
) -> None:
    """Print a rich error panel with context and tips."""
    run_id = state.get("run_id", "unknown")

    user_msg = _format_user_error(error, stage=failed_stage)

    content = f"[red]{user_msg}[/red]"

    if verbose:
        import traceback

        tb = traceback.format_exc()
        content += f"\n\n[dim]Full traceback:[/dim]\n[dim]{tb}[/dim]"

    console.print()
    err_console.print(
        Panel(content, title=f"[red bold]Pipeline Failed[/red bold] (run_id={run_id})"),
    )


# Commands


@app.command()
def run(
    config_file: Path = typer.Argument(None, help="Path to pipeline YAML config"),
    config: Path | None = typer.Option(
        None, "--config", "-c", help="Path to pipeline YAML config (alternative to positional arg)"
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Run without loading to target"),
    verbose: bool = typer.Option(False, "--verbose", help="Print full traceback on errors"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip destructive confirmation prompts"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress progress output"),
    full_refresh: bool = typer.Option(
        False, "--full-refresh", help="Ignore the saved cursor and re-extract everything"
    ),
    local: bool = typer.Option(
        False,
        "--local",
        help="Explicitly run inside the CLI process instead of using loaferd.",
    ),
) -> None:
    """Run locally only when explicit; durable remote work uses `loafer enqueue`."""
    if not local:
        err_console.print(
            "[red]Inline execution is disabled by default.[/red] "
            "Use [bold]loafer enqueue[/bold] for loaferd or pass [bold]--local[/bold] "
            "for explicit compatibility mode."
        )
        raise typer.Exit(2)
    actual_config = config or config_file
    if not actual_config:
        err_console.print(
            "[red]Error: No config file specified. Use 'loafer run <config.yaml>' or 'loafer run -c <config.yaml>'[/red]"
        )
        raise typer.Exit(1)
    if not actual_config.exists():
        err_console.print(f"[red]Config file not found: {actual_config}[/red]")
        raise typer.Exit(1)

    service = get_local_application(reviewer=RichReviewPort())
    request = RunRequest(
        config_path=str(actual_config),
        dry_run=dry_run,
        auto_confirm=yes,
        full_refresh=full_refresh,
    )
    try:
        plan = service.run_pipeline.create_plan(request)
        pipeline_name = plan.pipeline_name
        mode = plan.mode
    except PipelineError as exc:
        user_msg = _format_user_error(exc)
        err_console.print(f"\n[red]{user_msg}[/red]")
        raise typer.Exit(1) from exc

    console.print(f"\n[bold]Running: {pipeline_name}[/bold] [{mode.upper()}]")
    if plan.incremental_column is not None:
        if full_refresh:
            console.print(f"[dim]Incremental on '{plan.incremental_column}' — full refresh[/dim]")
        else:
            console.print(
                f"[dim]Incremental on '{plan.incremental_column}' — "
                f"cursor: {plan.cursor_value!r}[/dim]"
            )
    console.print(Rule(style="dim"))

    failed_stage: str | None = None
    final_state: Any = None
    active_animator: StageAnimator | None = None

    try:
        for event in service.run_pipeline.stream(request):
            node_name = event.stage
            status = event.status.value
            state = event.snapshot.model_dump(mode="python")
            final_state = state
            if node_name == "batch":
                if active_animator is not None:
                    active_animator.pulse()
                continue
            label = _get_stage_label(node_name, state)
            row_info = _get_row_info(node_name, state)

            if quiet:
                if status == "failed":
                    failed_stage = node_name
                continue

            if status == "running":
                # Start spinner for this stage
                if active_animator is None or active_animator.stage != node_name:
                    if active_animator:
                        active_animator.finish("done", row_info)
                    active_animator = StageAnimator(node_name, label)
                    active_animator.start()
                else:
                    active_animator.pulse()
            else:
                # Stage completed, skipped, or failed
                if active_animator and active_animator.stage == node_name:
                    active_animator.finish(status, row_info)
                    active_animator = None
                else:
                    if active_animator:
                        active_animator.finish("done", row_info)
                        active_animator = None
                    _print_progress_bar(node_name, label, status, row_info)

                if status == "failed":
                    failed_stage = node_name

    except PipelineError as exc:
        if active_animator:
            active_animator.finish("failed")
            active_animator = None
        if not final_state:
            final_state = {"run_id": "unknown", "duration_ms": {}, "warnings": []}
        _print_error_panel(final_state, exc, verbose, failed_stage)
        raise typer.Exit(1) from exc
    except LLMError as exc:
        if active_animator:
            active_animator.finish("failed")
            active_animator = None
        user_msg = _format_user_error(exc)
        err_console.print(f"\n[red]{user_msg}[/red]")
        raise typer.Exit(1) from exc

    if failed_stage:
        if not final_state:
            final_state = {"run_id": "unknown", "duration_ms": {}, "warnings": []}
        _print_error_panel(
            final_state, Exception(f"Stage '{failed_stage}' failed"), verbose, failed_stage
        )
        raise typer.Exit(1)

    _print_summary_table(final_state, mode)

    if dry_run:
        console.print("\n[dim][dry-run] Data was not loaded to target[/dim]")


@app.command()
def validate(
    config_file: Path = _config_arg,
) -> None:
    """Validate a pipeline config without running it."""
    if not config_file.exists():
        err_console.print(f"[red]Config file not found: {config_file}[/red]")
        raise typer.Exit(1)

    try:
        result = get_local_application().validate(config_file)
    except PipelineError as exc:
        # The application service already prefixes "Config validation failed:" — don't
        # wrap it a second time (BUG: doubled error prefix).
        err_console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1) from exc

    console.print("[green]✓ Config is valid[/green]\n")

    table = Table(title="Pipeline Configuration")
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="green")

    plan = result.plan
    if plan.pipeline_name:
        table.add_row("Name", plan.pipeline_name)
    table.add_row("Mode", plan.mode)
    table.add_row("Source", plan.source_type)
    table.add_row("Target", plan.target_type)
    table.add_row("Transform", plan.transform_type)
    table.add_row("Transform class", plan.transform_class)
    table.add_row("Chunk size", str(plan.chunk_size))
    table.add_row("Streaming threshold", str(plan.streaming_threshold))
    table.add_row("Validation strict", str(plan.validation_strict))
    table.add_row("Schema drift", plan.schema_drift_policy)
    table.add_row("Delivery guarantee", plan.delivery_guarantee)
    table.add_row("LLM provider", plan.llm_provider)
    table.add_row("LLM model", plan.llm_model)

    console.print(table)


@app.command()
def connectors() -> None:
    """List available source and target connectors."""
    result = get_local_application().list_connectors()

    console.print("[bold]Available Connectors[/bold]\n")

    source_table = Table(title="Sources")
    source_table.add_column("Type", style="cyan")
    for source_type in result.sources:
        source_table.add_row(source_type)
    console.print(source_table)

    target_table = Table(title="Targets")
    target_table.add_column("Type", style="cyan")
    for target_type in result.targets:
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
    console.print(f"[green]✓ Scheduled pipeline[/green] {schedule_id}")
    console.print(f"  Name:    {pipeline_name}")
    console.print(f"  Config:  {config_file}")
    console.print(f"  Trigger: {trigger_desc}")
    console.print(
        "\nRun [bold]loafer start[/bold] to enqueue due jobs and "
        "[bold]loafer worker[/bold] in a separate process to execute them"
    )


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

    console.print(f"[green]✓ Removed job[/green] {job_id}")


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
    table.add_column("Last Run", style="blue")
    table.add_column("Last Status", style="blue")
    table.add_column("Paused", style="red")

    for job in jobs:
        table.add_row(
            job["id"],
            job["name"] or "—",
            job["config_path"],
            job["trigger"],
            job["next_run"] or "—",
            job.get("last_run") or "—",
            job.get("last_status") or "—",
            "yes" if job["paused"] else "no",
        )

    console.print(table)


@app.command()
def start(
    detached: bool = typer.Option(False, "--detached", "-d", help="Run in background"),
) -> None:
    """Start the scheduler that enqueues due run commands."""
    from loafer.scheduler import PipelineScheduler

    if detached:
        _start_background()
        return

    from loafer.scheduler import configure_file_logging

    log_path = configure_file_logging()
    scheduler = PipelineScheduler()
    scheduler.start()

    console.print("[green]✓ Scheduler started[/green]")
    console.print(f"  Log: {log_path}")
    console.print("  Execution: start `loafer worker` in a separate process")
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
        console.print("[green]✓ Scheduler stopped[/green]")
        raise typer.Exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.stop()
        console.print("[green]✓ Scheduler stopped[/green]")


@app.command(name="_daemon_entry", hidden=True)
def _daemon_entry() -> None:
    """Internal: run the scheduler in the foreground, blocking.

    This is the process the background daemon execs (`python -m loafer
    _daemon_entry`). It wires file logging and blocks in run_forever so the
    scheduler thread stays alive and jobs actually fire.
    """
    from loafer.scheduler import PipelineScheduler, configure_file_logging

    configure_file_logging()
    logging.getLogger("loafer.scheduler").info("Daemon entry: starting scheduler")
    scheduler = PipelineScheduler()
    scheduler.run_forever()


def _start_background() -> None:
    """Start the scheduler as a background daemon."""

    from loafer.daemon import start_daemon

    pid, log_path = start_daemon()
    console.print(f"[green]✓ Scheduler started in background[/green] (PID {pid})")
    console.print(f"  Log: {log_path}")
    console.print("\nManage with:")
    console.print("  [bold]loafer status[/bold]   — check status")
    console.print("  [bold]loafer stop[/bold]     — stop scheduler")
    console.print("  [bold]loafer logs[/bold]     — view logs")


@app.command()
def status() -> None:
    """Check if the background scheduler is running."""
    from loafer.daemon import get_daemon_status

    running, pid, log_path = get_daemon_status()

    if running:
        console.print(f"[green]● Scheduler running[/green] (PID {pid})")
        console.print(f"  Log: {log_path}")

        from loafer.scheduler import PipelineScheduler

        try:
            scheduler = PipelineScheduler()
            jobs = scheduler.list_schedules()
            console.print(f"  Active schedules: {len(jobs)}")
            for job in jobs:
                name = f" ({job['name']})" if job["name"] else ""
                console.print(f"    {job['id']}{name}: {job['trigger']}")
        except Exception:
            pass
    else:
        console.print("[dim]○ Scheduler not running[/dim]")
        console.print("  Start with: [bold]loafer start -d[/bold]")


@app.command()
def stop() -> None:
    """Stop the background scheduler."""
    from loafer.daemon import stop_daemon

    stopped = stop_daemon()
    if stopped:
        console.print("[green]✓ Scheduler stopped[/green]")
    else:
        console.print("[yellow]Scheduler was not running[/yellow]")


@app.command()
def logs(
    follow: bool = typer.Option(True, "--follow", "-f", help="Follow log output"),
    lines: int = typer.Option(50, "--lines", "-n", help="Number of lines to show"),
) -> None:
    """View scheduler logs."""
    from loafer.daemon import get_log_path, tail_log

    log_path = get_log_path()
    if not log_path.exists():
        console.print("[dim]No log file found. Start the scheduler first.[/dim]")
        raise typer.Exit(1)

    if follow:
        import time

        console.print(f"[dim]Tailing {log_path} (Ctrl+C to stop)[/dim]\n")
        offset = 0
        try:
            while True:
                with open(log_path) as f:
                    f.seek(offset)
                    new_content = f.read()
                    if new_content:
                        console.print(new_content, end="")
                        offset = f.tell()
                time.sleep(0.5)
        except KeyboardInterrupt:
            console.print()
    else:
        tail_log(log_path, lines)


@app.command()
def init(
    project_dir: str = typer.Argument(..., help="Directory to create"),
) -> None:
    """Scaffold a new pipeline project interactively."""
    from pathlib import Path as _Path

    target = _Path(project_dir)
    if target.exists():
        err_console.print(f"[red]Directory already exists: {target}[/red]")
        raise typer.Exit(1)

    console.print("\n[bold]Create a new Loafer pipeline[/bold]")
    console.print(Rule(style="dim"))

    # Interactive prompts
    name = typer.prompt("Pipeline name", default=target.name)
    source_type = typer.prompt(
        "Source type",
        default="csv",
        type=click.Choice(["csv", "excel", "postgres", "mysql", "mongo", "rest_api"]),
    )
    target_type = typer.prompt(
        "Target type",
        default="json",
        type=click.Choice(["csv", "json", "postgres"]),
    )
    transform_type = typer.prompt(
        "Transform mode",
        default="custom",
        type=click.Choice(["ai", "custom", "sql"]),
    )
    pipeline_mode = typer.prompt(
        "Pipeline mode",
        default="etl",
        type=click.Choice(["etl", "elt"]),
    )

    # Build source config
    source_config: dict[str, Any] = {"type": source_type}
    if source_type in ("csv", "excel"):
        source_config["path"] = f"data/input.{source_type}"
    elif source_type in ("postgres", "mysql"):
        source_config["url"] = "${DATABASE_URL}"
        source_config["query"] = "SELECT * FROM my_table"
    elif source_type == "mongo":
        source_config["url"] = "${MONGO_URL}"
        source_config["database"] = "mydb"
        source_config["collection"] = "my_collection"
    elif source_type == "rest_api":
        source_config["url"] = "https://api.example.com/data"

    # Build target config
    target_config: dict[str, Any] = {"type": target_type}
    if target_type in ("csv", "json"):
        target_config["path"] = f"data/output.{target_type}"
    elif target_type == "postgres":
        target_config["url"] = "${DATABASE_URL}"
        target_config["table"] = "my_table"

    # Build transform config
    transform_config: dict[str, Any] = {"type": transform_type}
    if transform_type == "custom":
        transform_config["path"] = "transform.py"
    elif transform_type == "ai":
        transform_config["instruction"] = "Describe your transformation here"
    elif transform_type == "sql":
        transform_config["query"] = "SELECT * FROM {{source}}"

    import yaml

    pipeline_config: dict[str, Any] = {
        "name": name,
        "source": source_config,
        "target": target_config,
        "transform": transform_config,
        "mode": pipeline_mode,
        "llm": {
            "provider": "gemini",
            "model": DEFAULT_GEMINI_MODEL,
            "api_key": "${GEMINI_API_KEY}",
        },
    }

    target.mkdir(parents=True)
    (target / "data").mkdir(exist_ok=True)

    yaml_path = target / "pipeline.yaml"
    yaml_path.write_text(yaml.dump(pipeline_config, default_flow_style=False, sort_keys=False))

    # Write transform.py if custom. Initialize to None so the summary below
    # can reference it unconditionally without an UnboundLocalError when the
    # user picks a non-custom transform.
    transform_path: Path | None = None
    if transform_type == "custom":
        transform_path = target / "transform.py"
        transform_path.write_text(
            'def transform(data):\n    """Transform the extracted data.\n\n    Args:\n        data: list[dict] — rows from the source.\n\n    Returns:\n        list[dict] — transformed rows.\n    """\n    return data\n'
        )

    # Write sample data if CSV (same None-init guard as transform_path).
    sample_csv: Path | None = None
    if source_type == "csv":
        sample_csv = target / "data" / "input.csv"
        sample_csv.write_text(
            "id,name,email,score\n1,Alice,alice@example.com,95.5\n2,Bob,bob@example.com,88.0\n"
        )

    readme = target / "README.md"
    readme.write_text(
        f"# {name}\n\n"
        f"Generated by `loafer init`.\n\n"
        f"## Run\n\n"
        f"```bash\n"
        f"uv run loafer run pipeline.yaml --verbose\n"
        f"uv run loafer run pipeline.yaml --dry-run\n"
        f"```\n\n"
        f"## Schedule\n\n"
        f"```bash\n"
        f"uv run loafer schedule pipeline.yaml --cron '0 9 * * *'\n"
        f"uv run loafer start\n"
        f"```\n"
    )

    console.print()
    console.print(f"[green]✓ Created pipeline[/green] in {target}/")
    console.print()
    console.print("Files created:")
    console.print(f"  [cyan]{yaml_path}[/cyan]")
    if transform_path:
        console.print(f"  [cyan]{transform_path}[/cyan]")
    if sample_csv:
        console.print(f"  [cyan]{sample_csv}[/cyan]")
    console.print(f"  [cyan]{readme}[/cyan]")
    console.print()
    console.print("Next steps:")
    console.print(f"  1. Edit [bold]{yaml_path}[/bold] with your connection details")
    if transform_path:
        console.print(f"  2. Edit [bold]{transform_path}[/bold] with your transformation logic")
    console.print(f"  3. Run [bold]loafer run {yaml_path}[/bold]")
