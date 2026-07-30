"""Backward-compatible local runner facade.

New clients should use :mod:`loafer.application`. These functions preserve
the pre-Phase 1 Python API while delegating orchestration to the same
application use cases used by the CLI and scheduler.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from loafer.adapters.runtime import EnvironmentSecretResolver
from loafer.application import RunRequest, get_local_application
from loafer.config import PipelineConfig
from loafer.engine import (
    _build_initial_state,
    _raise_on_terminal_failure,
    _transform_requires_llm,
)
from loafer.engine import (
    _build_llm_provider as _engine_build_llm_provider,
)
from loafer.graph.state import PipelineState
from loafer.ports.llm import LLMProvider
from loafer.ports.runtime import SecretResolver

_build_llm_provider = _engine_build_llm_provider


def run_pipeline(
    config_path: str | Path,
    dry_run: bool = False,
    verbose: bool = False,
    yes: bool = False,
    full_refresh: bool = False,
) -> PipelineState:
    """Run a pipeline and return its legacy in-process state."""
    request = RunRequest(
        config_path=str(config_path),
        dry_run=dry_run,
        auto_confirm=yes,
        full_refresh=full_refresh,
    )
    state = get_local_application(provider_factory=_compat_provider_factory).run_pipeline.run_state(
        request
    )
    if verbose:
        _print_legacy_summary(state)
    return state


def run_pipeline_streaming(
    config_path: str | Path,
    dry_run: bool = False,
    yes: bool = False,
    full_refresh: bool = False,
) -> Iterator[tuple[str, str, PipelineState]]:
    """Yield legacy stage tuples through the application use case."""
    request = RunRequest(
        config_path=str(config_path),
        dry_run=dry_run,
        auto_confirm=yes,
        full_refresh=full_refresh,
    )
    service = get_local_application(provider_factory=_compat_provider_factory)
    for event, state in service.run_pipeline.stream_states(request):
        yield event.stage, event.status, state


def validate_config(config_path: str | Path) -> PipelineConfig:
    """Validate a config and return the legacy Pydantic model."""
    return get_local_application().validate_config_model(config_path)


def list_connectors() -> dict[str, list[str]]:
    """Return the legacy connector catalog mapping."""
    catalog = get_local_application().list_connectors()
    return {
        "sources": list(catalog.sources),
        "targets": list(catalog.targets),
    }


def _compat_provider_factory(
    config: PipelineConfig,
    secret_resolver: SecretResolver | None,
) -> LLMProvider:
    """Keep monkeypatching the legacy provider factory effective."""
    if _build_llm_provider is _engine_build_llm_provider:
        return _build_llm_provider(
            config,
            secret_resolver or EnvironmentSecretResolver(),
        )
    return _build_llm_provider(config)


def _print_legacy_summary(state: PipelineState) -> None:
    """Preserve ``run_pipeline(verbose=True)`` as a client-side concern."""
    from rich.console import Console

    console = Console()
    console.print(f"\n[bold]Pipeline Summary[/bold] (run_id={state.get('run_id', 'unknown')})")
    console.print(f"  Rows extracted: {state.get('rows_extracted', 0)}")
    console.print(f"  Rows loaded:    {state.get('rows_loaded', 0)}")
    console.print(f"  Warnings:       {len(state.get('warnings', []))}")
    if state.get("token_usage"):
        console.print(f"  Token usage:    {state.get('token_usage', {})}")
    console.print(f"  Duration:       {state.get('duration_ms', {}).get('total', 0):.0f}ms")


__all__ = [
    "_build_initial_state",
    "_build_llm_provider",
    "_raise_on_terminal_failure",
    "_transform_requires_llm",
    "list_connectors",
    "run_pipeline",
    "run_pipeline_streaming",
    "validate_config",
]
