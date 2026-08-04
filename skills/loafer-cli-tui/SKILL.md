---
name: loafer-cli-tui
description: Design, implement, refactor, or review Loafer's Typer/Rich CLI and professional terminal dashboard. Use for commands, local and remote execution modes, run progress, metrics, logs, prompts, JSON output, exit codes, shell automation, accessibility, and full-screen TUI workflows.
---

# Loafer CLI and TUI

Keep the CLI scriptable and the TUI operational. Treat both as clients of the same application
contract used by the web UI.

## Start

1. Inspect `loafer/cli.py`, scheduler/daemon commands, runner interfaces, and tests.
2. Read [references/terminal-contract.md](references/terminal-contract.md) for dashboard layout and
   output rules.
3. Decide whether the command runs locally or against a control-plane API. Do not silently switch.
4. Preserve non-interactive behavior before adding animation or prompts.

Remote mode always calls `loaferd` over the shared HTTPS `/api/v1` contract. Do not add a Unix
socket or direct metadata/worker shortcut for the CLI. Require an explicit `--local` flag for
embedded compatibility mode, and fail clearly if remote configuration or authentication is
missing.

## Keep commands professional

- Use stable command names, options, environment variables, help text, and exit codes.
- Support `--output table|json|yaml`, `--no-color`, `--quiet`, and non-interactive confirmation
  where appropriate.
- Write machine output to stdout and diagnostics/progress to stderr.
- Never parse human-formatted output inside another Loafer component.
- Use typed application-client calls; do not duplicate orchestration in command handlers.
- Obtain CLI credentials through Better Auth device authorization, keep the long-lived credential
  in the OS keyring, and exchange it for a short-lived audience-bound `loaferd` token.
- Redact URLs, headers, environment values, samples, generated code, and exceptions.
- Require explicit confirmation for destructive or expensive work; fail in non-interactive mode
  unless approval was supplied.

## Build the terminal dashboard

- Provide a full-screen TUI for `dashboard`, `runs watch`, or an equivalent explicit command.
- Show workspace/environment, connection state, active/queued/failed runs, throughput, freshness,
  quality, worker health, and recent events.
- Use the same event sequence and metric definitions as the web dashboard.
- Provide run drill-down with stage timeline, partitions, metrics, checkpoint, retry history, and
  filtered logs.
- Support keyboard-only operation, compact terminals, resize, monochrome mode, and screen readers.
- Pause event following while selecting/copying text and surface reconnect/gap state.

## Design run output

- For a foreground run, show one stable line per stage and a concise final summary.
- Avoid rotating marketing-style messages during incidents; prefer exact stage, attempt, rows,
  bytes, duration, and checkpoint.
- Keep detailed logs behind `--verbose`, `logs`, or the TUI log panel.
- Preserve output after failure and return a non-zero exit code.
- Never show success when `last_error`, failed stage state, or incomplete publication exists.

## Validate

Add Typer command tests, snapshot tests for terminal widths and no-color mode, JSON schema tests,
keyboard tests for the TUI, and end-to-end tests for local and API-backed execution. Test broken
pipes, redirected output, signals, reconnects, expired auth, permission denial, and missing TTY.

Run:

```bash
uv run pytest tests/unit -q
uv run ruff check loafer tests
```
