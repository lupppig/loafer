# Terminal contract

## Modes

| Mode | Purpose |
|---|---|
| Plain CLI | scripts, CI, one-shot commands |
| Live foreground | one run with bounded stage updates |
| Full-screen TUI | multi-run operations and drill-down |
| JSON/YAML | stable machine-readable integration |

Never enter the TUI automatically when stdout is redirected.

## Dashboard layout

```text
organization / workspace / environment        connection and event lag
active | queued | failed | success rate | freshness | worker capacity
runs table
selected run: stage timeline and checkpoint
logs/events with severity, stage, worker, attempt, trace
key help and reconnect/error state
```

Use `j/k` or arrows for navigation, `/` for search, `f` for filters, `l` for logs, `r` for refresh,
and `q` for quit where they do not conflict with text input. Confirm cancel, retry, backfill, and
production actions.

## Exit codes

Keep a documented stable mapping:

- `0`: successful command or completed run;
- `1`: pipeline/run failure;
- `2`: invalid command or configuration;
- `3`: connection or authentication failure;
- `4`: permission denied;
- `5`: cancelled or interrupted;
- `6`: unavailable service or worker capacity.

Do not overload exit codes with stage-specific details; put structured details in JSON output.
