# Contributing to Loafer

Loafer is an open-source CLI tool for AI-assisted ETL and ELT pipelines.
Contributions are welcome. Please read this guide before opening a pull request.

## Development Setup

This project uses Python 3.11+ and `uv` for package management.

```bash
# Clone the repository
git clone https://github.com/your-org/loafer.git
cd loafer

# Create virtual environment and install dependencies
uv sync

# Run the full test suite
uv run pytest

# Run linter and type checker
uv run ruff check loafer tests
uv run mypy loafer
```

## Architecture

Loafer follows the Ports and Adapters (Hexagonal) pattern:

- `loafer/core/` — pure domain logic. No I/O. No framework imports.
- `loafer/ports/` — abstract interfaces (ABCs). What the core needs from the outside.
- `loafer/adapters/` — concrete implementations. Connectors, LLM clients, schedulers.
- `loafer/graph/` — LangGraph wiring. Depends on core + ports only.
- `loafer/cli.py` — entry point. Assembles adapters and hands them to the graph.

The core domain never imports from infrastructure.

## Adding a New Connector

1. Create a file in `loafer/connectors/sources/` or `loafer/connectors/targets/`
2. Implement the `SourceConnector` or `TargetConnector` ABC from `loafer/connectors/base.py`
3. Register it in `loafer/connectors/registry.py`
4. Add tests in `tests/unit/connectors/`
5. Follow the edge cases documented in the spec

## Adding a New LLM Provider

1. Create a file in `loafer/llm/` implementing `LLMProvider` from `loafer/llm/base.py`
2. Register it in `loafer/llm/registry.py`
3. Add tests in `tests/unit/`
4. Agent code must never import a specific provider directly

## Code Style

- All code is formatted with `ruff`
- Type annotations are required. `mypy` must pass in strict mode
- No unnecessary comments. If the code is clear, there is no comment
- No TODO comments committed to the repo
- No commented-out code blocks
- No numbered comments
- Exception: docstrings are encouraged for public APIs

## Git Hygiene

- Name branches for the work using a conventional work-type prefix:
  `feat/<scope>`, `fix/<scope>`, `docs/<scope>`, `refactor/<scope>`,
  `test/<scope>`, or `chore/<scope>`
- Never use an agent/tool prefix such as `agent/`, and never name branches after roadmap phases
  such as `phase-1` or `phase-2`
- Commits are incremental. One logical unit of work per commit
- Commit messages are lowercase, imperative, and descriptive
- Never start a commit message with `feat:`, `fix:`, `chore:` — just describe what it does
- Never start a commit message with `Phase 0`, `Phase 1`, or any phase label
- Stage only the files you actually created or modified
- Never commit local prompt scratchpads, credentials, or generated test secrets
- Before every commit, update the `[Unreleased]` section of `CHANGELOG.md` with the change; never
  commit implementation work first and backfill its changelog entry later

## Testing

- Every agent must be independently testable
- Use fixtures from `tests/conftest.py`
- Unit tests go in `tests/unit/`
- Integration tests go in `tests/integration/`
- End-to-end tests go in `tests/e2e/`
- Each edge case listed in the spec must have a corresponding test

## Pre-Release Smoke Test

Before cutting a release, verify the **built artifacts** (not the dev `.venv`)
actually run. `uv sync` pulls in transitive dependencies that the published
wheel / Docker image may not declare, so a pipeline can pass locally and crash
on every command once installed by a user. The smoke harness installs the built
artifact into a clean container and runs a real pipeline end-to-end:

```bash
# Build + test both the PyPI wheel and the Docker image in clean rooms
scripts/smoke.sh            # default: all
scripts/smoke.sh wheel      # just the pip-installable wheel
scripts/smoke.sh docker     # just the Docker image

# Test as if releasing a specific version
VERSION=0.3.2 scripts/smoke.sh
```

Requires Docker. The same checks run automatically in CI (the `smoke` job in
`ci.yml`) and gate the real release: `publish.yml` will not publish a wheel that
fails the clean-room test, and `docker.yml` will not push an image that fails.

## Pull Request Process

1. Create a feature branch off `main`
2. Write or update tests
3. Ensure `ruff` and `mypy` pass
4. Open a pull request with a clear description of what changed and why
5. Address review feedback promptly
