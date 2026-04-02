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

- Commits are incremental. One logical unit of work per commit
- Commit messages are lowercase, imperative, and descriptive
- Never start a commit message with `feat:`, `fix:`, `chore:` — just describe what it does
- Never start a commit message with `Phase 0`, `Phase 1`, or any phase label
- Stage only the files you actually created or modified
- Never stage `ANTIGRAVITY_PROMPT.md`

## Testing

- Every agent must be independently testable
- Use fixtures from `tests/conftest.py`
- Unit tests go in `tests/unit/`
- Integration tests go in `tests/integration/`
- End-to-end tests go in `tests/e2e/`
- Each edge case listed in the spec must have a corresponding test

## Pull Request Process

1. Create a feature branch off `main`
2. Write or update tests
3. Ensure `ruff` and `mypy` pass
4. Open a pull request with a clear description of what changed and why
5. Address review feedback promptly
