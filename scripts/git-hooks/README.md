# Git hooks

Two ways to run the same Python and web lint checks locally before a
commit lands.

## Option A — pre-commit framework (preferred)

If you have [pre-commit](https://pre-commit.com) installed:

```sh
pip install pre-commit            # or: uv tool install pre-commit
pre-commit install                # installs into .git/hooks/pre-commit
```

Config lives at [`.pre-commit-config.yaml`](../../.pre-commit-config.yaml).
The pinned ruff version there should match `uv.lock`.

## Option B — plain shell scripts (zero deps)

If you don't want to install pre-commit:

```sh
bash scripts/git-hooks/install.sh
```

This symlinks `pre-commit` and `pre-push` from this directory into
`.git/hooks/`. Existing hooks are backed up to `<name>.backup-<ts>`.

### What they do

- **pre-commit** — runs `ruff check --fix` and `ruff format` on staged
  `.py` files, plus ESLint `--fix` on staged JavaScript/TypeScript files
  under `web/`. It re-stages only those files and blocks the commit on
  errors the linters cannot fix. Unstaged changes are stashed with
  `--keep-index` so partial staging is preserved. Install web dependencies
  with `npm ci` in `web/` before committing web source changes.
- **pre-push** — runs `ruff check .` and `ruff format --check .` on
  the whole tree, mirroring CI. Catches commits made with
  `--no-verify` before they reach the remote.

### Skipping

Both hooks honor `--no-verify`:

```sh
git commit --no-verify
git push --no-verify
```

CI is still the backstop in either case.
