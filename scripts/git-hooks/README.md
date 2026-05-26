# Git hooks

Two ways to get the same checks the CI lint job runs (`ruff check` +
`ruff format --check`) to run locally before a commit lands.

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

- **pre-commit** — on staged `.py` files only: runs `ruff check --fix`
  then `ruff format`, re-stages the files, blocks the commit only if
  ruff reports errors it can't auto-fix. Unstaged changes are stashed
  with `--keep-index` so partial staging is preserved.
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
