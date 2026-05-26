#!/usr/bin/env bash
# Install the plain git hooks shipped in this directory.
#
# Usage:
#   bash scripts/git-hooks/install.sh
#
# Idempotent. Backs up any existing hooks to <name>.backup-<timestamp>
# the first time it would overwrite them.
#
# Prefer the pre-commit framework (see .pre-commit-config.yaml) if you
# have it installed — this script exists as a zero-dependency fallback.

set -euo pipefail

repo_root=$(git rev-parse --show-toplevel 2>/dev/null) || {
    echo "install.sh: not inside a git repo" >&2
    exit 1
}

hooks_src="$repo_root/scripts/git-hooks"
hooks_dst="$repo_root/.git/hooks"

mkdir -p "$hooks_dst"

install_one() {
    local name=$1
    local src="$hooks_src/$name"
    local dst="$hooks_dst/$name"

    if [[ ! -f "$src" ]]; then
        echo "install.sh: missing $src" >&2
        return 1
    fi

    # If a real (non-symlink) hook already exists and doesn't match our
    # source, preserve it before we clobber. Symlinks to our source are
    # treated as already-installed.
    if [[ -L "$dst" ]] && [[ "$(readlink "$dst")" == "$src" ]]; then
        echo "  $name: already linked"
        return 0
    fi
    if [[ -e "$dst" ]] && ! cmp -s "$dst" "$src"; then
        local backup="$dst.backup-$(date +%Y%m%d-%H%M%S)"
        mv "$dst" "$backup"
        echo "  $name: existing hook backed up to $(basename "$backup")"
    fi

    ln -sf "$src" "$dst"
    chmod +x "$src"
    echo "  $name: installed"
}

echo "Installing git hooks into .git/hooks/…"
install_one pre-commit
install_one pre-push
echo "Done. Skip a single run with \`git commit --no-verify\` or \`git push --no-verify\`."
