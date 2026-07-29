"""Tests for the transform sandbox and its config.

Resource-limit tests are POSIX-only (rlimits / SIGKILL); they skip on Windows
where the sandbox degrades to a best-effort in-process thread.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from typing import TYPE_CHECKING

import pytest

from loafer.config import load_config
from loafer.core.sandbox import run_sandboxed
from loafer.exceptions import ConfigError, TransformError

if TYPE_CHECKING:
    from pathlib import Path

_PASSTHROUGH = "def transform(d):\n    return d\n"
_posix_only = pytest.mark.skipif(os.name == "nt", reason="rlimits are POSIX-only")


class TestSandboxExecution:
    def test_happy_path(self) -> None:
        code = "def transform(d):\n    return [{'x': r['x'] * 2} for r in d]"
        assert run_sandboxed(code, [{"x": 1}, {"x": 2}], timeout=10) == [{"x": 2}, {"x": 4}]

    def test_transform_print_does_not_corrupt_worker_response(self) -> None:
        code = "def transform(d):\n    print('transform log')\n    return d"
        assert run_sandboxed(code, [{"x": 1}], timeout=10) == [{"x": 1}]

    def test_missing_transform_function(self) -> None:
        with pytest.raises(TransformError, match="does not define"):
            run_sandboxed("y = 1", [], timeout=10)

    def test_non_list_result_rejected(self) -> None:
        with pytest.raises(TransformError, match="must return list"):
            run_sandboxed("def transform(d):\n    return 42", [{"x": 1}], timeout=10)

    def test_runtime_error_is_wrapped(self) -> None:
        with pytest.raises(TransformError, match="execution failed"):
            run_sandboxed("def transform(d):\n    return d[5]", [{"x": 1}], timeout=10)

    @_posix_only
    def test_infinite_loop_times_out(self) -> None:
        start = time.monotonic()
        with pytest.raises(TransformError, match=r"timeout|killed"):
            run_sandboxed(
                "def transform(d):\n    while True:\n        pass",
                [{"x": 1}],
                timeout=2,
                max_memory_mb=256,
            )
        # Should be terminated promptly, not hang indefinitely.
        assert time.monotonic() - start < 8

    @_posix_only
    def test_memory_limit_enforced(self) -> None:
        code = "def transform(d):\n    x = [0] * (10 ** 9)\n    return d"
        with pytest.raises(TransformError):
            run_sandboxed(code, [{"x": 1}], timeout=10, max_memory_mb=128)

    def test_import_primitive_is_removed(self) -> None:
        # No __import__ in the sandbox builtins → `import os` fails in the child,
        # so code cannot pull in os/subprocess/socket the normal way.
        with pytest.raises(TransformError, match="execution failed"):
            run_sandboxed("def transform(d):\n    import os\n    return d", [{"x": 1}], timeout=10)

    @_posix_only
    def test_filesystem_write_is_blocked(self, tmp_path: Path) -> None:
        target = tmp_path / "escaped.txt"
        code = f"def transform(d):\n    open({str(target)!r}, 'w').write('x')\n    return d"
        with pytest.raises(TransformError):
            run_sandboxed(code, [{"x": 1}], timeout=5)
        assert not target.exists()

    @_posix_only
    def test_programmatic_pipeline_without_main_guard(self, tmp_path: Path) -> None:
        """A top-level library call must not be re-imported by the sandbox worker."""
        source = tmp_path / "input.csv"
        source.write_text("id,name\n1,Alice\n", encoding="utf-8")
        transform = tmp_path / "transform.py"
        transform.write_text(
            "def transform(data):\n"
            "    return [{**row, 'name': row['name'].lower()} for row in data]\n",
            encoding="utf-8",
        )
        output = tmp_path / "output.json"
        config = tmp_path / "pipeline.yaml"
        config.write_text(
            textwrap.dedent(
                f"""
                source:
                  type: csv
                  path: {source}
                target:
                  type: json
                  path: {output}
                transform:
                  type: custom
                  path: {transform}
                sandbox:
                  timeout: 2
                  max_memory_mb: 256
                """
            ),
            encoding="utf-8",
        )
        caller = tmp_path / "call_loafer.py"
        caller.write_text(
            "import sys\n"
            "from loafer.runner import run_pipeline\n"
            # Deliberately no if __name__ == '__main__' guard.
            "state = run_pipeline(sys.argv[1], yes=True)\n"
            "print(f\"ROWS={state['rows_loaded']}\")\n",
            encoding="utf-8",
        )

        completed = subprocess.run(
            [sys.executable, str(caller), str(config)],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )

        assert completed.returncode == 0, completed.stderr
        assert "ROWS=1" in completed.stdout
        assert output.exists()


class TestSandboxConfig:
    def _write(self, tmp_path: Path, sandbox_block: str = "") -> Path:
        (tmp_path / "in.csv").write_text("x\n1\n", encoding="utf-8")
        base = textwrap.dedent(
            f"""
            source:
              type: csv
              path: {tmp_path / "in.csv"}
            target:
              type: json
              path: {tmp_path / "out.json"}
            transform:
              type: ai
              instruction: noop
            """
        )
        p = tmp_path / "pipeline.yaml"
        p.write_text(base + sandbox_block, encoding="utf-8")
        return p

    def test_defaults(self, tmp_path: Path) -> None:
        cfg = load_config(self._write(tmp_path))
        assert cfg.sandbox.timeout == 60
        assert cfg.sandbox.max_memory_mb == 512

    def test_custom_limits(self, tmp_path: Path) -> None:
        cfg = load_config(self._write(tmp_path, "sandbox:\n  timeout: 30\n  max_memory_mb: 256\n"))
        assert cfg.sandbox.timeout == 30
        assert cfg.sandbox.max_memory_mb == 256

    def test_non_positive_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="positive"):
            load_config(self._write(tmp_path, "sandbox:\n  timeout: 0\n"))
