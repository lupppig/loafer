"""Tests for loafer.transform.code_validator."""

from __future__ import annotations

from loafer.transform.code_validator import validate_transform_function


class TestValidateTransformFunction:
    """Code validator edge cases from the spec."""

    def test_valid_transform_function(self) -> None:
        code = "def transform(data: list[dict]) -> list[dict]:\n    return data\n"
        ok, err = validate_transform_function(code)
        assert ok is True
        assert err is None

    def test_valid_with_helper_functions(self) -> None:
        code = (
            "def clean(s: str) -> str:\n"
            "    return s.strip().lower()\n"
            "\n"
            "def transform(data: list[dict]) -> list[dict]:\n"
            "    return [{k: clean(v) if isinstance(v, str) else v for k, v in row.items()} for row in data]\n"
        )
        ok, err = validate_transform_function(code)
        assert ok is True
        assert err is None

    def test_valid_with_stdlib_imports(self) -> None:
        code = (
            "import re\n"
            "import json\n"
            "from datetime import datetime\n"
            "\n"
            "def transform(data: list[dict]) -> list[dict]:\n"
            "    return data\n"
        )
        ok, err = validate_transform_function(code)
        assert ok is True
        assert err is None

    def test_blocked_import_os(self) -> None:
        code = "import os\ndef transform(data):\n    return data\n"
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "os" in err

    def test_blocked_from_import_subprocess(self) -> None:
        code = "from subprocess import run\ndef transform(data):\n    return data\n"
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "subprocess" in err

    def test_blocked_eval_call(self) -> None:
        code = "def transform(data):\n    return eval('data')\n"
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "eval" in err

    def test_blocked_exec_call(self) -> None:
        code = "def transform(data):\n    exec('pass')\n    return data\n"
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "exec" in err

    def test_blocked_open_call(self) -> None:
        code = "def transform(data):\n    f = open('/etc/passwd')\n    return data\n"
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "open" in err

    def test_blocked_builtins(self) -> None:
        code = "def transform(data):\n    return __builtins__\n"
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "__builtins__" in err

    def test_no_transform_function(self) -> None:
        code = "def process(data):\n    return data\n"
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "transform" in err.lower()

    def test_syntax_error(self) -> None:
        code = "def transform(data:\n    return data"
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "syntax" in err.lower()

    def test_wrong_parameter_count_two_params(self) -> None:
        code = "def transform(a, b):\n    return a\n"
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "parameter" in err.lower()

    def test_wrong_parameter_count_zero_params(self) -> None:
        code = "def transform():\n    return []\n"
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "parameter" in err.lower()

    def test_code_too_long(self) -> None:
        lines = ["def transform(data):"] + ["    x = 1"] * 201 + ["    return data"]
        code = "\n".join(lines)
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "lines" in err.lower()

    def test_blocked_import_httpx(self) -> None:
        code = "import httpx\ndef transform(data):\n    return data\n"
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "httpx" in err

    def test_blocked_import_requests(self) -> None:
        code = "import requests\ndef transform(data):\n    return data\n"
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "requests" in err

    def test_blocked_import_socket(self) -> None:
        code = "import socket\ndef transform(data):\n    return data\n"
        ok, err = validate_transform_function(code)
        assert ok is False
        assert err is not None and "socket" in err
