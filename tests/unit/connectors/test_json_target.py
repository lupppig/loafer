"""Tests for JsonTargetConnector."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from loafer.connectors.targets.json_target import JsonTargetConnector
from loafer.exceptions import LoadError


class TestJsonTargetConnector:
    def test_write_basic(self, tmp_path: Any) -> None:
        f = tmp_path / "out.json"
        with JsonTargetConnector(str(f)) as conn:
            conn.write_chunk([{"a": 1}, {"b": 2}])

        data = json.loads(f.read_text())
        assert data == [{"a": 1}, {"b": 2}]

    def test_multiple_chunks(self, tmp_path: Any) -> None:
        f = tmp_path / "out.json"
        with JsonTargetConnector(str(f)) as conn:
            conn.write_chunk([{"x": 1}])
            conn.write_chunk([{"x": 2}])
            conn.write_chunk([{"x": 3}])

        data = json.loads(f.read_text())
        assert len(data) == 3

    def test_decimal_serialization(self, tmp_path: Any) -> None:
        f = tmp_path / "out.json"
        with JsonTargetConnector(str(f)) as conn:
            conn.write_chunk([{"price": Decimal("19.99")}])

        data = json.loads(f.read_text())
        assert data[0]["price"] == 19.99

    def test_uuid_serialization(self, tmp_path: Any) -> None:
        uid = uuid.uuid4()
        f = tmp_path / "out.json"
        with JsonTargetConnector(str(f)) as conn:
            conn.write_chunk([{"id": uid}])

        data = json.loads(f.read_text())
        assert data[0]["id"] == str(uid)

    def test_datetime_serialization(self, tmp_path: Any) -> None:
        dt = datetime(2024, 1, 15, 10, 30, 0)
        f = tmp_path / "out.json"
        with JsonTargetConnector(str(f)) as conn:
            conn.write_chunk([{"ts": dt}])

        data = json.loads(f.read_text())
        assert data[0]["ts"] == "2024-01-15T10:30:00"

    def test_write_mode_error(self, tmp_path: Any) -> None:
        f = tmp_path / "existing.json"
        f.write_text("[]")

        with (
            pytest.raises(LoadError, match="already exists"),
            JsonTargetConnector(str(f), write_mode="error") as _,
        ):
            pass

    def test_creates_parent_dirs(self, tmp_path: Any) -> None:
        f = tmp_path / "sub" / "out.json"
        with JsonTargetConnector(str(f)) as conn:
            conn.write_chunk([{"x": 1}])

        assert f.exists()
        data = json.loads(f.read_text())
        assert len(data) == 1

    def test_empty_array(self, tmp_path: Any) -> None:
        f = tmp_path / "out.json"
        with JsonTargetConnector(str(f)):
            pass  # no chunks written

        data = json.loads(f.read_text())
        assert data == []
