"""Tests for Load Agent."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from loafer.agents.load import load_agent
from loafer.exceptions import LoadError


class TestLoadAgent:
    def test_writes_all_rows(self) -> None:
        mock_connector = MagicMock()
        mock_connector.write_chunk.return_value = 2

        with patch(
            "loafer.agents.load.get_target_connector",
            return_value=mock_connector,
        ):
            state: dict[str, Any] = {
                "target_config": {"type": "csv", "path": "/tmp/out.csv"},
                "transformed_data": [
                    {"id": 1, "name": "Alice"},
                    {"id": 2, "name": "Bob"},
                ],
                "is_streaming": False,
                "chunk_size": 500,
                "duration_ms": {},
                "warnings": [],
            }
            result = load_agent(state)

        assert result["rows_loaded"] == 2
        mock_connector.write_chunk.assert_called_once()
        mock_connector.finalize.assert_called_once()

    def test_streaming_mode(self) -> None:
        """In streaming mode, transform has already consumed the stream
        and stored results in transformed_data. Load writes from there."""
        mock_connector = MagicMock()
        mock_connector.write_chunk.return_value = 3

        with patch(
            "loafer.agents.load.get_target_connector",
            return_value=mock_connector,
        ):
            state: dict[str, Any] = {
                "target_config": {"type": "csv", "path": "/tmp/out.csv"},
                "is_streaming": True,
                "chunk_size": 500,
                "transformed_data": [
                    {"id": 1},
                    {"id": 2},
                    {"id": 3},
                ],
                "duration_ms": {},
                "warnings": [],
            }
            result = load_agent(state)

        assert result["rows_loaded"] == 3
        assert mock_connector.write_chunk.call_count == 1

    def test_connection_failure_raises(self) -> None:
        mock_connector = MagicMock()
        mock_connector.connect.side_effect = Exception("connection refused")

        with patch(
            "loafer.agents.load.get_target_connector",
            return_value=mock_connector,
        ):
            state: dict[str, Any] = {
                "target_config": {"type": "csv", "path": "/tmp/out.csv"},
                "transformed_data": [],
                "is_streaming": False,
                "duration_ms": {},
                "warnings": [],
            }
            with pytest.raises(LoadError, match="Failed to connect"):
                load_agent(state)

    def test_empty_transformed_data(self) -> None:
        mock_connector = MagicMock()

        with patch(
            "loafer.agents.load.get_target_connector",
            return_value=mock_connector,
        ):
            state: dict[str, Any] = {
                "target_config": {"type": "csv", "path": "/tmp/out.csv"},
                "transformed_data": [],
                "is_streaming": False,
                "chunk_size": 500,
                "duration_ms": {},
                "warnings": [],
            }
            result = load_agent(state)

        assert result["rows_loaded"] == 0
        assert any("No rows" in w for w in result["warnings"])

    def test_write_chunk_failure_records_partial_count(self) -> None:
        mock_connector = MagicMock()
        mock_connector.write_chunk.side_effect = [2, Exception("disk full")]

        with patch(
            "loafer.agents.load.get_target_connector",
            return_value=mock_connector,
        ):
            state: dict[str, Any] = {
                "target_config": {"type": "csv", "path": "/tmp/out.csv"},
                "transformed_data": [
                    {"id": 1},
                    {"id": 2},
                    {"id": 3},
                    {"id": 4},
                ],
                "is_streaming": False,
                "chunk_size": 2,
                "duration_ms": {},
                "warnings": [],
            }
            with pytest.raises(LoadError, match="2 rows"):
                load_agent(state)
