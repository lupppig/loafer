"""Tests for Load Raw Agent (ELT mode)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loafer.agents.load_raw import (
    _build_staging_table_name,
    _write_stream_raw,
    load_raw_agent,
)
from loafer.exceptions import LoadError


class TestBuildStagingTableName:
    def test_generates_unique_name(self) -> None:
        config = MagicMock(type="postgres")
        name1 = _build_staging_table_name(config)
        name2 = _build_staging_table_name(config)
        assert name1 != name2

    def test_includes_target_type(self) -> None:
        config = MagicMock(type="mongo")
        name = _build_staging_table_name(config)
        assert "mongo" in name

    def test_prefix_is_loafer_raw(self) -> None:
        config = MagicMock(type="postgres")
        name = _build_staging_table_name(config)
        assert name.startswith("loafer_raw_")

    def test_unknown_type_fallback(self) -> None:
        config = MagicMock(spec=[])  # no type attribute
        name = _build_staging_table_name(config)
        assert "unknown" in name


class TestWriteStreamRaw:
    def test_writes_first_chunk(self) -> None:
        connector = MagicMock()
        connector.write_chunk.return_value = 10
        state: dict[str, Any] = {
            "_first_chunk": [{"id": i} for i in range(10)],
            "stream_iterator": iter([]),
        }
        total = _write_stream_raw(connector, state, 500)
        assert total == 10
        connector.write_chunk.assert_called_once()

    def test_writes_all_stream_chunks(self) -> None:
        connector = MagicMock()
        connector.write_chunk.side_effect = [5, 3, 2]
        state: dict[str, Any] = {
            "_first_chunk": None,
            "stream_iterator": iter(
                [
                    [{"id": i} for i in range(5)],
                    [{"id": i} for i in range(5, 8)],
                    [{"id": i} for i in range(8, 10)],
                ]
            ),
        }
        total = _write_stream_raw(connector, state, 500)
        assert total == 10
        assert connector.write_chunk.call_count == 3

    def test_no_first_chunk_no_stream(self) -> None:
        connector = MagicMock()
        state: dict[str, Any] = {
            "_first_chunk": None,
            "stream_iterator": None,
        }
        total = _write_stream_raw(connector, state, 500)
        assert total == 0
        connector.write_chunk.assert_not_called()


class TestLoadRawAgent:
    def test_connect_failure_raises(self) -> None:
        mock_connector = MagicMock()
        mock_connector.connect.side_effect = ConnectionError("refused")

        state: dict[str, Any] = {
            "target_config": MagicMock(),
            "raw_data": [],
            "is_streaming": False,
            "chunk_size": 500,
            "duration_ms": {},
        }

        with pytest.raises(LoadError, match="refused"):
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(
                    "loafer.agents.load_raw.get_target_connector",
                    lambda cfg: mock_connector,
                )
                load_raw_agent(state)

    def test_loads_raw_data_non_streaming(self) -> None:
        mock_connector = MagicMock()
        mock_connector.write_chunk.return_value = 5

        state: dict[str, Any] = {
            "target_config": MagicMock(type="postgres"),
            "raw_data": [{"id": i} for i in range(10)],
            "is_streaming": False,
            "chunk_size": 5,
            "duration_ms": {},
        }

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "loafer.agents.load_raw.get_target_connector",
                lambda cfg: mock_connector,
            )
            result = load_raw_agent(state)

        assert result["raw_table_name"].startswith("loafer_raw_")
        assert result["rows_extracted"] == 10
        assert "load_raw" in result["duration_ms"]
        assert mock_connector.write_chunk.call_count == 2
        mock_connector.finalize.assert_called_once()
        mock_connector.disconnect.assert_called_once()

    def test_loads_raw_data_streaming(self) -> None:
        mock_connector = MagicMock()
        mock_connector.write_chunk.side_effect = [3, 2]

        state: dict[str, Any] = {
            "target_config": MagicMock(type="postgres"),
            "raw_data": [],
            "is_streaming": True,
            "chunk_size": 500,
            "_first_chunk": [{"id": i} for i in range(3)],
            "stream_iterator": iter(
                [
                    [{"id": i} for i in range(3, 5)],
                ]
            ),
            "duration_ms": {},
        }

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "loafer.agents.load_raw.get_target_connector",
                lambda cfg: mock_connector,
            )
            result = load_raw_agent(state)

        assert result["rows_extracted"] == 5
        assert mock_connector.write_chunk.call_count == 2

    def test_empty_raw_data(self) -> None:
        mock_connector = MagicMock()

        state: dict[str, Any] = {
            "target_config": MagicMock(type="postgres"),
            "raw_data": [],
            "is_streaming": False,
            "chunk_size": 500,
            "duration_ms": {},
        }

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "loafer.agents.load_raw.get_target_connector",
                lambda cfg: mock_connector,
            )
            result = load_raw_agent(state)

        assert result["rows_extracted"] == 0
        mock_connector.write_chunk.assert_not_called()
        mock_connector.finalize.assert_called_once()

    def test_disconnect_on_exception(self) -> None:
        mock_connector = MagicMock()
        mock_connector.write_chunk.side_effect = RuntimeError("boom")

        state: dict[str, Any] = {
            "target_config": MagicMock(type="postgres"),
            "raw_data": [{"id": 1}],
            "is_streaming": False,
            "chunk_size": 500,
            "duration_ms": {},
        }

        with pytest.raises(LoadError, match="boom"):
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(
                    "loafer.agents.load_raw.get_target_connector",
                    lambda cfg: mock_connector,
                )
                load_raw_agent(state)

        mock_connector.disconnect.assert_called_once()
