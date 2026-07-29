"""Tests for Extract Agent."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from loafer.agents.extract import extract_agent
from loafer.config import CsvSourceConfig, IncrementalConfig
from loafer.exceptions import ExtractionError


class TestExtractAgent:
    def test_small_dataset_not_streaming(self) -> None:
        mock_connector = MagicMock()
        mock_connector.count.return_value = 50
        mock_connector.read_all.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        with patch(
            "loafer.agents.extract.get_source_connector",
            return_value=mock_connector,
        ):
            state: dict[str, Any] = {
                "source_config": {"type": "csv", "path": "/tmp/data.csv"},
                "streaming_threshold": 1000,
                "chunk_size": 500,
                "duration_ms": {},
                "warnings": [],
            }
            result = extract_agent(state)

        assert result["is_streaming"] is False
        assert len(result["raw_data"]) == 2
        assert result["rows_extracted"] == 2
        assert "extract" in result["duration_ms"]

    def test_large_dataset_streaming(self) -> None:
        mock_connector = MagicMock()
        mock_connector.count.return_value = 50000
        mock_stream = iter([[{"id": i}] for i in range(10)])
        mock_connector.stream.return_value = mock_stream

        with patch(
            "loafer.agents.extract.get_source_connector",
            return_value=mock_connector,
        ):
            state: dict[str, Any] = {
                "source_config": {"type": "postgres", "url": "..."},
                "streaming_threshold": 10000,
                "chunk_size": 500,
                "duration_ms": {},
                "warnings": [],
            }
            result = extract_agent(state)

        assert result["is_streaming"] is True
        assert result["stream_iterator"] is not None
        assert "schema_sample" in result

    def test_connection_failure_raises(self) -> None:
        mock_connector = MagicMock()
        mock_connector.connect.side_effect = Exception("connection refused")

        with patch(
            "loafer.agents.extract.get_source_connector",
            return_value=mock_connector,
        ):
            state: dict[str, Any] = {
                "source_config": {"type": "postgres", "url": "..."},
                "duration_ms": {},
                "warnings": [],
            }
            with pytest.raises(ExtractionError, match="Failed to connect"):
                extract_agent(state)

    def test_zero_rows_warning(self) -> None:
        mock_connector = MagicMock()
        mock_connector.count.return_value = 0
        mock_connector.read_all.return_value = []

        with patch(
            "loafer.agents.extract.get_source_connector",
            return_value=mock_connector,
        ):
            state: dict[str, Any] = {
                "source_config": {"type": "csv", "path": "/tmp/empty.csv"},
                "streaming_threshold": 1000,
                "chunk_size": 500,
                "duration_ms": {},
                "warnings": [],
            }
            result = extract_agent(state)

        assert "Source returned 0 rows" in result["warnings"]

    def test_streaming_unknown_count_no_false_zero_warning(self) -> None:
        """BUG-5: Postgres count() is None → must not warn '0 rows' before drain.

        The warning previously fired at extract time using the placeholder
        count of 0, even though thousands of rows streamed afterward. It must
        not be present until (and unless) the stream actually yields nothing.
        """
        mock_connector = MagicMock()
        mock_connector.count.return_value = None  # Postgres behavior
        mock_connector.stream.return_value = iter([[{"id": i} for i in range(500)]])

        with patch(
            "loafer.agents.extract.get_source_connector",
            return_value=mock_connector,
        ):
            state: dict[str, Any] = {
                "source_config": {"type": "postgres", "url": "..."},
                "streaming_threshold": 10000,
                "chunk_size": 500,
                "duration_ms": {},
                "warnings": [],
            }
            result = extract_agent(state)

        # No premature warning right after extract_agent returns.
        assert "Source returned 0 rows" not in result["warnings"]

        # Drain the stream → real count known, still no false warning.
        drained = [row for chunk in result["stream_iterator"] for row in chunk]
        assert len(drained) == 500
        assert result["rows_extracted"] == 500
        assert "Source returned 0 rows" not in result["warnings"]

    def test_streaming_truly_empty_warns_after_drain(self) -> None:
        """A genuinely empty stream must still warn — once the stream is drained."""
        mock_connector = MagicMock()
        mock_connector.count.return_value = None
        mock_connector.stream.return_value = iter([])

        with patch(
            "loafer.agents.extract.get_source_connector",
            return_value=mock_connector,
        ):
            state: dict[str, Any] = {
                "source_config": {"type": "postgres", "url": "..."},
                "streaming_threshold": 10000,
                "chunk_size": 500,
                "duration_ms": {},
                "warnings": [],
            }
            result = extract_agent(state)

        # Not warned yet (stream not consumed)...
        assert "Source returned 0 rows" not in result["warnings"]
        # ...but warned once drained and the true count (0) is known.
        list(result["stream_iterator"])
        assert "Source returned 0 rows" in result["warnings"]

    def test_schema_sample_built(self) -> None:
        mock_connector = MagicMock()
        mock_connector.count.return_value = 3
        mock_connector.read_all.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]

        with patch(
            "loafer.agents.extract.get_source_connector",
            return_value=mock_connector,
        ):
            state: dict[str, Any] = {
                "source_config": {"type": "csv", "path": "/tmp/data.csv"},
                "streaming_threshold": 1000,
                "chunk_size": 500,
                "duration_ms": {},
                "warnings": [],
            }
            result = extract_agent(state)

        assert "schema_sample" in result
        assert "id" in result["schema_sample"]
        assert "name" in result["schema_sample"]

    def test_csv_incremental_filters_stream_client_side(self, tmp_path: Any) -> None:
        source_path = tmp_path / "orders.csv"
        source_path.write_text("id\n1\n")
        mock_connector = MagicMock()
        mock_connector.count.return_value = 4
        mock_connector.stream.return_value = iter(
            [[{"id": 1}, {"id": 2}], [{"id": None}, {"id": 3}]]
        )

        with patch(
            "loafer.agents.extract.get_source_connector",
            return_value=mock_connector,
        ) as get_connector:
            state: dict[str, Any] = {
                "source_config": CsvSourceConfig(type="csv", path=str(source_path)),
                "incremental_config": IncrementalConfig(column="id", initial=1),
                "cursor_value": 1,
                "streaming_threshold": 2,
                "chunk_size": 2,
                "duration_ms": {},
                "warnings": [],
            }
            result = extract_agent(state)
            drained = [row for chunk in result["stream_iterator"] for row in chunk]

        assert drained == [{"id": 2}, {"id": 3}]
        assert result["rows_extracted"] == 2
        assert result["new_cursor"] == 3
        assert any("client-side" in warning for warning in result["warnings"])
        get_connector.assert_called_once_with(state["source_config"])

    def test_non_streaming_schema_statistics_use_all_rows(self) -> None:
        mock_connector = MagicMock()
        mock_connector.count.return_value = 9
        rows = [{"value": "x"} for _ in range(8)] + [{"value": None}]
        mock_connector.read_all.return_value = rows

        with patch(
            "loafer.agents.extract.get_source_connector",
            return_value=mock_connector,
        ):
            state: dict[str, Any] = {
                "source_config": {"type": "csv", "path": "/tmp/data.csv"},
                "streaming_threshold": 1000,
                "chunk_size": 500,
                "duration_ms": {},
                "warnings": [],
            }
            result = extract_agent(state)

        metadata = result["schema_sample"]["value"]
        assert metadata["total_count"] == 9
        assert metadata["null_count"] == 1
        assert metadata["nullable"] is True
        assert len(metadata["sample_values"]) <= 5
