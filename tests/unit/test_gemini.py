"""Tests for loafer.llm.gemini — Gemini provider (mocked SDK)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from loafer.exceptions import LLMInvalidOutputError, LLMRateLimitError
from loafer.llm.gemini import GeminiProvider, _strip_markdown_fences

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(
    text: str,
    prompt_tokens: int = 100,
    completion_tokens: int = 50,
    total_tokens: int = 150,
) -> MagicMock:
    """Build a fake Gemini response object."""
    resp = MagicMock()
    resp.text = text
    resp.usage_metadata = SimpleNamespace(
        prompt_token_count=prompt_tokens,
        candidates_token_count=completion_tokens,
        total_token_count=total_tokens,
    )
    return resp


# ---------------------------------------------------------------------------
# _strip_markdown_fences
# ---------------------------------------------------------------------------


class TestStripMarkdownFences:
    def test_plain_text_unchanged(self) -> None:
        assert _strip_markdown_fences("def transform(d): return d") == "def transform(d): return d"

    def test_python_fence_stripped(self) -> None:
        raw = "```python\ndef transform(d): return d\n```"
        assert _strip_markdown_fences(raw) == "def transform(d): return d"

    def test_sql_fence_stripped(self) -> None:
        raw = "```sql\nSELECT 1\n```"
        assert _strip_markdown_fences(raw) == "SELECT 1"

    def test_bare_fence_stripped(self) -> None:
        raw = "```\nSELECT 1\n```"
        assert _strip_markdown_fences(raw) == "SELECT 1"


# ---------------------------------------------------------------------------
# GeminiProvider
# ---------------------------------------------------------------------------


class TestGeminiProviderTransform:
    """generate_transform_function tests."""

    def test_successful_response(self) -> None:
        code = "def transform(data):\n    return data\n"
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = _make_response(code)

        with patch("loafer.llm.gemini.genai.Client", return_value=mock_client):
            provider = GeminiProvider(api_key="test-key")
            result = provider.generate_transform_function(
                schema_sample={"col": {}}, instruction="noop"
            )

        assert result.code == code.strip()
        assert result.token_usage["prompt_tokens"] == 100
        assert result.token_usage["completion_tokens"] == 50
        assert result.token_usage["total_tokens"] == 150

    def test_response_with_markdown_fences(self) -> None:
        raw = "```python\ndef transform(data):\n    return data\n```"
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = _make_response(raw)

        with patch("loafer.llm.gemini.genai.Client", return_value=mock_client):
            provider = GeminiProvider(api_key="test-key")
            result = provider.generate_transform_function({}, "noop")

        assert "```" not in result.code
        assert "def transform" in result.code

    def test_empty_response_raises(self) -> None:
        resp = MagicMock()
        resp.text = ""
        resp.usage_metadata = None
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = resp

        with patch("loafer.llm.gemini.genai.Client", return_value=mock_client):
            provider = GeminiProvider(api_key="test-key")
            with pytest.raises(LLMInvalidOutputError):
                provider.generate_transform_function({}, "noop")

    def test_rate_limit_raises_after_retries(self) -> None:
        from google.genai import errors as genai_errors

        mock_client = MagicMock()
        mock_client.models.generate_content.side_effect = genai_errors.ClientError(
            code=429, response_json={"error": {"message": "Rate limit exceeded"}}
        )

        with patch("loafer.llm.gemini.genai.Client", return_value=mock_client):
            provider = GeminiProvider(api_key="test-key")
            with pytest.raises(LLMRateLimitError):
                provider.generate_transform_function({}, "noop")

        # Should have retried 4 times (stop_after_attempt(4))
        assert mock_client.models.generate_content.call_count == 4


class TestGeminiProviderEltSql:
    """generate_elt_sql tests."""

    def test_successful_sql_response(self) -> None:
        sql = "SELECT id, LOWER(name) AS name FROM users_raw"
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = _make_response(sql)

        with patch("loafer.llm.gemini.genai.Client", return_value=mock_client):
            provider = GeminiProvider(api_key="test-key")
            result = provider.generate_elt_sql(
                target_schema={}, raw_table_name="users_raw", instruction="lowercase"
            )

        assert result.sql == sql
        assert result.token_usage["total_tokens"] == 150

    def test_sql_fences_stripped(self) -> None:
        raw = "```sql\nSELECT 1 FROM t\n```"
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = _make_response(raw)

        with patch("loafer.llm.gemini.genai.Client", return_value=mock_client):
            provider = GeminiProvider(api_key="test-key")
            result = provider.generate_elt_sql({}, "t", "noop")

        assert "```" not in result.sql
        assert "SELECT 1" in result.sql
