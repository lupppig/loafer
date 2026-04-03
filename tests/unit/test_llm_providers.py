"""Tests for Claude, OpenAI, and Qwen LLM providers with mocked SDKs."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from loafer.exceptions import LLMRateLimitError


class TestClaudeProvider:
    def _make_response(
        self,
        text: str = "def transform(data): return data",
        prompt_tokens: int = 100,
        completion_tokens: int = 50,
    ) -> MagicMock:
        resp = MagicMock()
        resp.content = [MagicMock(text=text)]
        resp.usage = MagicMock(input_tokens=prompt_tokens, output_tokens=completion_tokens)
        return resp

    def test_generate_transform_function(self) -> None:
        from loafer.llm.claude import ClaudeProvider

        mock_client = MagicMock()
        mock_client.messages.create.return_value = self._make_response()

        with patch("loafer.llm.claude.anthropic.Anthropic", return_value=mock_client):
            provider = ClaudeProvider(api_key="test-key")
            result = provider.generate_transform_function(
                schema_sample={"id": {"inferred_type": "integer", "sample_values": [1]}},
                instruction="double the id",
            )

        assert "def transform" in result.code
        assert result.token_usage["total_tokens"] == 150

    def test_generate_transform_function_with_previous_error(self) -> None:
        from loafer.llm.claude import ClaudeProvider

        mock_client = MagicMock()
        mock_client.messages.create.return_value = self._make_response()

        with patch("loafer.llm.claude.anthropic.Anthropic", return_value=mock_client):
            provider = ClaudeProvider(api_key="test-key")
            provider.generate_transform_function(
                schema_sample={},
                instruction="noop",
                previous_error="previous attempt failed",
                previous_code="def transform(data): pass",
            )

        mock_client.messages.create.assert_called_once()
        call_kwargs = mock_client.messages.create.call_args[1]
        assert "previous attempt failed" in call_kwargs["messages"][0]["content"]

    def test_generate_elt_sql(self) -> None:
        from loafer.llm.claude import ClaudeProvider

        mock_client = MagicMock()
        mock_client.messages.create.return_value = self._make_response(text="SELECT id FROM raw")

        with patch("loafer.llm.claude.anthropic.Anthropic", return_value=mock_client):
            provider = ClaudeProvider(api_key="test-key")
            result = provider.generate_elt_sql(
                target_schema={},
                raw_table_name="raw_data",
                instruction="select all",
            )

        assert result.sql == "SELECT id FROM raw"

    def test_generate_elt_sql_with_previous_error(self) -> None:
        from loafer.llm.claude import ClaudeProvider

        mock_client = MagicMock()
        mock_client.messages.create.return_value = self._make_response(text="SELECT 1")

        with patch("loafer.llm.claude.anthropic.Anthropic", return_value=mock_client):
            provider = ClaudeProvider(api_key="test-key")
            provider.generate_elt_sql(
                target_schema={},
                raw_table_name="raw",
                instruction="select",
                previous_error="column not found",
            )

        call_kwargs = mock_client.messages.create.call_args[1]
        assert "column not found" in call_kwargs["messages"][0]["content"]

    def test_strip_markdown_fences_python(self) -> None:
        from loafer.llm.claude import _strip_markdown_fences

        assert _strip_markdown_fences("```python\ndef foo(): pass\n```") == "def foo(): pass"

    def test_strip_markdown_fences_sql(self) -> None:
        from loafer.llm.claude import _strip_markdown_fences

        assert _strip_markdown_fences("```sql\nSELECT 1;\n```") == "SELECT 1;"

    def test_strip_markdown_fences_bare(self) -> None:
        from loafer.llm.claude import _strip_markdown_fences

        assert _strip_markdown_fences("```\ncode\n```") == "code"

    def test_strip_markdown_fences_plain_text(self) -> None:
        from loafer.llm.claude import _strip_markdown_fences

        assert _strip_markdown_fences("def foo(): pass") == "def foo(): pass"

    def test_rate_limit_error_429(self) -> None:
        from anthropic import APIStatusError
        from loafer.llm.claude import ClaudeProvider

        mock_client = MagicMock()
        mock_client.messages.create.side_effect = APIStatusError(
            message="rate limit", response=MagicMock(status_code=429), body={}
        )

        with patch("loafer.llm.claude.anthropic.Anthropic", return_value=mock_client):
            provider = ClaudeProvider(api_key="test-key")

            with pytest.raises(LLMRateLimitError):
                provider.generate_transform_function(schema_sample={}, instruction="x")

    def test_non_rate_limit_error_propagates(self) -> None:
        from anthropic import APIStatusError
        from loafer.llm.claude import ClaudeProvider

        mock_client = MagicMock()
        mock_client.messages.create.side_effect = APIStatusError(
            message="bad request", response=MagicMock(status_code=400), body={}
        )

        with patch("loafer.llm.claude.anthropic.Anthropic", return_value=mock_client):
            provider = ClaudeProvider(api_key="test-key")

            with pytest.raises(APIStatusError):
                provider.generate_transform_function(schema_sample={}, instruction="x")

    def test_custom_model_and_max_tokens(self) -> None:
        from loafer.llm.claude import ClaudeProvider

        mock_client = MagicMock()
        mock_client.messages.create.return_value = self._make_response()

        with patch("loafer.llm.claude.anthropic.Anthropic", return_value=mock_client):
            provider = ClaudeProvider(
                api_key="test-key", model="claude-3-5-sonnet-20241022", max_tokens=2048
            )
            provider.generate_transform_function(schema_sample={}, instruction="x")

        call_kwargs = mock_client.messages.create.call_args[1]
        assert call_kwargs["model"] == "claude-3-5-sonnet-20241022"
        assert call_kwargs["max_tokens"] == 2048


class TestOpenAIProvider:
    def _make_response(
        self,
        text: str = "def transform(data): return data",
        prompt_tokens: int = 100,
        completion_tokens: int = 50,
    ) -> MagicMock:
        resp = MagicMock()
        resp.choices = [MagicMock(message=MagicMock(content=text))]
        resp.usage = MagicMock(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
        )
        return resp

    def test_generate_transform_function(self) -> None:
        from loafer.llm.openai import OpenAIProvider

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_response()

        with patch("loafer.llm.openai.openai.OpenAI", return_value=mock_client):
            provider = OpenAIProvider(api_key="test-key")
            result = provider.generate_transform_function(
                schema_sample={"id": {"inferred_type": "integer", "sample_values": [1]}},
                instruction="double the id",
            )

        assert "def transform" in result.code
        assert result.token_usage["total_tokens"] == 150

    def test_generate_transform_function_with_previous_error(self) -> None:
        from loafer.llm.openai import OpenAIProvider

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_response()

        with patch("loafer.llm.openai.openai.OpenAI", return_value=mock_client):
            provider = OpenAIProvider(api_key="test-key")
            provider.generate_transform_function(
                schema_sample={},
                instruction="noop",
                previous_error="previous attempt failed",
                previous_code="def transform(data): pass",
            )

        mock_client.chat.completions.create.assert_called_once()
        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert "previous attempt failed" in call_kwargs["messages"][0]["content"]

    def test_generate_elt_sql(self) -> None:
        from loafer.llm.openai import OpenAIProvider

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_response(
            text="SELECT id FROM raw"
        )

        with patch("loafer.llm.openai.openai.OpenAI", return_value=mock_client):
            provider = OpenAIProvider(api_key="test-key")
            result = provider.generate_elt_sql(
                target_schema={},
                raw_table_name="raw_data",
                instruction="select all",
            )

        assert result.sql == "SELECT id FROM raw"

    def test_generate_elt_sql_with_previous_error(self) -> None:
        from loafer.llm.openai import OpenAIProvider

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_response(text="SELECT 1")

        with patch("loafer.llm.openai.openai.OpenAI", return_value=mock_client):
            provider = OpenAIProvider(api_key="test-key")
            provider.generate_elt_sql(
                target_schema={},
                raw_table_name="raw",
                instruction="select",
                previous_error="column not found",
            )

        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert "column not found" in call_kwargs["messages"][0]["content"]

    def test_strip_markdown_fences_python(self) -> None:
        from loafer.llm.openai import _strip_markdown_fences

        assert _strip_markdown_fences("```python\ndef foo(): pass\n```") == "def foo(): pass"

    def test_strip_markdown_fences_sql(self) -> None:
        from loafer.llm.openai import _strip_markdown_fences

        assert _strip_markdown_fences("```sql\nSELECT 1;\n```") == "SELECT 1;"

    def test_strip_markdown_fences_plain_text(self) -> None:
        from loafer.llm.openai import _strip_markdown_fences

        assert _strip_markdown_fences("def foo(): pass") == "def foo(): pass"

    def test_rate_limit_error_429(self) -> None:
        from openai import APIStatusError
        from loafer.llm.openai import OpenAIProvider

        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = APIStatusError(
            message="rate limit", response=MagicMock(status_code=429), body={}
        )

        with patch("loafer.llm.openai.openai.OpenAI", return_value=mock_client):
            provider = OpenAIProvider(api_key="test-key")

            with pytest.raises(LLMRateLimitError):
                provider.generate_transform_function(schema_sample={}, instruction="x")

    def test_non_rate_limit_error_propagates(self) -> None:
        from openai import APIStatusError
        from loafer.llm.openai import OpenAIProvider

        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = APIStatusError(
            message="bad request", response=MagicMock(status_code=400), body={}
        )

        with patch("loafer.llm.openai.openai.OpenAI", return_value=mock_client):
            provider = OpenAIProvider(api_key="test-key")

            with pytest.raises(APIStatusError):
                provider.generate_transform_function(schema_sample={}, instruction="x")

    def test_custom_model_and_max_tokens(self) -> None:
        from loafer.llm.openai import OpenAIProvider

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_response()

        with patch("loafer.llm.openai.openai.OpenAI", return_value=mock_client):
            provider = OpenAIProvider(api_key="test-key", model="gpt-4o", max_tokens=2048)
            provider.generate_transform_function(schema_sample={}, instruction="x")

        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert call_kwargs["model"] == "gpt-4o"
        assert call_kwargs["max_tokens"] == 2048


class TestQwenProvider:
    def _make_response(
        self,
        text: str = "def transform(data): return data",
        prompt_tokens: int = 100,
        completion_tokens: int = 50,
    ) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        resp.get.side_effect = lambda key, default=None: {
            "output": {"choices": [{"message": {"content": text}}]},
            "usage": {"input_tokens": prompt_tokens, "output_tokens": completion_tokens},
        }.get(key, default)
        return resp

    def test_generate_transform_function(self) -> None:
        from loafer.llm.qwen import QwenProvider

        mock_response = self._make_response()

        with patch("loafer.llm.qwen.dashscope.Generation.call", return_value=mock_response):
            provider = QwenProvider(api_key="test-key")
            result = provider.generate_transform_function(
                schema_sample={"id": {"inferred_type": "integer", "sample_values": [1]}},
                instruction="double the id",
            )

        assert "def transform" in result.code
        assert result.token_usage["total_tokens"] == 150
