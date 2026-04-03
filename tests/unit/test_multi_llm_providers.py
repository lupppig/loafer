"""Tests for Claude, OpenAI, and Qwen provider registration."""

import pytest

from loafer.llm.registry import get_provider, list_providers


class TestProviderRegistration:
    def test_list_providers_includes_all_four(self) -> None:
        providers = list_providers()
        assert "gemini" in providers
        assert "claude" in providers
        assert "openai" in providers
        assert "qwen" in providers

    def test_get_gemini_provider(self) -> None:
        provider = get_provider("gemini", api_key="test-key")
        assert provider.__class__.__name__ == "GeminiProvider"

    def test_get_claude_provider(self) -> None:
        provider = get_provider("claude", api_key="test-key")
        assert provider.__class__.__name__ == "ClaudeProvider"

    def test_get_openai_provider(self) -> None:
        provider = get_provider("openai", api_key="test-key")
        assert provider.__class__.__name__ == "OpenAIProvider"

    def test_get_qwen_provider(self) -> None:
        provider = get_provider("qwen", api_key="test-key")
        assert provider.__class__.__name__ == "QwenProvider"

    def test_unknown_provider_raises(self) -> None:
        from loafer.exceptions import LLMError

        with pytest.raises(LLMError):
            get_provider("unknown", api_key="test-key")


class TestClaudeProvider:
    def test_instantiation(self) -> None:
        from loafer.llm.claude import ClaudeProvider

        p = ClaudeProvider(api_key="test-key")
        assert p._model == "claude-sonnet-4-20250514"

    def test_custom_model(self) -> None:
        from loafer.llm.claude import ClaudeProvider

        p = ClaudeProvider(api_key="test-key", model="claude-3-5-sonnet-20241022")
        assert p._model == "claude-3-5-sonnet-20241022"

    def test_strip_markdown_fences_python(self) -> None:
        from loafer.llm.claude import _strip_markdown_fences

        result = _strip_markdown_fences("```python\ndef foo(): pass\n```")
        assert result == "def foo(): pass"

    def test_strip_markdown_fences_sql(self) -> None:
        from loafer.llm.claude import _strip_markdown_fences

        result = _strip_markdown_fences("```sql\nSELECT 1;\n```")
        assert result == "SELECT 1;"

    def test_strip_markdown_fences_bare(self) -> None:
        from loafer.llm.claude import _strip_markdown_fences

        result = _strip_markdown_fences("```\ndef foo(): pass\n```")
        assert result == "def foo(): pass"

    def test_plain_text_unchanged(self) -> None:
        from loafer.llm.claude import _strip_markdown_fences

        result = _strip_markdown_fences("def foo(): pass")
        assert result == "def foo(): pass"


class TestOpenAIProvider:
    def test_instantiation(self) -> None:
        from loafer.llm.openai import OpenAIProvider

        p = OpenAIProvider(api_key="test-key")
        assert p._model == "gpt-4o-mini"

    def test_custom_model(self) -> None:
        from loafer.llm.openai import OpenAIProvider

        p = OpenAIProvider(api_key="test-key", model="gpt-4o")
        assert p._model == "gpt-4o"

    def test_strip_markdown_fences_python(self) -> None:
        from loafer.llm.openai import _strip_markdown_fences

        result = _strip_markdown_fences("```python\ndef foo(): pass\n```")
        assert result == "def foo(): pass"

    def test_strip_markdown_fences_sql(self) -> None:
        from loafer.llm.openai import _strip_markdown_fences

        result = _strip_markdown_fences("```sql\nSELECT 1;\n```")
        assert result == "SELECT 1;"

    def test_plain_text_unchanged(self) -> None:
        from loafer.llm.openai import _strip_markdown_fences

        result = _strip_markdown_fences("def foo(): pass")
        assert result == "def foo(): pass"


class TestQwenProvider:
    def test_instantiation(self) -> None:
        from loafer.llm.qwen import QwenProvider

        p = QwenProvider(api_key="test-key")
        assert p._model == "qwen-plus"

    def test_custom_model(self) -> None:
        from loafer.llm.qwen import QwenProvider

        p = QwenProvider(api_key="test-key", model="qwen-max")
        assert p._model == "qwen-max"

    def test_strip_markdown_fences_python(self) -> None:
        from loafer.llm.qwen import _strip_markdown_fences

        result = _strip_markdown_fences("```python\ndef foo(): pass\n```")
        assert result == "def foo(): pass"

    def test_strip_markdown_fences_sql(self) -> None:
        from loafer.llm.qwen import _strip_markdown_fences

        result = _strip_markdown_fences("```sql\nSELECT 1;\n```")
        assert result == "SELECT 1;"

    def test_plain_text_unchanged(self) -> None:
        from loafer.llm.qwen import _strip_markdown_fences

        result = _strip_markdown_fences("def foo(): pass")
        assert result == "def foo(): pass"
