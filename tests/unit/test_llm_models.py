"""Tests for provider-specific model defaults and user-facing guidance."""

from __future__ import annotations

import pytest

from loafer.cli import _parse_model_not_found
from loafer.llm.models import default_model_for, provider_for_model
from loafer.transform.ai_runner import _human_readable_llm_error


@pytest.mark.parametrize(
    ("model", "provider", "recommended"),
    [
        ("gemini-retired", "gemini", "gemini-3.6-flash"),
        ("claude-retired", "claude", "claude-sonnet-5"),
        ("gpt-retired", "openai", "gpt-5.6-terra"),
        ("qwen-retired", "qwen", "qwen3.7-plus"),
    ],
)
def test_model_not_found_guidance_uses_current_provider_default(
    model: str,
    provider: str,
    recommended: str,
) -> None:
    cli_message = _parse_model_not_found(f"404 model {model} was not found")
    runner_message = _human_readable_llm_error(RuntimeError(f"404 model {model} was not found"))

    assert provider_for_model(model) == provider
    assert default_model_for(provider) == recommended
    assert recommended in cli_message
    assert recommended in runner_message


def test_unknown_model_prefix_uses_generic_guidance() -> None:
    message = _parse_model_not_found("404 model custom-retired was not found")

    assert provider_for_model("custom-retired") is None
    assert "provider's docs" in message
