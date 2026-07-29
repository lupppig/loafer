"""Current provider model defaults used throughout Loafer."""

from __future__ import annotations

from typing import Final

DEFAULT_GEMINI_MODEL: Final = "gemini-3.6-flash"
DEFAULT_CLAUDE_MODEL: Final = "claude-sonnet-5"
DEFAULT_OPENAI_MODEL: Final = "gpt-5.6-terra"
DEFAULT_QWEN_MODEL: Final = "qwen3.7-plus"

DEFAULT_MODELS: Final[dict[str, str]] = {
    "gemini": DEFAULT_GEMINI_MODEL,
    "claude": DEFAULT_CLAUDE_MODEL,
    "openai": DEFAULT_OPENAI_MODEL,
    "qwen": DEFAULT_QWEN_MODEL,
}

_MODEL_PREFIXES: Final[tuple[tuple[str, str], ...]] = (
    ("gemini", "gemini"),
    ("claude", "claude"),
    ("gpt", "openai"),
    ("qwen", "qwen"),
)


def default_model_for(provider: str) -> str:
    """Return Loafer's production default model for *provider*."""
    try:
        return DEFAULT_MODELS[provider]
    except KeyError as exc:
        supported = ", ".join(sorted(DEFAULT_MODELS))
        raise ValueError(
            f"unknown LLM provider {provider!r}; expected one of: {supported}"
        ) from exc


def provider_for_model(model: str) -> str | None:
    """Infer a supported provider from a provider-native model ID."""
    lowered = model.lower()
    for prefix, provider in _MODEL_PREFIXES:
        if lowered.startswith(prefix):
            return provider
    return None
