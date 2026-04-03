"""LLM provider registry.

Centralised lookup for LLM backends.  Providers are registered by name
and resolved at runtime from config.  Adding a new provider requires
only implementing ``LLMProvider`` and calling ``register_provider``.
"""

from __future__ import annotations

from collections.abc import Callable

from loafer.exceptions import LLMError
from loafer.llm.base import LLMProvider

_ProviderFactory = Callable[..., LLMProvider]

_registry: dict[str, _ProviderFactory] = {}


def register_provider(name: str, factory: _ProviderFactory) -> None:
    """Register an LLM provider factory under the given name.

    Args:
        name: Canonical provider identifier (e.g. ``"gemini"``).
        factory: Callable that returns an ``LLMProvider`` instance.
    """
    _registry[name] = factory


def get_provider(name: str, **kwargs: object) -> LLMProvider:
    """Resolve and instantiate a registered LLM provider.

    Args:
        name: Provider name as registered via ``register_provider``.
        **kwargs: Arguments forwarded to the provider factory.

    Returns:
        An ``LLMProvider`` instance.

    Raises:
        LLMError: If the provider name is not registered.
    """
    if name not in _registry:
        available = ", ".join(sorted(_registry))
        raise LLMError(f"Unknown LLM provider: {name!r}. Available: {available}")
    return _registry[name](**kwargs)


def list_providers() -> list[str]:
    """Return all registered provider names."""
    return sorted(_registry)


def _register_gemini() -> None:
    from loafer.llm.gemini import GeminiProvider

    def _factory(**kwargs: object) -> GeminiProvider:
        api_key = kwargs.get("api_key") or kwargs.get("api_key", "")
        model = kwargs.get("model", "gemini-2.5-flash")
        return GeminiProvider(api_key=str(api_key), model=str(model))

    register_provider("gemini", _factory)


def _register_claude() -> None:
    from loafer.llm.claude import ClaudeProvider

    def _factory(**kwargs: object) -> ClaudeProvider:
        api_key = kwargs.get("api_key", "")
        model = kwargs.get("model", "claude-sonnet-4-20250514")
        max_tokens = kwargs.get("max_tokens", 4096)
        return ClaudeProvider(api_key=str(api_key), model=str(model), max_tokens=int(max_tokens))

    register_provider("claude", _factory)


def _register_openai() -> None:
    from loafer.llm.openai import OpenAIProvider

    def _factory(**kwargs: object) -> OpenAIProvider:
        api_key = kwargs.get("api_key", "")
        model = kwargs.get("model", "gpt-4o-mini")
        max_tokens = kwargs.get("max_tokens", 4096)
        return OpenAIProvider(api_key=str(api_key), model=str(model), max_tokens=int(max_tokens))

    register_provider("openai", _factory)


def _register_qwen() -> None:
    from loafer.llm.qwen import QwenProvider

    def _factory(**kwargs: object) -> QwenProvider:
        api_key = kwargs.get("api_key", "")
        model = kwargs.get("model", "qwen-plus")
        return QwenProvider(api_key=str(api_key), model=str(model))

    register_provider("qwen", _factory)


_register_gemini()
_register_claude()
_register_openai()
_register_qwen()
