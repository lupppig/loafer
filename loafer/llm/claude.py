"""Claude LLM provider implementation.

Uses the ``anthropic`` SDK with ``claude-sonnet-4-20250514`` for transform
generation.  The SDK includes built-in retry on 429/5xx errors.
"""

from __future__ import annotations

import re

import anthropic
from anthropic import APIStatusError

from loafer.exceptions import LLMRateLimitError
from loafer.llm.base import ELTSQLResult, LLMProvider, TransformPromptResult
from loafer.llm.prompt_builder import build_elt_sql_prompt, build_etl_transform_prompt

_FENCE_RE = re.compile(
    r"^```(?:python|sql|)\s*\n(.*?)\n```\s*$",
    re.DOTALL,
)


def _strip_markdown_fences(text: str) -> str:
    """Remove accidental markdown code fences from model output."""
    m = _FENCE_RE.match(text.strip())
    return m.group(1) if m else text.strip()


class ClaudeProvider(LLMProvider):
    """Concrete ``LLMProvider`` backed by Anthropic Claude."""

    def __init__(
        self,
        api_key: str,
        model: str = "claude-sonnet-4-20250514",
        max_tokens: int = 4096,
    ) -> None:
        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = model
        self._max_tokens = max_tokens

    def generate_transform_function(
        self,
        schema_sample: dict[str, object],
        instruction: str,
        previous_error: str | None = None,
        previous_code: str | None = None,
    ) -> TransformPromptResult:
        prompt = build_etl_transform_prompt(
            schema_sample, instruction, previous_error, previous_code
        )
        response = self._call(prompt)
        raw_text = response.content[0].text  # type: ignore[union-attr]
        code = _strip_markdown_fences(raw_text)
        return TransformPromptResult(
            code=code,
            raw_response=raw_text,
            token_usage={
                "prompt_tokens": response.usage.input_tokens,
                "completion_tokens": response.usage.output_tokens,
                "total_tokens": response.usage.input_tokens + response.usage.output_tokens,
            },
        )

    def generate_elt_sql(
        self,
        target_schema: dict[str, object],
        raw_table_name: str,
        instruction: str,
        previous_error: str | None = None,
    ) -> ELTSQLResult:
        prompt = build_elt_sql_prompt(target_schema, raw_table_name, instruction, previous_error)
        response = self._call(prompt)
        raw_text = response.content[0].text  # type: ignore[union-attr]
        sql = _strip_markdown_fences(raw_text)
        return ELTSQLResult(
            sql=sql,
            raw_response=raw_text,
            token_usage={
                "prompt_tokens": response.usage.input_tokens,
                "completion_tokens": response.usage.output_tokens,
                "total_tokens": response.usage.input_tokens + response.usage.output_tokens,
            },
        )

    def _call(self, prompt: str) -> anthropic.types.Message:
        try:
            return self._client.messages.create(
                model=self._model,
                max_tokens=self._max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
        except APIStatusError as exc:
            if exc.status_code == 429:
                raise LLMRateLimitError(str(exc)) from exc
            raise
        except Exception as exc:
            if "429" in str(exc):
                raise LLMRateLimitError(str(exc)) from exc
            raise
