"""Qwen LLM provider implementation.

Uses the ``dashscope`` SDK (Alibaba Cloud) with ``qwen-plus`` for transform
generation.  The SDK supports both API and local model inference.
"""

from __future__ import annotations

import re

import dashscope
from dashscope.api_entities.dashscope_response import GenerationResponse

from loafer.exceptions import LLMInvalidOutputError, LLMRateLimitError
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


def _extract_token_usage(response: GenerationResponse) -> dict[str, int]:
    """Pull token counts from the Qwen response metadata."""
    usage = response.get("usage", {})
    return {
        "prompt_tokens": usage.get("input_tokens", 0),
        "completion_tokens": usage.get("output_tokens", 0),
        "total_tokens": usage.get("input_tokens", 0) + usage.get("output_tokens", 0),
    }


def _extract_text(response: GenerationResponse) -> str:
    """Extract text content from Qwen response."""
    output = response.get("output", {})
    choices = output.get("choices", [])
    if not choices:
        raise LLMInvalidOutputError(f"Qwen returned empty or unparseable output: {response}")
    text = choices[0].get("message", {}).get("content", "")
    if not text or not text.strip():
        raise LLMInvalidOutputError(f"Qwen returned empty or unparseable output: {response}")
    return text


class QwenProvider(LLMProvider):
    """Concrete ``LLMProvider`` backed by Alibaba Qwen."""

    def __init__(
        self,
        api_key: str,
        model: str = "qwen-plus",
    ) -> None:
        dashscope.api_key = api_key
        self._model = model

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
        raw_text = _extract_text(response)
        code = _strip_markdown_fences(raw_text)
        return TransformPromptResult(
            code=code,
            raw_response=raw_text,
            token_usage=_extract_token_usage(response),
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
        raw_text = _extract_text(response)
        sql = _strip_markdown_fences(raw_text)
        return ELTSQLResult(
            sql=sql,
            raw_response=raw_text,
            token_usage=_extract_token_usage(response),
        )

    def _call(self, prompt: str) -> GenerationResponse:
        from http import HTTPStatus

        response = dashscope.Generation.call(
            model=self._model,
            messages=[{"role": "user", "content": prompt}],
            result_format="message",
        )

        if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            raise LLMRateLimitError(str(response))

        if response.status_code != HTTPStatus.OK:
            raise LLMInvalidOutputError(
                f"Qwen API error (status={response.status_code}): {response}"
            )

        return response
