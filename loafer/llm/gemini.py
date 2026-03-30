"""Gemini LLM provider implementation.

Uses ``google-generativeai`` with ``gemini-1.5-flash`` (fast and cheap,
appropriate for transform generation).  Rate-limit errors trigger
exponential backoff via *tenacity*.
"""

from __future__ import annotations

import re

import google.generativeai as genai
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

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


def _extract_token_usage(response: genai.types.GenerateContentResponse) -> dict[str, int]:
    """Pull token counts from the Gemini response metadata."""
    usage: dict[str, int] = {}
    meta = getattr(response, "usage_metadata", None)
    if meta:
        usage["prompt_tokens"] = getattr(meta, "prompt_token_count", 0)
        usage["completion_tokens"] = getattr(meta, "candidates_token_count", 0)
        usage["total_tokens"] = getattr(meta, "total_token_count", 0)
    return usage


class GeminiProvider(LLMProvider):
    """Concrete ``LLMProvider`` backed by Google Gemini."""

    def __init__(self, api_key: str, model: str = "gemini-1.5-flash") -> None:
        genai.configure(api_key=api_key)  # type: ignore[attr-defined]
        self._model = genai.GenerativeModel(model)  # type: ignore[attr-defined]

    # -- ETL transform -------------------------------------------------------

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
        response = self._call_with_retry(prompt)
        raw_text = self._response_text(response)
        code = _strip_markdown_fences(raw_text)
        return TransformPromptResult(
            code=code,
            raw_response=raw_text,
            token_usage=_extract_token_usage(response),
        )

    # -- ELT SQL -------------------------------------------------------------

    def generate_elt_sql(
        self,
        target_schema: dict[str, object],
        raw_table_name: str,
        instruction: str,
        previous_error: str | None = None,
    ) -> ELTSQLResult:
        prompt = build_elt_sql_prompt(target_schema, raw_table_name, instruction, previous_error)
        response = self._call_with_retry(prompt)
        raw_text = self._response_text(response)
        sql = _strip_markdown_fences(raw_text)
        return ELTSQLResult(
            sql=sql,
            raw_response=raw_text,
            token_usage=_extract_token_usage(response),
        )

    # -- internals -----------------------------------------------------------

    @retry(
        retry=retry_if_exception_type(LLMRateLimitError),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def _call_with_retry(self, prompt: str) -> genai.types.GenerateContentResponse:
        """Call Gemini with exponential backoff on rate-limit errors."""
        try:
            return self._model.generate_content(prompt)
        except Exception as exc:
            # google-generativeai raises google.api_core.exceptions.ResourceExhausted
            # (or similar 429-family errors) when rate-limited.
            exc_name = type(exc).__name__
            if "429" in str(exc) or "ResourceExhausted" in exc_name:
                raise LLMRateLimitError(str(exc)) from exc
            raise

    @staticmethod
    def _response_text(response: genai.types.GenerateContentResponse) -> str:
        """Extract text from Gemini response, raising on empty output."""
        try:
            text = response.text
        except (ValueError, AttributeError):
            text = None

        if not text or not text.strip():
            raise LLMInvalidOutputError(
                f"Gemini returned empty or unparseable output: {response!r}"
            )
        return str(text.strip())
