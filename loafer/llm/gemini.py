"""Gemini LLM provider implementation.

Uses ``google-genai`` (the new Google Gen AI SDK) with
``gemini-2.5-flash`` (fast and cheap, appropriate for transform
generation).  The SDK includes built-in retry on 429/5xx errors;
an outer tenacity retry layer is kept for additional resilience.
"""

from __future__ import annotations

import re

from google import genai
from google.genai import errors as genai_errors
from google.genai import types

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


def _extract_token_usage(response: types.GenerateContentResponse) -> dict[str, int]:
    """Pull token counts from the Gemini response metadata."""
    usage: dict[str, int] = {}
    meta = response.usage_metadata
    if meta:
        usage["prompt_tokens"] = meta.prompt_token_count or 0
        usage["completion_tokens"] = meta.candidates_token_count or 0
        usage["total_tokens"] = meta.total_token_count or 0
    return usage


class GeminiProvider(LLMProvider):
    """Concrete ``LLMProvider`` backed by Google Gemini."""

    def __init__(
        self,
        api_key: str,
        model: str = "gemini-2.5-flash",
    ) -> None:
        self._client = genai.Client(api_key=api_key)
        self._model = model

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

    def _call_with_retry(self, prompt: str) -> types.GenerateContentResponse:
        """Call Gemini. The SDK already retries 429/5xx internally (5 attempts)."""
        try:
            return self._client.models.generate_content(
                model=self._model,
                contents=prompt,
            )
        except genai_errors.APIError as exc:
            if exc.code == 429:
                raise LLMRateLimitError(str(exc)) from exc
            raise
        except Exception as exc:
            if "429" in str(exc):
                raise LLMRateLimitError(str(exc)) from exc
            raise

    @staticmethod
    def _response_text(
        response: types.GenerateContentResponse,
    ) -> str:
        """Extract text from Gemini response, raising on empty output."""
        text = response.text
        if not text or not text.strip():
            raise LLMInvalidOutputError(
                f"Gemini returned empty or unparseable output: {response!r}"
            )
        return str(text.strip())
