"""A scriptable ``LLMProvider`` for exercising Loafer's AI transforms offline.

Loafer's AI transform is an *authoring* step: the provider returns Python (ETL)
or SQL (ELT) as text, and Loafer then validates, sandboxes, executes, and
retries it. Everything after the provider call is deterministic and is where
almost all of the behaviour lives — so it can, and should, be tested without a
network round trip.

``ScriptedProvider`` stands in for Claude/Gemini/OpenAI and replays a fixed
script of responses. Each entry is either:

* ``str``          — returned as the generated code/SQL
* ``Exception``    — raised, to exercise Loafer's error handling
* ``callable``     — invoked with the recorded call, returns a ``str``

Every call is recorded in ``.calls`` so a test can assert on what Loafer fed
back into the model on retry (the self-correction loop).

Run the same scenarios against a real provider with
``ai_transform_tests.py --live``; this module is then bypassed entirely.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from loafer.ports.llm import ELTSQLResult, LLMProvider, TransformPromptResult

_Response = str | Exception | Callable[["RecordedCall"], str]


@dataclass
class RecordedCall:
    """One provider invocation, captured for assertions."""

    kind: str  # "transform" or "sql"
    instruction: str
    schema_sample: dict[str, Any]
    previous_error: str | None = None
    previous_code: str | None = None
    custom_code: str | None = None
    raw_table_name: str | None = None


class ScriptExhausted(RuntimeError):  # noqa: N818 - reads better without the suffix
    """Raised when Loafer calls the provider more times than the script allows."""


@dataclass
class ScriptedProvider(LLMProvider):
    """Replay a fixed list of provider responses, recording every call."""

    responses: list[_Response] = field(default_factory=list)
    tokens_per_call: tuple[int, int] = (1200, 180)
    calls: list[RecordedCall] = field(default_factory=list)

    def _next(self, call: RecordedCall) -> str:
        self.calls.append(call)
        index = len(self.calls) - 1
        if index >= len(self.responses):
            raise ScriptExhausted(
                f"provider called {len(self.calls)}x but script has "
                f"{len(self.responses)} response(s)"
            )
        response = self.responses[index]
        if isinstance(response, Exception):
            raise response
        if callable(response):
            return response(call)
        return response

    def _usage(self) -> dict[str, int]:
        prompt, completion = self.tokens_per_call
        return {
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "total_tokens": prompt + completion,
        }

    def generate_transform_function(
        self,
        schema_sample: dict[str, object],
        instruction: str,
        previous_error: str | None = None,
        previous_code: str | None = None,
        custom_code: str | None = None,
    ) -> TransformPromptResult:
        code = self._next(
            RecordedCall(
                kind="transform",
                instruction=instruction,
                schema_sample=dict(schema_sample),
                previous_error=previous_error,
                previous_code=previous_code,
                custom_code=custom_code,
            )
        )
        return TransformPromptResult(code=code, raw_response=code, token_usage=self._usage())

    def generate_elt_sql(
        self,
        target_schema: dict[str, object],
        raw_table_name: str,
        instruction: str,
        previous_error: str | None = None,
    ) -> ELTSQLResult:
        sql = self._next(
            RecordedCall(
                kind="sql",
                instruction=instruction,
                schema_sample=dict(target_schema),
                previous_error=previous_error,
                raw_table_name=raw_table_name,
            )
        )
        return ELTSQLResult(sql=sql, raw_response=sql, token_usage=self._usage())


# --- Canned model outputs -------------------------------------------------
# These are the shapes a real model returns for the instructions in
# examples/pipelines/*.yaml. Keeping them here makes the offline runs
# reproducible; --live replaces them with whatever the model actually writes.

NORMALIZE_ORDERS = '''
def transform(data: list[dict]) -> list[dict]:
    """Lowercase emails, drop cancelled/refunded orders, convert to USD."""
    rates = {"USD": 1.0, "GBP": 1.27, "EUR": 1.08}
    out = []
    for row in data:
        if row.get("status") in ("cancelled", "refunded"):
            continue
        new = dict(row)
        email = new.get("email") or ""
        new["email"] = email.strip().lower()
        currency = new.get("currency", "USD")
        amount = float(new.get("amount") or 0)
        new["amount_usd"] = round(amount * rates.get(currency, 1.0), 2)
        out.append(new)
    return out
'''

ADD_ORDER_VALUE_BAND = '''
def transform(data: list[dict]) -> list[dict]:
    """Classify each order into a value band."""
    out = []
    for row in data:
        new = dict(row)
        amount = float(new.get("amount") or 0)
        if amount >= 300:
            new["value_band"] = "high"
        elif amount >= 100:
            new["value_band"] = "medium"
        else:
            new["value_band"] = "low"
        out.append(new)
    return out
'''

# Rejected by the AST validator: `os` is on the blocked-import list.
UNSAFE_READS_FILESYSTEM = """
import os


def transform(data: list[dict]) -> list[dict]:
    secrets = os.environ.get("ANTHROPIC_API_KEY", "")
    for row in data:
        row["leaked"] = secrets
    return data
"""

# Passes the AST validator, raises at runtime — exercises the execute-then-retry path.
RUNTIME_ERROR = """
def transform(data: list[dict]) -> list[dict]:
    out = []
    for row in data:
        out.append({**row, "ratio": float(row["amount"]) / 0})
    return out
"""

# Wrong shape: no `transform` function at all.
MISSING_TRANSFORM_FN = """
def process(data):
    return data
"""

# Drops every column except one — trips the destructive-operation detector.
DROP_ALL_BUT_ID = """
def transform(data: list[dict]) -> list[dict]:
    return [{"order_id": row["order_id"]} for row in data]
"""

ELT_SELECT_PAID = """
SELECT order_id, customer, lower(email) AS email, country, currency, amount
FROM {table}
WHERE status = 'paid'
"""

# Rejected by the SQL validator: not a bare SELECT.
ELT_DESTRUCTIVE_DDL = "DROP TABLE {table}"
