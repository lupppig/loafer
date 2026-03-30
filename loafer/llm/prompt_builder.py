"""Prompt construction for LLM-powered transforms.

Each function assembles a deterministic, reproducible prompt string that
is passed to an ``LLMProvider``.  Prompts are designed to minimise token
count while maximising output reliability.
"""

from __future__ import annotations

import json


def build_etl_transform_prompt(
    schema_sample: dict[str, object],
    instruction: str,
    previous_error: str | None = None,
    previous_code: str | None = None,
) -> str:
    """Build the ETL transform prompt.

    If *previous_error* is present the prompt includes the error and the
    previous code so the LLM can self-correct.

    The prompt instructs the model to return **only** a Python function
    named ``transform`` with signature::

        def transform(data: list[dict]) -> list[dict]

    No markdown fences.  No explanation.  No imports beyond stdlib.
    """
    schema_json = json.dumps(schema_sample, indent=2, default=str)

    parts: list[str] = [
        "You are a data transformation expert.",
        "",
        "## Schema",
        "The source data has the following schema (column name → metadata):",
        "",
        f"```json\n{schema_json}\n```",
        "",
        "## Task",
        "Write a Python function that performs the following transformation:",
        f'"{instruction}"',
        "",
        "## Rules",
        "1. The function MUST be named `transform`.",
        "2. Signature: `def transform(data: list[dict]) -> list[dict]`.",
        "3. It receives a list of row dicts and must return a list of row dicts.",
        "4. Only use Python standard library imports (re, json, datetime, math, decimal, uuid, itertools).",
        "5. Return ONLY the function code. No markdown fences, no explanation, no examples.",
        "6. You may define helper functions above `transform`.",
    ]

    if previous_error and previous_code:
        parts.extend(
            [
                "",
                "## Previous Attempt (failed)",
                "The following code was generated previously but raised an error.",
                "",
                "### Code",
                f"```python\n{previous_code}\n```",
                "",
                "### Error",
                f"```\n{previous_error}\n```",
                "",
                "Fix the error and return a corrected version of the function.",
            ]
        )

    return "\n".join(parts)


def build_elt_sql_prompt(
    target_schema: dict[str, object],
    raw_table_name: str,
    instruction: str,
    previous_error: str | None = None,
) -> str:
    """Build the ELT in-target SQL prompt.

    Instructs the LLM to return a single SQL ``SELECT`` statement.  The
    result will be used to create a transformed table via
    ``CREATE TABLE … AS SELECT …``.
    """
    schema_json = json.dumps(target_schema, indent=2, default=str)

    parts: list[str] = [
        "You are a SQL transformation expert.",
        "",
        "## Target Table Schema",
        f"The raw data is loaded in table `{raw_table_name}` with this schema:",
        "",
        f"```json\n{schema_json}\n```",
        "",
        "## Task",
        "Write a SQL SELECT statement to transform the data as follows:",
        f'"{instruction}"',
        "",
        "## Rules",
        f"1. SELECT from `{raw_table_name}`.",
        "2. Return ONLY a single SQL SELECT statement.",
        "3. No DDL (CREATE, ALTER, DROP). No DML (INSERT, UPDATE, DELETE).",
        "4. No markdown fences, no explanation.",
        "5. The result will be wrapped in CREATE TABLE AS SELECT automatically.",
    ]

    if previous_error:
        parts.extend(
            [
                "",
                "## Previous Attempt (failed)",
                "The previously generated SQL raised an error:",
                f"```\n{previous_error}\n```",
                "",
                "Fix the error and return a corrected SELECT statement.",
            ]
        )

    return "\n".join(parts)
