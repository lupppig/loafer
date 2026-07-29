"""Test helper for rendering psycopg2 composable SQL without a live database."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch


def render_sql(query: Any) -> str:
    """Render a psycopg2 composable using PostgreSQL identifier escaping rules."""
    if isinstance(query, str):
        return query

    def quote_identifier(value: str, _context: object) -> str:
        return f'"{value.replace(chr(34), chr(34) * 2)}"'

    with patch("psycopg2.extensions.quote_ident", side_effect=quote_identifier):
        return str(query.as_string(None))
