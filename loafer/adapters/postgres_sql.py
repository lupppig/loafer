"""Safe PostgreSQL identifier composition helpers."""

from __future__ import annotations

from psycopg2 import sql

from loafer.core.identifiers import split_qualified_name


def qualified_identifier(value: str) -> sql.Identifier:
    """Compose ``name`` or ``schema.name`` as a quoted PostgreSQL identifier."""
    schema, object_name = split_qualified_name(value)
    return sql.Identifier(schema, object_name)


def column_list(columns: list[str]) -> sql.Composed:
    """Compose a comma-separated list of quoted column identifiers."""
    return sql.SQL(", ").join(sql.Identifier(column) for column in columns)
