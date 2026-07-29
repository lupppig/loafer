"""Shared validation for user-configured database object names."""

from __future__ import annotations


def split_qualified_name(value: str) -> tuple[str, str]:
    """Return ``(schema, object_name)`` for ``name`` or ``schema.name``.

    Loafer configuration uses a dot only as the schema separator. Quoted SQL
    syntax is deliberately not accepted here; adapters quote each parsed part
    with their driver's identifier API.
    """
    if not value:
        raise ValueError("database object name must not be empty")

    parts = value.split(".")
    if len(parts) == 1:
        schema, object_name = "public", parts[0]
    elif len(parts) == 2:
        schema, object_name = parts
    else:
        raise ValueError("database object name must be 'name' or 'schema.name'")

    if not schema or not object_name:
        raise ValueError("database schema and object name must not be empty")

    return schema, object_name
