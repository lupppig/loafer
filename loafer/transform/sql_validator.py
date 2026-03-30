"""SQL safety validator using sqlglot AST analysis.

Every SQL transform — whether user-supplied or LLM-generated — passes
through this validator before touching the database.  Only a single
``SELECT`` statement is permitted.
"""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

# Expression types that are never allowed inside a transform query.
_DISALLOWED: tuple[type[exp.Expression], ...] = (
    exp.Drop,
    exp.Delete,
    exp.Update,
    exp.Insert,
    exp.Create,
    exp.Alter,
    exp.Command,
    exp.TruncateTable,
)


def validate_transform_sql(sql: str) -> tuple[bool, str | None]:
    """Validate a SQL string for use as a transform query.

    Returns ``(True, None)`` when the SQL is an acceptable single
    ``SELECT`` statement, or ``(False, reason)`` on rejection.
    """
    try:
        statements = sqlglot.parse(sql)
    except sqlglot.errors.ParseError as exc:
        return False, f"invalid SQL syntax: {exc}"

    # Filter out empty/None statements that sqlglot may return for
    # trailing semicolons or whitespace.
    statements = [s for s in statements if s is not None]

    if len(statements) != 1:
        return False, (f"exactly one SELECT statement required, got {len(statements)}")

    statement = statements[0]

    if not isinstance(statement, exp.Select):
        kind = type(statement).__name__
        return False, f"only SELECT is allowed, got {kind}"

    # Walk the AST looking for disallowed nodes.
    for node in statement.walk():
        if isinstance(node, _DISALLOWED):
            kind = type(node).__name__
            return False, f"disallowed operation in statement: {kind}"

    return True, None
