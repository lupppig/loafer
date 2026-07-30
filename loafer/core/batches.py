"""Pure bounded-batch schema, validation, and reconciliation policies."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

from loafer.config import ValidationConfig
from loafer.exceptions import ValidationError

_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?"
    r"(?:Z|[+-]\d{2}:?\d{2})?)?$"
)


def value_type(value: Any) -> str:
    """Return the stable schema type used by batch contracts."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if isinstance(value, str) and _DATETIME_RE.match(value):
        return "datetime"
    return "string"


def infer_schema(rows: list[dict[str, Any]]) -> dict[str, str]:
    """Infer a deterministic column/type mapping for one bounded batch."""
    observed: dict[str, set[str]] = {}
    for row in rows:
        for column, value in row.items():
            observed.setdefault(column, set()).add(value_type(value))

    schema: dict[str, str] = {}
    for column in sorted(observed):
        types = observed[column] - {"null"}
        if not types:
            schema[column] = "null"
        elif len(types) == 1:
            schema[column] = next(iter(types))
        else:
            schema[column] = "mixed"
    return schema


def schema_version(schema: dict[str, str]) -> str:
    """Return a content-addressed version for a schema mapping."""
    payload = json.dumps(schema, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def canonical_row_bytes(row: dict[str, Any]) -> bytes:
    """Serialize a row deterministically for counts, bytes, and checksums."""
    return (
        json.dumps(
            row,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        ).encode("utf-8")
        + b"\n"
    )


@dataclass
class RollingRowsDigest:
    """Chunk-boundary-independent checksum and byte accumulator."""

    _digest: Any = field(default_factory=hashlib.sha256)
    rows: int = 0
    bytes: int = 0

    def update(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            payload = canonical_row_bytes(row)
            self._digest.update(payload)
            self.rows += 1
            self.bytes += len(payload)

    @property
    def checksum(self) -> str:
        return f"sha256:{self._digest.hexdigest()}"


@dataclass(frozen=True)
class RejectedRow:
    """One rejected source row and its machine-readable reason."""

    row: dict[str, Any]
    stage: str
    reason: str


@dataclass(frozen=True)
class SchemaBatchResult:
    rows: list[dict[str, Any]]
    rejected: list[RejectedRow]
    schema: dict[str, str]
    version: str
    evolved: bool = False


class SchemaTracker:
    """Apply one explicit schema-drift policy without retaining prior rows."""

    def __init__(self) -> None:
        self._schema: dict[str, str] | None = None

    @property
    def schema(self) -> dict[str, str]:
        return dict(self._schema or {})

    @property
    def version(self) -> str | None:
        return schema_version(self._schema) if self._schema is not None else None

    def apply(
        self,
        rows: list[dict[str, Any]],
        policy: str,
    ) -> SchemaBatchResult:
        if self._schema is None:
            self._schema = infer_schema(rows)
            return SchemaBatchResult(
                rows=list(rows),
                rejected=[],
                schema=self.schema,
                version=schema_version(self._schema),
            )

        accepted: list[dict[str, Any]] = []
        rejected: list[RejectedRow] = []
        evolved = False

        if policy == "evolve":
            incoming = infer_schema(rows)
            merged = _merge_schemas(self._schema, incoming)
            evolved = merged != self._schema
            self._schema = merged
            accepted = list(rows)
        else:
            for row in rows:
                differences = _schema_differences(row, self._schema)
                if not differences:
                    accepted.append(row)
                    continue

                reason = "; ".join(differences)
                if policy == "fail":
                    raise ValidationError(f"schema drift detected: {reason}")
                if policy == "quarantine":
                    rejected.append(RejectedRow(row=row, stage="schema", reason=reason))
                    continue
                if policy == "coerce":
                    try:
                        accepted.append(_coerce_row(row, self._schema))
                    except (TypeError, ValueError) as exc:
                        rejected.append(
                            RejectedRow(
                                row=row,
                                stage="schema",
                                reason=f"schema coercion failed: {exc}",
                            )
                        )
                    continue
                raise ValidationError(f"unknown schema drift policy: {policy}")

        return SchemaBatchResult(
            rows=accepted,
            rejected=rejected,
            schema=self.schema,
            version=schema_version(self._schema),
            evolved=evolved,
        )


@dataclass(frozen=True)
class ValidationBatchResult:
    rows: list[dict[str, Any]]
    rejected: list[RejectedRow]
    warnings: list[str]
    column_counts: dict[str, dict[str, int]]


def validate_batch(
    rows: list[dict[str, Any]],
    config: ValidationConfig,
) -> ValidationBatchResult:
    """Validate every row in a batch and apply fail/quarantine semantics."""
    rejected_by_index: dict[int, list[str]] = {}
    columns = sorted({column for row in rows for column in row})
    column_counts: dict[str, dict[str, int]] = {
        column: {"total_count": len(rows), "null_count": 0} for column in columns
    }

    for index, row in enumerate(rows):
        reasons: list[str] = []
        for column in config.required_columns:
            if column not in row or row[column] is None:
                reasons.append(f"required column '{column}' is missing or null")
        for column, expected in config.column_types.items():
            if column not in row or row[column] is None:
                continue
            actual = value_type(row[column])
            if not _type_compatible(actual, expected):
                reasons.append(f"column '{column}' expected {expected}, got {actual}")
        if reasons:
            rejected_by_index[index] = reasons

        for column in columns:
            if column not in row or row[column] is None:
                column_counts[column]["null_count"] += 1

    warnings: list[str] = []
    if rows:
        for column, counts in column_counts.items():
            null_rate = counts["null_count"] / counts["total_count"]
            if null_rate <= config.max_null_rate:
                continue
            message = (
                f"Column '{column}' null rate {null_rate:.2%} exceeds "
                f"max_null_rate {config.max_null_rate:.2%}"
            )
            if config.strict:
                for index, row in enumerate(rows):
                    if column not in row or row[column] is None:
                        rejected_by_index.setdefault(index, []).append(message)
            else:
                warnings.append(message)

    if rejected_by_index and config.on_failure == "fail":
        first_index = min(rejected_by_index)
        raise ValidationError(
            f"batch validation failed at row {first_index}: "
            f"{'; '.join(rejected_by_index[first_index])}"
        )

    accepted = [row for index, row in enumerate(rows) if index not in rejected_by_index]
    rejected = [
        RejectedRow(
            row=rows[index],
            stage="validation",
            reason="; ".join(reasons),
        )
        for index, reasons in sorted(rejected_by_index.items())
    ]
    return ValidationBatchResult(
        rows=accepted,
        rejected=rejected,
        warnings=warnings,
        column_counts=column_counts,
    )


def _type_compatible(actual: str, expected: str) -> bool:
    if actual == expected:
        return True
    return expected == "float" and actual == "integer"


def _schema_differences(row: dict[str, Any], schema: dict[str, str]) -> list[str]:
    differences: list[str] = []
    missing = sorted(set(schema) - set(row))
    extra = sorted(set(row) - set(schema))
    if missing:
        differences.append(f"missing columns: {', '.join(missing)}")
    if extra:
        differences.append(f"new columns: {', '.join(extra)}")

    for column in sorted(set(row) & set(schema)):
        value = row[column]
        if value is None or schema[column] in {"mixed", "null"}:
            continue
        actual = value_type(value)
        if not _type_compatible(actual, schema[column]):
            differences.append(f"column '{column}' changed from {schema[column]} to {actual}")
    return differences


def _merge_schemas(
    current: dict[str, str],
    incoming: dict[str, str],
) -> dict[str, str]:
    merged = dict(current)
    for column, incoming_type in incoming.items():
        current_type = merged.get(column)
        if current_type is None or current_type == "null":
            merged[column] = incoming_type
        elif incoming_type not in {current_type, "null"}:
            merged[column] = "mixed"
    return dict(sorted(merged.items()))


def _coerce_row(row: dict[str, Any], schema: dict[str, str]) -> dict[str, Any]:
    return {column: _coerce_value(row.get(column), expected) for column, expected in schema.items()}


def _coerce_value(value: Any, expected: str) -> Any:
    if value is None or expected in {"mixed", "null"}:
        return value
    if _type_compatible(value_type(value), expected):
        return float(value) if expected == "float" else value
    if expected == "string":
        return str(value)
    if expected == "integer":
        return int(value)
    if expected == "float":
        return float(value)
    if expected == "boolean":
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes"}:
                return True
            if normalized in {"false", "0", "no"}:
                return False
        if isinstance(value, (int, float)):
            return bool(value)
    raise ValueError(f"cannot coerce {value!r} to {expected}")
