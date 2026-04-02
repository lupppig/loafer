import pytest

from loafer.core.destructive import (
    DestructiveReason,
    DestructiveWarning,
    _analyze_sql_destructive,
    _check_columns_removed,
    _check_rows_dropped,
    _check_type_changes,
    _extract_selected_columns,
    _infer_type,
    detect_destructive_operations,
    format_destructive_warnings,
    raise_if_destructive,
)
from loafer.exceptions import TransformError


class TestInferType:
    def test_empty_list(self):
        assert _infer_type([]) == "null"

    def test_all_none(self):
        assert _infer_type([None, None]) == "null"

    def test_strings(self):
        assert _infer_type(["a", "b", "c"]) == "string"

    def test_integers(self):
        assert _infer_type([1, 2, 3]) == "integer"

    def test_floats(self):
        assert _infer_type([1.0, 2.5]) == "float"

    def test_booleans(self):
        assert _infer_type([True, False]) == "boolean"

    def test_lists(self):
        assert _infer_type([[1, 2], [3]]) == "object"

    def test_dicts(self):
        assert _infer_type([{"a": 1}]) == "object"

    def test_mixed_types(self):
        assert _infer_type([1, "two", 3.0]) == "mixed"

    def test_nulls_ignored(self):
        assert _infer_type([None, 1, 2]) == "integer"


class TestCheckRowsDropped:
    def test_no_drops(self):
        raw = [{"id": i} for i in range(10)]
        transformed = list(raw)
        result = _check_rows_dropped(raw, transformed, threshold=0.3)
        assert result == []

    def test_below_threshold(self):
        raw = [{"id": i} for i in range(10)]
        transformed = raw[:8]
        result = _check_rows_dropped(raw, transformed, threshold=0.3)
        assert result == []

    def test_above_threshold(self):
        raw = [{"id": i} for i in range(10)]
        transformed = raw[:5]
        result = _check_rows_dropped(raw, transformed, threshold=0.3)
        assert len(result) == 1
        assert result[0].reason == DestructiveReason.ROWS_DROPPED
        assert result[0].severity == "warning"
        assert "50%" in result[0].message

    def test_all_dropped(self):
        raw = [{"id": i} for i in range(10)]
        transformed = []
        result = _check_rows_dropped(raw, transformed, threshold=0.3)
        assert len(result) == 1
        assert result[0].severity == "error"
        assert result[0].details["drop_rate"] == 1.0

    def test_empty_raw(self):
        result = _check_rows_dropped([], [{"id": 1}], threshold=0.3)
        assert result == []


class TestCheckColumnsRemoved:
    def test_no_columns_removed(self):
        raw = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        transformed = [{"a": 10, "b": 20}]
        result = _check_columns_removed(raw, transformed)
        assert result == []

    def test_columns_removed(self):
        raw = [{"a": 1, "b": 2, "c": 3}]
        transformed = [{"a": 10}]
        result = _check_columns_removed(raw, transformed)
        assert len(result) == 1
        assert result[0].reason == DestructiveReason.COLUMNS_REMOVED
        assert "b" in result[0].details["removed"]
        assert "c" in result[0].details["removed"]

    def test_sparse_data_all_columns_detected(self):
        raw = [{"a": 1}, {"b": 2}, {"c": 3}]
        transformed = [{"a": 10}]
        result = _check_columns_removed(raw, transformed)
        assert len(result) == 1
        assert "b" in result[0].details["removed"]
        assert "c" in result[0].details["removed"]

    def test_empty_data(self):
        assert _check_columns_removed([], []) == []
        assert _check_columns_removed([{"a": 1}], []) == []
        assert _check_columns_removed([], [{"a": 1}]) == []


class TestCheckTypeChanges:
    def test_no_type_change(self):
        raw = [{"a": 1}, {"a": 2}]
        transformed = [{"a": 10}, {"a": 20}]
        result = _check_type_changes(raw, transformed)
        assert result == []

    def test_type_change_detected(self):
        raw = [{"a": "hello"}, {"a": "world"}]
        transformed = [{"a": 123}, {"a": 456}]
        result = _check_type_changes(raw, transformed)
        assert len(result) == 1
        assert result[0].reason == DestructiveReason.COLUMN_TYPES_CHANGED
        assert "a:" in result[0].message

    def test_null_to_type_not_flagged(self):
        raw = [{"a": None}, {"a": None}]
        transformed = [{"a": 1}, {"a": 2}]
        result = _check_type_changes(raw, transformed)
        assert result == []

    def test_type_to_null_not_flagged(self):
        raw = [{"a": 1}, {"a": 2}]
        transformed = [{"a": None}, {"a": None}]
        result = _check_type_changes(raw, transformed)
        assert result == []

    def test_sparse_data_columns(self):
        raw = [{"a": 1}, {"a": "hello"}]
        transformed = [{"a": 10}]
        result = _check_type_changes(raw, transformed)
        assert len(result) == 1


class TestAnalyzeSqlDestructive:
    def test_simple_select_no_warnings(self):
        result = _analyze_sql_destructive("SELECT id, name FROM users", None)
        assert result == []

    def test_where_clause(self):
        result = _analyze_sql_destructive("SELECT id FROM users WHERE age > 18", None)
        assert len(result) == 1
        assert result[0].reason == DestructiveReason.SQL_FILTERS_ROWS
        assert "WHERE" in result[0].message

    def test_group_by(self):
        result = _analyze_sql_destructive("SELECT dept, COUNT(*) FROM users GROUP BY dept", None)
        assert any(
            w.reason == DestructiveReason.SQL_FILTERS_ROWS and "GROUP BY" in w.message
            for w in result
        )

    def test_having(self):
        result = _analyze_sql_destructive(
            "SELECT dept, COUNT(*) FROM users GROUP BY dept HAVING COUNT(*) > 5", None
        )
        assert any(
            w.reason == DestructiveReason.SQL_FILTERS_ROWS and "HAVING" in w.message for w in result
        )

    def test_distinct(self):
        result = _analyze_sql_destructive("SELECT DISTINCT name FROM users", None)
        assert any(
            w.reason == DestructiveReason.SQL_FILTERS_ROWS and "DISTINCT" in w.message
            for w in result
        )

    def test_limit(self):
        result = _analyze_sql_destructive("SELECT id FROM users LIMIT 10", None)
        assert any(
            w.reason == DestructiveReason.SQL_FILTERS_ROWS and "LIMIT" in w.message for w in result
        )

    def test_offset(self):
        result = _analyze_sql_destructive("SELECT id FROM users LIMIT 10 OFFSET 5", None)
        assert any(
            w.reason == DestructiveReason.SQL_FILTERS_ROWS and "OFFSET" in w.message for w in result
        )

    def test_inner_join(self):
        result = _analyze_sql_destructive(
            "SELECT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id", None
        )
        assert any(
            w.reason == DestructiveReason.SQL_FILTERS_ROWS and "INNER JOIN" in w.message
            for w in result
        )

    def test_left_join_no_warning(self):
        result = _analyze_sql_destructive(
            "SELECT u.id FROM users u LEFT JOIN orders o ON u.id = o.user_id", None
        )
        assert not any("INNER JOIN" in w.message for w in result)

    def test_column_drops_detected(self):
        raw = [{"id": 1, "name": "Alice", "email": "alice@example.com"}]
        result = _analyze_sql_destructive("SELECT id FROM raw_table", raw)
        assert any(w.reason == DestructiveReason.SQL_DROPS_COLUMNS for w in result)

    def test_select_star_no_column_warning(self):
        raw = [{"id": 1, "name": "Alice"}]
        result = _analyze_sql_destructive("SELECT * FROM raw_table", raw)
        assert not any(w.reason == DestructiveReason.SQL_DROPS_COLUMNS for w in result)

    def test_aliased_columns(self):
        raw = [{"id": 1, "name": "Alice", "email": "alice@example.com"}]
        result = _analyze_sql_destructive("SELECT id, name AS full_name FROM raw_table", raw)
        drop_warnings = [w for w in result if w.reason == DestructiveReason.SQL_DROPS_COLUMNS]
        if drop_warnings:
            assert "email" in drop_warnings[0].details["dropped"]

    def test_parse_error_returns_warning(self):
        result = _analyze_sql_destructive("SELECT * FORM users", None)
        assert len(result) >= 1
        assert result[0].severity == "error"

    def test_non_select_returns_empty(self):
        result = _analyze_sql_destructive("DROP TABLE users", None)
        assert result == []

    def test_empty_sql(self):
        result = _analyze_sql_destructive("", None)
        assert result == []


class TestExtractSelectedColumns:
    def test_simple_columns(self):
        import sqlglot

        stmt = sqlglot.parse("SELECT id, name FROM users")[0]
        cols = _extract_selected_columns(stmt)
        assert cols == {"id", "name"}

    def test_aliased_columns(self):
        import sqlglot

        stmt = sqlglot.parse("SELECT id, name AS full_name FROM users")[0]
        cols = _extract_selected_columns(stmt)
        assert "full_name" in cols
        assert "id" in cols

    def test_star_returns_empty(self):
        import sqlglot

        stmt = sqlglot.parse("SELECT * FROM users")[0]
        cols = _extract_selected_columns(stmt)
        assert cols == set()

    def test_expression_columns(self):
        import sqlglot

        stmt = sqlglot.parse("SELECT COUNT(*) AS cnt FROM users")[0]
        cols = _extract_selected_columns(stmt)
        assert "cnt" in cols


class TestDetectDestructiveOperations:
    def test_etl_no_changes(self):
        data = [{"id": i, "name": f"user_{i}"} for i in range(10)]
        before = {"raw_data": data}
        after = {"transformed_data": data}
        result = detect_destructive_operations(before, after)
        assert result == []

    def test_etl_rows_dropped(self):
        raw = [{"id": i} for i in range(10)]
        transformed = raw[:3]
        before = {"raw_data": raw}
        after = {"transformed_data": transformed}
        result = detect_destructive_operations(before, after)
        assert any(w.reason == DestructiveReason.ROWS_DROPPED for w in result)

    def test_etl_columns_removed(self):
        raw = [{"id": 1, "name": "Alice", "email": "a@b.com"}]
        transformed = [{"id": 1}]
        before = {"raw_data": raw}
        after = {"transformed_data": transformed}
        result = detect_destructive_operations(before, after)
        assert any(w.reason == DestructiveReason.COLUMNS_REMOVED for w in result)

    def test_elt_sql_filtering(self):
        before = {}
        after = {"generated_sql": "SELECT id FROM users WHERE active = 1"}
        result = detect_destructive_operations(before, after)
        assert any(w.reason == DestructiveReason.SQL_FILTERS_ROWS for w in result)

    def test_both_etl_and_elt_data(self):
        raw = [{"id": i, "name": f"user_{i}"} for i in range(10)]
        transformed = raw[:2]
        before = {"raw_data": raw}
        after = {
            "transformed_data": transformed,
            "generated_sql": "SELECT id FROM users WHERE active = 1",
        }
        result = detect_destructive_operations(before, after)
        reasons = {w.reason for w in result}
        assert DestructiveReason.ROWS_DROPPED in reasons
        assert DestructiveReason.SQL_FILTERS_ROWS in reasons


class TestFormatDestructiveWarnings:
    def test_empty_returns_empty_string(self):
        assert format_destructive_warnings([]) == ""

    def test_warning_format(self):
        warnings = [
            DestructiveWarning(
                reason=DestructiveReason.ROWS_DROPPED,
                message="Dropped 50% of rows",
                severity="warning",
                details={},
            )
        ]
        result = format_destructive_warnings(warnings)
        assert "Destructive operations detected:" in result
        assert "Dropped 50% of rows" in result
        assert "--yes" in result

    def test_error_icon(self):
        warnings = [
            DestructiveWarning(
                reason=DestructiveReason.ROWS_DROPPED,
                message="All rows dropped",
                severity="error",
                details={},
            )
        ]
        result = format_destructive_warnings(warnings)
        assert "All rows dropped" in result


class TestRaiseIfDestructive:
    def test_no_warnings_no_raise(self):
        raise_if_destructive([])

    def test_warnings_raises(self):
        warnings = [
            DestructiveWarning(
                reason=DestructiveReason.ROWS_DROPPED,
                message="Dropped rows",
                severity="warning",
                details={},
            )
        ]
        with pytest.raises(TransformError):
            raise_if_destructive(warnings)

    def test_auto_confirmed_no_raise(self):
        warnings = [
            DestructiveWarning(
                reason=DestructiveReason.ROWS_DROPPED,
                message="Dropped rows",
                severity="warning",
                details={},
            )
        ]
        raise_if_destructive(warnings, auto_confirmed=True)
