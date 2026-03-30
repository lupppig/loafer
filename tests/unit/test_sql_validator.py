"""Tests for loafer.transform.sql_validator."""

from __future__ import annotations

from loafer.transform.sql_validator import validate_transform_sql


class TestValidateTransformSql:
    """SQL validator edge cases from the spec."""

    def test_valid_select(self) -> None:
        ok, err = validate_transform_sql("SELECT id, name FROM orders")
        assert ok is True
        assert err is None

    def test_valid_select_with_where_and_order(self) -> None:
        sql = "SELECT id, name, total FROM orders WHERE total > 100 ORDER BY total DESC"
        ok, err = validate_transform_sql(sql)
        assert ok is True
        assert err is None

    def test_valid_select_with_multiple_columns(self) -> None:
        sql = "SELECT a, b, c, d, e FROM t WHERE a > 1"
        ok, err = validate_transform_sql(sql)
        assert ok is True
        assert err is None

    def test_drop_table(self) -> None:
        ok, err = validate_transform_sql("DROP TABLE orders")
        assert ok is False
        assert err is not None and "Drop" in err

    def test_delete_from(self) -> None:
        ok, err = validate_transform_sql("DELETE FROM users WHERE 1=1")
        assert ok is False
        assert err is not None and "Delete" in err

    def test_update(self) -> None:
        ok, err = validate_transform_sql("UPDATE orders SET status = 'paid'")
        assert ok is False
        assert err is not None and "Update" in err

    def test_insert(self) -> None:
        ok, err = validate_transform_sql("INSERT INTO orders VALUES (1)")
        assert ok is False
        assert err is not None and "Insert" in err

    def test_create_table(self) -> None:
        ok, err = validate_transform_sql("CREATE TABLE foo AS SELECT 1")
        assert ok is False
        assert err is not None and "Create" in err

    def test_two_statements(self) -> None:
        ok, err = validate_transform_sql("SELECT id FROM orders; DELETE FROM users")
        assert ok is False
        assert err is not None and "2" in err

    def test_malformed_sql(self) -> None:
        ok, err = validate_transform_sql("SELEKT id FORM orders")
        # sqlglot may or may not parse this — either a syntax error or a
        # non-SELECT type rejection is acceptable.
        assert ok is False
        assert err is not None

    def test_valid_select_with_subquery(self) -> None:
        sql = "SELECT * FROM orders WHERE id IN (SELECT order_id FROM items)"
        ok, err = validate_transform_sql(sql)
        assert ok is True
        assert err is None

    def test_select_with_join(self) -> None:
        sql = "SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id"
        ok, err = validate_transform_sql(sql)
        assert ok is True
        assert err is None

    def test_select_with_aggregation(self) -> None:
        sql = "SELECT user_id, COUNT(*) as cnt FROM orders GROUP BY user_id HAVING cnt > 1"
        ok, err = validate_transform_sql(sql)
        assert ok is True
        assert err is None
