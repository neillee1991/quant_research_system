"""
Unit tests for QueryBuilder

测试覆盖：
- SELECT 子句（指定列、所有列）
- WHERE 子句（=, >, <, LIKE 等）
- WHERE IN 子句
- WHERE BETWEEN 子句
- WHERE NULL/NOT NULL 子句
- ORDER BY 子句
- LIMIT 子句
- 链式调用
- 参数化查询（防止SQL注入）
"""
import pytest
from infrastructure.database.query_builder import QueryBuilder, Query


class TestQueryBuilder:
    """QueryBuilder 单元测试"""

    def test_select_all(self):
        """测试 SELECT * 查询"""
        query = QueryBuilder("test_table").select_all().build()

        assert query.sql == "SELECT * FROM test_table"
        assert query.params == ()

    def test_select_columns(self):
        """测试 SELECT 指定列"""
        query = QueryBuilder("test_table") \
            .select(["col1", "col2", "col3"]) \
            .build()

        assert query.sql == "SELECT col1, col2, col3 FROM test_table"
        assert query.params == ()

    def test_where_simple(self):
        """测试简单 WHERE 条件"""
        query = QueryBuilder("test_table") \
            .select_all() \
            .where("id", "=", 123) \
            .build()

        assert query.sql == "SELECT * FROM test_table WHERE id = %s"
        assert query.params == (123,)

    def test_where_multiple(self):
        """测试多个 WHERE 条件"""
        query = QueryBuilder("test_table") \
            .select_all() \
            .where("id", ">", 100) \
            .where("status", "=", "active") \
            .build()

        assert query.sql == "SELECT * FROM test_table WHERE id > %s AND status = %s"
        assert query.params == (100, "active")

    def test_where_in(self):
        """测试 WHERE IN 子句"""
        query = QueryBuilder("test_table") \
            .select_all() \
            .where_in("ts_code", ["000001.SZ", "000002.SZ", "000003.SZ"]) \
            .build()

        assert query.sql == "SELECT * FROM test_table WHERE ts_code IN (%s, %s, %s)"
        assert query.params == ("000001.SZ", "000002.SZ", "000003.SZ")

    def test_where_in_empty(self):
        """测试 WHERE IN 空列表（应该被忽略）"""
        query = QueryBuilder("test_table") \
            .select_all() \
            .where_in("ts_code", []) \
            .build()

        assert query.sql == "SELECT * FROM test_table"
        assert query.params == ()

    def test_where_between(self):
        """测试 WHERE BETWEEN 子句"""
        query = QueryBuilder("test_table") \
            .select_all() \
            .where_between("trade_date", "20240101", "20240131") \
            .build()

        assert query.sql == "SELECT * FROM test_table WHERE trade_date >= %s AND trade_date <= %s"
        assert query.params == ("20240101", "20240131")

    def test_where_null(self):
        """测试 WHERE IS NULL"""
        query = QueryBuilder("test_table") \
            .select_all() \
            .where_null("deleted_at") \
            .build()

        assert query.sql == "SELECT * FROM test_table WHERE deleted_at IS NULL"
        assert query.params == ()

    def test_where_not_null(self):
        """测试 WHERE IS NOT NULL"""
        query = QueryBuilder("test_table") \
            .select_all() \
            .where_not_null("factor_value") \
            .build()

        assert query.sql == "SELECT * FROM test_table WHERE factor_value IS NOT NULL"
        assert query.params == ()

    def test_order_by_single(self):
        """测试单列排序"""
        query = QueryBuilder("test_table") \
            .select_all() \
            .order_by(["trade_date DESC"]) \
            .build()

        assert query.sql == "SELECT * FROM test_table ORDER BY trade_date DESC"
        assert query.params == ()

    def test_order_by_multiple(self):
        """测试多列排序"""
        query = QueryBuilder("test_table") \
            .select_all() \
            .order_by(["trade_date DESC", "ts_code ASC"]) \
            .build()

        assert query.sql == "SELECT * FROM test_table ORDER BY trade_date DESC, ts_code ASC"
        assert query.params == ()

    def test_limit(self):
        """测试 LIMIT 子句"""
        query = QueryBuilder("test_table") \
            .select_all() \
            .limit(100) \
            .build()

        assert query.sql == "SELECT * FROM test_table LIMIT 100"
        assert query.params == ()

    def test_complex_query(self):
        """测试复杂查询（组合多个子句）"""
        query = QueryBuilder("sync_daily_data") \
            .select(["ts_code", "trade_date", "close"]) \
            .where_in("ts_code", ["000001.SZ", "000002.SZ"]) \
            .where_between("trade_date", "20240101", "20240131") \
            .where("close", ">", 10.0) \
            .order_by(["trade_date DESC", "ts_code ASC"]) \
            .limit(100) \
            .build()

        expected_sql = (
            "SELECT ts_code, trade_date, close FROM sync_daily_data "
            "WHERE ts_code IN (%s, %s) AND trade_date >= %s AND trade_date <= %s AND close > %s "
            "ORDER BY trade_date DESC, ts_code ASC LIMIT 100"
        )

        assert query.sql == expected_sql
        assert query.params == ("000001.SZ", "000002.SZ", "20240101", "20240131", 10.0)

    def test_reset(self):
        """测试重置查询构建器"""
        builder = QueryBuilder("test_table")

        # 构建第一个查询
        query1 = builder.select(["col1"]).where("id", "=", 1).build()
        assert query1.sql == "SELECT col1 FROM test_table WHERE id = %s"

        # 重置后构建第二个查询
        query2 = builder.reset().select(["col2"]).where("id", "=", 2).build()
        assert query2.sql == "SELECT col2 FROM test_table WHERE id = %s"
        assert query2.params == (2,)

    def test_chaining(self):
        """测试链式调用返回 self"""
        builder = QueryBuilder("test_table")

        result = builder.select(["col1"]) \
            .where("id", "=", 1) \
            .where_in("status", ["active", "pending"]) \
            .order_by(["created_at DESC"]) \
            .limit(10)

        assert result is builder

    def test_sql_injection_prevention(self):
        """测试防止SQL注入（参数化查询）"""
        # 尝试注入恶意SQL
        malicious_input = "'; DROP TABLE test_table; --"

        query = QueryBuilder("test_table") \
            .select_all() \
            .where("name", "=", malicious_input) \
            .build()

        # 参数应该被正确转义
        assert query.sql == "SELECT * FROM test_table WHERE name = %s"
        assert query.params == (malicious_input,)

    def test_query_repr(self):
        """测试 Query 对象的字符串表示"""
        query = Query("SELECT * FROM test", (1, 2, 3))
        repr_str = repr(query)

        assert "SELECT * FROM test" in repr_str
        assert "(1, 2, 3)" in repr_str

    def test_empty_where_clauses(self):
        """测试没有 WHERE 条件的查询"""
        query = QueryBuilder("test_table") \
            .select(["col1", "col2"]) \
            .build()

        assert query.sql == "SELECT col1, col2 FROM test_table"
        assert query.params == ()

    def test_where_operators(self):
        """测试不同的 WHERE 操作符"""
        q1 = QueryBuilder("test").where("price", ">", 100).build()
        assert "price > %s" in q1.sql

        q2 = QueryBuilder("test").where("price", "<=", 50).build()
        assert "price <= %s" in q2.sql

        q3 = QueryBuilder("test").where("status", "!=", "deleted").build()
        assert "status != %s" in q3.sql

        q4 = QueryBuilder("test").where("name", "LIKE", "%test%").build()
        assert "name LIKE %s" in q4.sql

    def test_multiple_where_in(self):
        """测试多个 WHERE IN 条件"""
        query = QueryBuilder("test_table") \
            .where_in("ts_code", ["000001.SZ", "000002.SZ"]) \
            .where_in("status", ["active", "pending"]) \
            .build()

        assert "ts_code IN (%s, %s)" in query.sql
        assert "status IN (%s, %s)" in query.sql
        assert query.params == ("000001.SZ", "000002.SZ", "active", "pending")

    def test_mixed_conditions(self):
        """测试混合条件"""
        query = QueryBuilder("test_table") \
            .where("status", "=", "active") \
            .where_in("category", ["A", "B"]) \
            .where_between("price", 10, 100) \
            .where_not_null("description") \
            .build()

        assert "status = %s" in query.sql
        assert "category IN (%s, %s)" in query.sql
        assert "price >= %s AND price <= %s" in query.sql
        assert "description IS NOT NULL" in query.sql
        assert query.params == ("active", "A", "B", 10, 100)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
