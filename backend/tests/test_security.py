"""
安全相关测试
验证 SQL 注入防护、XSS 防护、路径遍历防护等安全机制
"""
import pytest
import re
from typing import Any, List


class TestQueryBuilderSecurity:
    """测试 QueryBuilder 的安全性（参数化查询防护）"""

    def test_string_value_uses_parameter(self):
        """字符串值应使用参数化查询占位符"""
        from app.core.utils import QueryBuilder
        clause, params = QueryBuilder.build_where_clause({"ts_code": "000001.SZ"})
        assert "= %s" in clause
        assert params == ["000001.SZ"]

    def test_none_value_becomes_is_null(self):
        """None 值应生成 IS NULL 条件"""
        from app.core.utils import QueryBuilder
        clause, params = QueryBuilder.build_where_clause({"field": None})
        assert "IS NULL" in clause
        assert params == []

    def test_list_value_becomes_in_clause(self):
        """列表值应生成带参数的 IN 子句"""
        from app.core.utils import QueryBuilder
        clause, params = QueryBuilder.build_where_clause({"ts_code": ["A", "B", "C"]})
        assert "IN (" in clause
        assert "%s" in clause
        assert params == ["A", "B", "C"]

    def test_special_chars_in_string_safe_with_parameters(self):
        """
        验证 QueryBuilder 使用参数化查询来防止 SQL 注入。
        特殊字符不再直接嵌入 SQL 中，而是作为参数传递，
        彻底避免 SQL 注入风险。
        """
        from app.core.utils import QueryBuilder
        # 包含单引号的值，现在不会被直接插入 SQL
        malicious = "value' OR '1'='1"
        clause, params = QueryBuilder.build_where_clause({"field": malicious})
        # 验证使用参数占位符
        assert "= %s" in clause
        # 验证恶意值在参数列表中，不是直接嵌入 SQL
        assert params == [malicious]
        # 验证恶意字符串没有直接出现在 SQL 中
        assert malicious not in clause

    def test_integer_value_uses_parameter(self):
        """整数值应使用参数化查询"""
        from app.core.utils import QueryBuilder
        clause, params = QueryBuilder.build_where_clause({"count": 42})
        assert "= %s" in clause
        assert params == [42]

    def test_empty_filters_returns_empty_clause(self):
        """空过滤器应返回空字符串和空参数列表"""
        from app.core.utils import QueryBuilder
        clause, params = QueryBuilder.build_where_clause({})
        assert clause == ""
        assert params == []

    def test_multiple_conditions_joined_with_and(self):
        """多个条件应用 AND 连接，使用参数化查询"""
        from app.core.utils import QueryBuilder
        clause, params = QueryBuilder.build_where_clause({
            "ts_code": "000001.SZ",
            "trade_date": "20240101"
        })
        assert "AND" in clause
        assert clause.startswith("WHERE")
        assert params == ["000001.SZ", "20240101"]
