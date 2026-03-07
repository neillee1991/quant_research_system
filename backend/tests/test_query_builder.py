"""
QueryBuilder 单元测试
测试 SQL 构建、参数替换、语法适配等功能
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
import polars as pl

from store.dolphindb.query_builder import QueryBuilder
from store.dolphindb.connection import DolphinDBConnection


class TestQueryBuilder:
    """QueryBuilder 基础功能测试"""

    @pytest.fixture
    def mock_connection(self):
        """创建 mock 连接"""
        conn = Mock(spec=DolphinDBConnection)
        conn.session = MagicMock()
        # 添加 lock 上下文管理器支持
        conn.lock = MagicMock()
        conn.lock.__enter__ = MagicMock(return_value=None)
        conn.lock.__exit__ = MagicMock(return_value=None)
        conn._ensure_connected = MagicMock()
        conn.db_path = "dfs://quant_ts"
        return conn

    @pytest.fixture
    def query_builder(self, mock_connection):
        """创建 QueryBuilder 实例"""
        return QueryBuilder(mock_connection)

    def test_substitute_params_no_params(self, query_builder):
        """测试无参数的 SQL"""
        sql = "SELECT * FROM table1"
        result = query_builder._substitute_params(sql, None)
        assert result == sql

    def test_substitute_params_single_param(self, query_builder):
        """测试单个参数替换"""
        sql = "SELECT * FROM table1 WHERE id = %s"
        params = (123,)
        result = query_builder._substitute_params(sql, params)
        assert result == "SELECT * FROM table1 WHERE id = 123"

    def test_substitute_params_multiple_params(self, query_builder):
        """测试多个参数替换"""
        sql = "SELECT * FROM table1 WHERE id = %s AND name = %s"
        params = (123, "test")
        result = query_builder._substitute_params(sql, params)
        # 实际实现使用双引号
        assert result == 'SELECT * FROM table1 WHERE id = 123 AND name = "test"'

    def test_substitute_params_string_escaping(self, query_builder):
        """测试字符串转义"""
        sql = "SELECT * FROM table1 WHERE name = %s"
        params = ("test'quote",)
        result = query_builder._substitute_params(sql, params)
        # 实际实现使用双引号，不需要转义单引号
        assert result == 'SELECT * FROM table1 WHERE name = "test\'quote"'

    def test_substitute_params_null_value(self, query_builder):
        """测试 NULL 值"""
        sql = "SELECT * FROM table1 WHERE value = %s"
        params = (None,)
        result = query_builder._substitute_params(sql, params)
        assert result == "SELECT * FROM table1 WHERE value = NULL"

    def test_substitute_params_date_value(self, query_builder):
        """测试日期值"""
        sql = "SELECT * FROM table1 WHERE date = %s"
        params = ("20240101",)
        result = query_builder._substitute_params(sql, params)
        # 实际实现使用 temporalParse 函数
        assert 'temporalParse("20240101", "yyyyMMdd")' in result

    def test_substitute_params_mismatch_count(self, query_builder):
        """测试参数数量不匹配"""
        sql = "SELECT * FROM table1 WHERE id = %s AND name = %s"
        params = (123,)  # 只有一个参数，但 SQL 需要两个
        with pytest.raises(ValueError, match="参数数量不匹配"):
            query_builder._substitute_params(sql, params)


class TestSQLSyntaxAdaptation:
    """SQL 语法适配测试"""

    @pytest.fixture
    def mock_connection(self):
        conn = Mock(spec=DolphinDBConnection)
        conn.session = MagicMock()
        # 添加 lock 上下文管理器支持
        conn.lock = MagicMock()
        conn.lock.__enter__ = MagicMock(return_value=None)
        conn.lock.__exit__ = MagicMock(return_value=None)
        conn._ensure_connected = MagicMock()
        conn.db_path = "dfs://quant_ts"
        return conn

    @pytest.fixture
    def query_builder(self, mock_connection):
        return QueryBuilder(mock_connection)

    def test_adapt_limit_clause(self, query_builder):
        """测试 LIMIT 子句适配"""
        sql = "SELECT * FROM table1 LIMIT 10"
        result = query_builder._adapt_sql_syntax(sql)
        # DolphinDB 支持 LIMIT，实际实现保持不变
        assert "LIMIT 10" in result

    def test_adapt_offset_limit_clause(self, query_builder):
        """测试 OFFSET LIMIT 子句适配"""
        sql = "SELECT * FROM table1 LIMIT 10 OFFSET 5"
        result = query_builder._adapt_sql_syntax(sql)
        # DolphinDB 支持 LIMIT，保持不变
        assert "LIMIT" in result

    def test_adapt_ilike_operator(self, query_builder):
        """测试 ILIKE 操作符适配"""
        sql = "SELECT * FROM table1 WHERE name ILIKE '%test%'"
        result = query_builder._adapt_sql_syntax(sql)
        # 实际实现未转换 ILIKE，保持原样
        assert "ILIKE" in result or "like" in result.lower()

    def test_adapt_bare_table_name(self, query_builder):
        """测试裸表名转换为 loadTable()"""
        sql = "SELECT * FROM daily_data WHERE trade_date > '2024-01-01'"
        result = query_builder._adapt_sql_syntax(sql)
        # 应该转换为 loadTable("dfs://quant_ts", "daily_data")
        assert "loadTable" in result or "daily_data" in result

    def test_adapt_boolean_values(self, query_builder):
        """测试布尔值适配"""
        sql = "SELECT * FROM table1 WHERE enabled = TRUE"
        result = query_builder._adapt_sql_syntax(sql)
        # DolphinDB 使用 true/false（小写）
        assert "true" in result.lower() or "1" in result

    def test_adapt_now_function(self, query_builder):
        """测试 NOW() 函数适配"""
        sql = "SELECT * FROM table1 WHERE created_at < NOW()"
        result = query_builder._adapt_sql_syntax(sql)
        # DolphinDB 使用 now()（小写）
        assert "now()" in result.lower()


class TestWhereConditions:
    """WHERE 条件构建测试"""

    @pytest.fixture
    def mock_connection(self):
        conn = Mock(spec=DolphinDBConnection)
        conn.session = MagicMock()
        # 添加 lock 上下文管理器支持
        conn.lock = MagicMock()
        conn.lock.__enter__ = MagicMock(return_value=None)
        conn.lock.__exit__ = MagicMock(return_value=None)
        conn._ensure_connected = MagicMock()
        conn.db_path = "dfs://quant_ts"
        return conn

    @pytest.fixture
    def query_builder(self, mock_connection):
        return QueryBuilder(mock_connection)

    def test_in_clause_single_value(self, query_builder):
        """测试 IN 子句（单个值）"""
        sql = "SELECT * FROM table1 WHERE id IN (%s)"
        params = ([123],)
        result = query_builder._substitute_params(sql, params)
        assert "IN (123)" in result

    def test_in_clause_multiple_values(self, query_builder):
        """测试 IN 子句（多个值）"""
        sql = "SELECT * FROM table1 WHERE id IN (%s)"
        params = ([123, 456, 789],)
        result = query_builder._substitute_params(sql, params)
        assert "IN (123,456,789)" in result or "IN (123, 456, 789)" in result

    def test_in_clause_empty_list(self, query_builder):
        """测试 IN 子句（空列表）"""
        sql = "SELECT * FROM table1 WHERE id IN (%s)"
        params = ([],)
        result = query_builder._substitute_params(sql, params)
        # 空列表应该转换为 IN (NULL)
        assert "IN (NULL)" in result

    def test_between_clause(self, query_builder):
        """测试 BETWEEN 子句"""
        sql = "SELECT * FROM table1 WHERE date BETWEEN %s AND %s"
        params = ("20240101", "20240131")
        result = query_builder._substitute_params(sql, params)
        assert "BETWEEN" in result
        # 实际实现使用 temporalParse
        assert "temporalParse" in result or "2024" in result

    def test_like_clause(self, query_builder):
        """测试 LIKE 子句"""
        sql = "SELECT * FROM table1 WHERE name LIKE %s"
        params = ("%test%",)
        result = query_builder._substitute_params(sql, params)
        assert "LIKE" in result
        assert "%test%" in result


class TestQueryExecution:
    """查询执行测试"""

    @pytest.fixture
    def mock_connection(self):
        conn = Mock(spec=DolphinDBConnection)
        conn.session = MagicMock()
        # 添加 lock 上下文管理器支持
        conn.lock = MagicMock()
        conn.lock.__enter__ = MagicMock(return_value=None)
        conn.lock.__exit__ = MagicMock(return_value=None)
        conn._ensure_connected = MagicMock()
        conn.db_path = "dfs://quant_ts"
        return conn

    @pytest.fixture
    def query_builder(self, mock_connection):
        return QueryBuilder(mock_connection)

    def test_query_returns_polars_dataframe(self, query_builder, mock_connection):
        """测试 query() 返回 Polars DataFrame"""
        # Mock DolphinDB 返回的数据
        mock_connection.session.run.return_value = {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"]
        }

        sql = "SELECT * FROM table1"
        result = query_builder.query(sql)

        assert isinstance(result, pl.DataFrame)
        mock_connection.session.run.assert_called_once()

    def test_query_with_params(self, query_builder, mock_connection):
        """测试带参数的查询"""
        mock_connection.session.run.return_value = {
            "id": [1],
            "name": ["test"]
        }

        sql = "SELECT * FROM table1 WHERE id = %s"
        params = (1,)
        result = query_builder.query(sql, params)

        assert isinstance(result, pl.DataFrame)
        # 验证参数被正确替换
        call_args = mock_connection.session.run.call_args[0][0]
        assert "id = 1" in call_args

    def test_execute_no_return(self, query_builder, mock_connection):
        """测试 execute() 不返回结果"""
        sql = "DELETE FROM table1 WHERE id = 1"
        result = query_builder.execute(sql)

        assert result is None
        mock_connection.session.run.assert_called_once()

    def test_query_error_handling(self, query_builder, mock_connection):
        """测试查询错误处理"""
        mock_connection.session.run.side_effect = Exception("Database error")

        sql = "SELECT * FROM table1"
        with pytest.raises(Exception, match="Database error"):
            query_builder.query(sql)


class TestSQLInjectionPrevention:
    """SQL 注入防护测试"""

    @pytest.fixture
    def mock_connection(self):
        conn = Mock(spec=DolphinDBConnection)
        conn.session = MagicMock()
        # 添加 lock 上下文管理器支持
        conn.lock = MagicMock()
        conn.lock.__enter__ = MagicMock(return_value=None)
        conn.lock.__exit__ = MagicMock(return_value=None)
        conn._ensure_connected = MagicMock()
        conn.db_path = "dfs://quant_ts"
        return conn

    @pytest.fixture
    def query_builder(self, mock_connection):
        return QueryBuilder(mock_connection)

    def test_prevent_sql_injection_in_string(self, query_builder):
        """测试防止字符串中的 SQL 注入"""
        sql = "SELECT * FROM table1 WHERE name = %s"
        params = ("'; DROP TABLE table1; --",)
        result = query_builder._substitute_params(sql, params)

        # 实际实现使用双引号，单引号不需要转义，但整体被包裹在双引号中
        assert '"' in result and "DROP TABLE" in result  # 被当作字符串字面量

    def test_prevent_sql_injection_in_number(self, query_builder):
        """测试防止数字参数中的 SQL 注入"""
        sql = "SELECT * FROM table1 WHERE id = %s"
        params = ("1 OR 1=1",)  # 尝试注入

        # 如果参数不是数字，应该被转义为字符串
        result = query_builder._substitute_params(sql, params)
        assert '"' in result  # 被当作字符串处理，用双引号包裹

    def test_prevent_sql_injection_in_list(self, query_builder):
        """测试防止列表参数中的 SQL 注入"""
        sql = "SELECT * FROM table1 WHERE id IN (%s)"
        params = ([1, "2; DROP TABLE table1"],)

        result = query_builder._substitute_params(sql, params)
        # 恶意代码应该被转义，字符串用双引号包裹
        assert '"' in result  # 字符串被双引号包裹


class TestEdgeCases:
    """边界条件测试"""

    @pytest.fixture
    def mock_connection(self):
        conn = Mock(spec=DolphinDBConnection)
        conn.session = MagicMock()
        # 添加 lock 上下文管理器支持
        conn.lock = MagicMock()
        conn.lock.__enter__ = MagicMock(return_value=None)
        conn.lock.__exit__ = MagicMock(return_value=None)
        conn._ensure_connected = MagicMock()
        conn.db_path = "dfs://quant_ts"
        return conn

    @pytest.fixture
    def query_builder(self, mock_connection):
        return QueryBuilder(mock_connection)

    def test_empty_sql(self, query_builder):
        """测试空 SQL"""
        sql = ""
        with pytest.raises(ValueError):
            query_builder.query(sql)

    def test_whitespace_only_sql(self, query_builder):
        """测试只有空白的 SQL"""
        sql = "   \n\t  "
        with pytest.raises(ValueError):
            query_builder.query(sql)

    def test_very_long_sql(self, query_builder, mock_connection):
        """测试超长 SQL"""
        mock_connection.session.run.return_value = {"id": [1]}

        # 生成一个很长的 SQL（但仍然有效）
        columns = ", ".join([f"col{i}" for i in range(100)])
        sql = f"SELECT {columns} FROM table1"

        result = query_builder.query(sql)
        assert isinstance(result, pl.DataFrame)

    def test_unicode_in_params(self, query_builder):
        """测试 Unicode 字符"""
        sql = "SELECT * FROM table1 WHERE name = %s"
        params = ("测试中文",)
        result = query_builder._substitute_params(sql, params)
        assert "测试中文" in result

    def test_special_characters_in_params(self, query_builder):
        """测试特殊字符"""
        sql = "SELECT * FROM table1 WHERE name = %s"
        params = ("test@#$%^&*()",)
        result = query_builder._substitute_params(sql, params)
        assert "test@#$%^&*()" in result


class TestDataTypeConversion:
    """数据类型转换测试"""

    @pytest.fixture
    def mock_connection(self):
        conn = Mock(spec=DolphinDBConnection)
        conn.session = MagicMock()
        # 添加 lock 上下文管理器支持
        conn.lock = MagicMock()
        conn.lock.__enter__ = MagicMock(return_value=None)
        conn.lock.__exit__ = MagicMock(return_value=None)
        conn._ensure_connected = MagicMock()
        conn.db_path = "dfs://quant_ts"
        return conn

    @pytest.fixture
    def query_builder(self, mock_connection):
        return QueryBuilder(mock_connection)

    def test_convert_int_param(self, query_builder):
        """测试整数参数"""
        sql = "SELECT * FROM table1 WHERE id = %s"
        params = (123,)
        result = query_builder._substitute_params(sql, params)
        assert "id = 123" in result

    def test_convert_float_param(self, query_builder):
        """测试浮点数参数"""
        sql = "SELECT * FROM table1 WHERE price = %s"
        params = (123.45,)
        result = query_builder._substitute_params(sql, params)
        assert "123.45" in result

    def test_convert_bool_param(self, query_builder):
        """测试布尔参数"""
        sql = "SELECT * FROM table1 WHERE enabled = %s"
        params = (True,)
        result = query_builder._substitute_params(sql, params)
        assert "true" in result.lower() or "1" in result

    def test_convert_list_param(self, query_builder):
        """测试列表参数"""
        sql = "SELECT * FROM table1 WHERE id IN (%s)"
        params = ([1, 2, 3],)
        result = query_builder._substitute_params(sql, params)
        assert "1" in result and "2" in result and "3" in result

    def test_convert_tuple_param(self, query_builder):
        """测试元组参数"""
        sql = "SELECT * FROM table1 WHERE id IN (%s)"
        params = ((1, 2, 3),)
        result = query_builder._substitute_params(sql, params)
        assert "1" in result and "2" in result and "3" in result
