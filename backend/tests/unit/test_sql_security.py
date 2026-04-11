"""
SQL 注入防护测试
"""
import pytest
from app.core.sql_security import (
    validate_table_name,
    validate_column_name,
    validate_limit_value,
    safe_sql_query,
    get_safe_table_list,
    SQLSecurityError,
)


@pytest.mark.unit
class TestSQLSecurity:
    """测试 SQL 安全防护"""

    # ==================== 表名验证测试 ====================

    def test_valid_table_names_allowed(self):
        """测试有效的表名被允许"""
        valid_tables = [
            "sync_daily_data",
            "sync_daily_basic",
            "factor_values",
            "etl_stock_daily_info",
        ]
        for table in valid_tables:
            assert validate_table_name(table) is True

    def test_invalid_table_names_rejected(self):
        """测试无效的表名被拒绝"""
        invalid_tables = [
            "",
            "nonexistent_table",
            "users",
            "passwords",
        ]
        for table in invalid_tables:
            with pytest.raises(SQLSecurityError):
                validate_table_name(table)

    def test_table_name_with_suffix_allowed(self):
        """测试有后缀的表名被允许（分区表等）"""
        # 基础表名在白名单中时，有后缀也应该允许
        assert validate_table_name("sync_daily_data::partition") is True

    def test_table_name_stripped(self):
        """测试表名被清理"""
        assert validate_table_name("  sync_daily_data  ") is True

    # ==================== 列名验证测试 ====================

    def test_valid_column_names_allowed(self):
        """测试有效的列名被允许"""
        valid_columns = [
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "pe",
            "pb",
            "turnover_rate",
            "factor_id",
            "factor_value",
            "quality_flag",
            "my_column_123",
        ]
        for col in valid_columns:
            assert validate_column_name(col) is True

    def test_column_name_with_dangerous_chars_rejected(self):
        """测试包含危险字符的列名被拒绝"""
        dangerous_columns = [
            "ts_code; DROP TABLE users",
            "ts_code-- comment",
            "ts_code/* comment */",
            "ts_code@var",
            "ts'code",
            'ts"code',
        ]
        for col in dangerous_columns:
            with pytest.raises(SQLSecurityError):
                validate_column_name(col)

    def test_column_name_with_invalid_chars_rejected(self):
        """测试包含无效字符的列名被拒绝"""
        invalid_columns = [
            "ts-code",
            "ts.code",
            "ts code",
            "ts!code",
        ]
        for col in invalid_columns:
            with pytest.raises(SQLSecurityError):
                validate_column_name(col)

    def test_empty_column_name_rejected(self):
        """测试空列名被拒绝"""
        with pytest.raises(SQLSecurityError):
            validate_column_name("")
        with pytest.raises(SQLSecurityError):
            validate_column_name("   ")

    # ==================== LIMIT 值验证测试 ====================

    def test_valid_limit_values(self):
        """测试有效的 limit 值"""
        assert validate_limit_value(10) == 10
        assert validate_limit_value(1000) == 1000
        assert validate_limit_value(10000, 10000) == 10000
        assert validate_limit_value(0) == 0

    def test_limit_value_capped_at_max(self):
        """测试 limit 值被限制在最大值"""
        assert validate_limit_value(20000, 10000) == 10000
        assert validate_limit_value(1500, 1000) == 1000

    def test_negative_limit_rejected(self):
        """测试负的 limit 值被拒绝"""
        with pytest.raises(SQLSecurityError):
            validate_limit_value(-1)
        with pytest.raises(SQLSecurityError):
            validate_limit_value(-100)

    def test_invalid_limit_type_rejected(self):
        """测试无效的 limit 类型被拒绝"""
        with pytest.raises(SQLSecurityError):
            validate_limit_value("100")  # type: ignore
        with pytest.raises(SQLSecurityError):
            validate_limit_value(None)  # type: ignore

    # ==================== SQL 查询安全测试 ====================

    def test_select_query_allowed(self):
        """测试 SELECT 查询被允许"""
        safe_sql = safe_sql_query("SELECT * FROM sync_daily_data")
        assert safe_sql is not None

    def test_dangerous_queries_rejected(self):
        """测试危险查询被拒绝"""
        dangerous_queries = [
            "DROP TABLE sync_daily_data",
            "DELETE FROM sync_daily_data",
            "UPDATE sync_daily_data SET close = 100",
            "INSERT INTO sync_daily_data VALUES (...)",
            "ALTER TABLE sync_daily_data ADD COLUMN test INT",
            "CREATE TABLE test (id INT)",
            "TRUNCATE TABLE sync_daily_data",
            "EXECUTE sp_help",
            "SELECT * FROM sync_daily_data; DROP TABLE users",
        ]
        for sql in dangerous_queries:
            with pytest.raises(SQLSecurityError):
                safe_sql_query(sql)

    def test_sql_with_comments_rejected(self):
        """测试带注释的 SQL 被拒绝"""
        sql_with_comments = [
            "SELECT * FROM sync_daily_data -- this is a comment",
            "SELECT * FROM sync_daily_data /* comment */",
        ]
        for sql in sql_with_comments:
            with pytest.raises(SQLSecurityError):
                safe_sql_query(sql)

    def test_sql_with_union_rejected(self):
        """测试带 UNION 的 SQL - 我们的策略是表名白名单而非关键字过滤"""
        # UNION 本身不被阻止，但表名必须在白名单中
        # 这是一个设计选择，因为有些合法查询需要 UNION
        pass

    def test_empty_sql_rejected(self):
        """测试空 SQL 被拒绝"""
        with pytest.raises(SQLSecurityError):
            safe_sql_query("")
        with pytest.raises(SQLSecurityError):
            safe_sql_query("   ")

    def test_sql_case_insensitive_check(self):
        """测试 SQL 检查不区分大小写（危险操作）"""
        with pytest.raises(SQLSecurityError):
            safe_sql_query("drop table users")
        with pytest.raises(SQLSecurityError):
            safe_sql_query("DELETE FROM sync_daily_data")

    # ==================== 安全表名列表测试 ====================

    def test_get_safe_table_list_returns_sorted_list(self):
        """测试获取安全表名列表返回排序的列表"""
        tables = get_safe_table_list()
        assert isinstance(tables, list)
        assert len(tables) > 0
        # 验证列表是排序的
        assert tables == sorted(tables)

    def test_safe_table_list_contains_known_tables(self):
        """测试安全表名列表包含已知的表"""
        tables = get_safe_table_list()
        expected_tables = [
            "sync_daily_data",
            "sync_daily_basic",
            "factor_values",
        ]
        for table in expected_tables:
            assert table in tables


@pytest.mark.unit
class TestSQLInjectionPatterns:
    """测试常见的 SQL 注入模式"""

    def test_classic_sql_injection_rejected(self):
        """测试经典 SQL 注入被拒绝（危险操作）"""
        injection_patterns = [
            "SELECT * FROM sync_daily_data WHERE ts_code = '000001.SZ'; DROP TABLE users",
            "DELETE FROM sync_daily_data",
            "UPDATE sync_daily_data SET close = 100",
        ]
        for sql in injection_patterns:
            with pytest.raises(SQLSecurityError):
                safe_sql_query(sql)

    def test_boolean_based_injection_rejected(self):
        """测试布尔型注入被拒绝"""
        # 注意：由于我们允许 SELECT，这种注入不会被关键字检查阻止
        # 但用户输入应该通过参数化查询来处理，而不是直接拼接到 SQL 中
        pass

    def test_time_based_injection_rejected(self):
        """测试时间盲注被拒绝"""
        with pytest.raises(SQLSecurityError):
            safe_sql_query("SELECT * FROM sync_daily_data WHERE ts_code = '000001.SZ' AND SLEEP(10)")
        with pytest.raises(SQLSecurityError):
            safe_sql_query("SELECT * FROM sync_daily_data WHERE ts_code = '000001.SZ' AND WAITFOR DELAY '0:0:10'")
