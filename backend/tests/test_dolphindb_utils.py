"""DolphinDB 客户端 SQL 工具方法的单元测试（不依赖数据库连接）"""
from datetime import datetime, date

import pytest
from store.dolphindb_client import DolphinDBClient


class TestConvertDateFormat:
    def test_yyyymmdd(self):
        assert DolphinDBClient._convert_date_format("20200101") == "2020.01.01"

    def test_non_date_string(self):
        assert DolphinDBClient._convert_date_format("hello") == "hello"

    def test_short_number_string(self):
        assert DolphinDBClient._convert_date_format("12345") == "12345"

    def test_non_string(self):
        assert DolphinDBClient._convert_date_format(12345678) == 12345678


class TestEscapeValue:
    def test_none(self):
        assert DolphinDBClient._escape_value(None) == "NULL"

    def test_bool_true(self):
        assert DolphinDBClient._escape_value(True) == "true"

    def test_bool_false(self):
        assert DolphinDBClient._escape_value(False) == "false"

    def test_int(self):
        assert DolphinDBClient._escape_value(42) == "42"

    def test_float(self):
        assert DolphinDBClient._escape_value(3.14) == "3.14"

    def test_datetime(self):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        assert DolphinDBClient._escape_value(dt) == "2024.01.15T10:30:00"

    def test_date(self):
        d = date(2024, 1, 15)
        assert DolphinDBClient._escape_value(d) == "2024.01.15"

    def test_date_string(self):
        # YYYYMMDD 日期字符串应转换格式并加引号
        assert DolphinDBClient._escape_value("20200101") == '"2020.01.01"'

    def test_plain_string(self):
        assert DolphinDBClient._escape_value("hello") == '"hello"'

    def test_string_with_quotes(self):
        result = DolphinDBClient._escape_value('say "hi"')
        assert result == '"say \\"hi\\""'


class TestSubstituteParams:
    def test_no_params(self):
        sql = "SELECT * FROM t"
        result = DolphinDBClient._substitute_params(
            DolphinDBClient, sql, None
        )
        assert result == sql

    def test_single_param(self):
        sql = "SELECT * FROM t WHERE id = %s"
        result = DolphinDBClient._substitute_params(
            DolphinDBClient, sql, (42,)
        )
        assert result == "SELECT * FROM t WHERE id = 42"

    def test_multiple_params(self):
        sql = "SELECT * FROM t WHERE name = %s AND age = %s"
        result = DolphinDBClient._substitute_params(
            DolphinDBClient, sql, ("test", 25)
        )
        assert result == 'SELECT * FROM t WHERE name = "test" AND age = 25'

    def test_param_count_mismatch(self):
        sql = "SELECT * FROM t WHERE id = %s"
        with pytest.raises(ValueError, match="参数数量不匹配"):
            DolphinDBClient._substitute_params(
                DolphinDBClient, sql, (1, 2)
            )
