"""DolphinDB 客户端 SQL 工具方法的单元测试（不依赖数据库连接）"""
from datetime import datetime, date

import pytest
from infrastructure.database.type_converter import TypeConverter
from infrastructure.database.sql_adapter import SQLAdapter


class TestConvertDateFormat:
    def test_yyyymmdd(self):
        assert TypeConverter.convert_date_format("20200101") == "2020.01.01"

    def test_non_date_string(self):
        assert TypeConverter.convert_date_format("hello") == "hello"

    def test_short_number_string(self):
        assert TypeConverter.convert_date_format("12345") == "12345"

    def test_non_string(self):
        assert TypeConverter.convert_date_format(12345678) == 12345678


class TestEscapeValue:
    def test_none(self):
        assert TypeConverter.escape_value(None) == "NULL"

    def test_bool_true(self):
        assert TypeConverter.escape_value(True) == "true"

    def test_bool_false(self):
        assert TypeConverter.escape_value(False) == "false"

    def test_int(self):
        assert TypeConverter.escape_value(42) == "42"

    def test_float(self):
        assert TypeConverter.escape_value(3.14) == "3.14"

    def test_datetime(self):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        assert TypeConverter.escape_value(dt) == "2024.01.15T10:30:00"

    def test_date(self):
        d = date(2024, 1, 15)
        assert TypeConverter.escape_value(d) == "2024.01.15"

    def test_date_string(self):
        # YYYYMMDD 日期字符串应使用 temporalParse 函数
        assert TypeConverter.escape_value("20200101") == 'temporalParse("20200101", "yyyyMMdd")'

    def test_plain_string(self):
        assert TypeConverter.escape_value("hello") == '"hello"'

    def test_string_with_quotes(self):
        result = TypeConverter.escape_value('say "hi"')
        assert result == '"say \\"hi\\""'


class TestSubstituteParams:
    def test_no_params(self):
        sql = "SELECT * FROM t"
        adapter = SQLAdapter("dfs://quant")
        result = adapter.substitute_params(sql, None)
        assert result == sql

    def test_single_param(self):
        sql = "SELECT * FROM t WHERE id = %s"
        adapter = SQLAdapter("dfs://quant")
        result = adapter.substitute_params(sql, (42,))
        assert result == "SELECT * FROM t WHERE id = 42"

    def test_multiple_params(self):
        sql = "SELECT * FROM t WHERE name = %s AND age = %s"
        adapter = SQLAdapter("dfs://quant")
        result = adapter.substitute_params(sql, ("test", 25))
        assert result == 'SELECT * FROM t WHERE name = "test" AND age = 25'

    def test_param_count_mismatch(self):
        sql = "SELECT * FROM t WHERE id = %s"
        adapter = SQLAdapter("dfs://quant")
        with pytest.raises(ValueError, match="参数数量不匹配"):
            adapter.substitute_params(sql, (1, 2))
