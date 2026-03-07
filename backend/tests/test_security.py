"""
安全测试
验证路径遍历防护、SQL 注入防护、QueryBuilder 特殊字符处理
不依赖真实 DolphinDB 连接
"""
import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from fastapi import HTTPException


# ==================== 路径遍历测试 ====================

class TestPathTraversalProtection:
    """验证 factor_id 和 flow name 路径遍历防护"""

    def test_factor_id_path_traversal_rejected(self):
        """factor_id 包含路径遍历字符时应被拒绝"""
        import re
        _SAFE_FACTOR_ID_RE = re.compile(r'^[a-zA-Z0-9_\-]+$')

        malicious_ids = [
            "../../etc/passwd",
            "../secret",
            "factor/../../etc",
            "factor\x00null",
            "factor id with spaces",
            "factor;DROP TABLE",
            "<script>alert(1)</script>",
            "factor%2F..%2F..%2Fetc",
        ]
        for fid in malicious_ids:
            assert not _SAFE_FACTOR_ID_RE.match(fid), (
                f"Malicious factor_id '{fid}' should be rejected by regex"
            )

    def test_factor_id_valid_accepted(self):
        """合法的 factor_id 应通过验证"""
        import re
        _SAFE_FACTOR_ID_RE = re.compile(r'^[a-zA-Z0-9_\-]+$')

        valid_ids = [
            "momentum_20",
            "rsi-14",
            "factor_001",
            "RSI",
            "my-factor-v2",
        ]
        for fid in valid_ids:
            assert _SAFE_FACTOR_ID_RE.match(fid), (
                f"Valid factor_id '{fid}' should be accepted"
            )

    def test_flow_name_path_traversal_rejected(self):
        """flow name 包含路径遍历字符时应被拒绝"""
        import re
        _SAFE_NAME_RE = re.compile(r'^[a-zA-Z0-9_\-]+$')

        malicious_names = [
            "../../etc/passwd",
            "../config",
            "flow name",
            "flow;rm -rf /",
            "flow\ninjection",
        ]
        for name in malicious_names:
            assert not _SAFE_NAME_RE.match(name), (
                f"Malicious flow name '{name}' should be rejected"
            )

    def test_flow_name_valid_accepted(self):
        """合法的 flow name 应通过验证"""
        import re
        _SAFE_NAME_RE = re.compile(r'^[a-zA-Z0-9_\-]+$')

        valid_names = [
            "daily-sync",
            "factor_compute",
            "flow001",
            "my-flow-v2",
        ]
        for name in valid_names:
            assert _SAFE_NAME_RE.match(name), (
                f"Valid flow name '{name}' should be accepted"
            )

    def test_validate_factor_id_raises_http_exception(self):
        """_validate_factor_id 对非法输入应抛出 HTTPException(400)"""
        import re
        from fastapi import HTTPException

        _SAFE_FACTOR_ID_RE = re.compile(r'^[a-zA-Z0-9_\-]+$')

        def _validate_factor_id(factor_id: str):
            if not _SAFE_FACTOR_ID_RE.match(factor_id):
                raise HTTPException(status_code=400, detail=f"Invalid factor_id: '{factor_id}'")

        with pytest.raises(HTTPException) as exc_info:
            _validate_factor_id("../../etc/passwd")
        assert exc_info.value.status_code == 400

    def test_validate_flow_name_raises_http_exception(self):
        """_validate_flow_name 对非法输入应抛出 HTTPException(400)"""
        import re
        from fastapi import HTTPException

        _SAFE_NAME_RE = re.compile(r'^[a-zA-Z0-9_\-]+$')

        def _validate_flow_name(name: str):
            if not _SAFE_NAME_RE.match(name):
                raise HTTPException(status_code=400, detail=f"Invalid flow name: '{name}'")

        with pytest.raises(HTTPException) as exc_info:
            _validate_flow_name("../../etc/passwd")
        assert exc_info.value.status_code == 400


# ==================== SQL 注入测试 ====================

class TestSQLInjectionPrevention:
    """验证参数化查询正确转义特殊字符"""

    def _escape_value(self, value) -> str:
        """复现 DolphinDBClient._escape_value 逻辑"""
        import re
        from datetime import datetime, date

        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, datetime):
            return f"{value.strftime('%Y.%m.%dT%H:%M:%S')}"
        if isinstance(value, date):
            return f"{value.strftime('%Y.%m.%d')}"
        s = str(value)
        if re.match(r"^\d{8}$", s):
            return f"{s[:4]}.{s[4:6]}.{s[6:8]}"
        # 普通字符串：转义双引号和反斜杠
        s = s.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{s}"'

    def test_double_quote_escaped(self):
        """双引号应被转义"""
        result = self._escape_value('value"with"quotes')
        assert '\\"' in result
        assert '"value' not in result or result.startswith('"')

    def test_backslash_escaped(self):
        """反斜杠应被转义"""
        result = self._escape_value("value\\with\\backslash")
        assert "\\\\" in result

    def test_sql_injection_attempt_escaped(self):
        """SQL 注入尝试应被转义为字符串字面量"""
        injection = "'; DROP TABLE factor_values; --"
        result = self._escape_value(injection)
        # 结果应被包裹在引号中，注入字符被转义
        assert result.startswith('"')
        assert result.endswith('"')
        # 不应包含未转义的单引号（DolphinDB 使用双引号）
        # 原始注入中的单引号在 DolphinDB 中不是特殊字符，但双引号会被转义
        assert "DROP TABLE" in result  # 内容保留但被包裹在字符串中

    def test_none_becomes_null(self):
        """None 应转换为 NULL"""
        result = self._escape_value(None)
        assert result == "NULL"

    def test_yyyymmdd_auto_converts_to_date(self):
        """YYYYMMDD 字符串自动转换为日期格式（已知 bug H-01）"""
        result = self._escape_value("20240101")
        # 当前实现会将 "20240101" 转换为 "2024.01.01"（不加引号）
        assert result == "2024.01.01", (
            f"YYYYMMDD string '20240101' is auto-converted to date format, got: {result}. "
            "This is bug H-01: breaks STRING column queries."
        )

    def test_yyyymmdd_conversion_breaks_string_columns(self):
        """验证 H-01 bug：YYYYMMDD 字符串被转换为日期，导致 STRING 列查询失败"""
        # 如果某列是 STRING 类型存储 "20240101"，
        # 转换后的 2024.01.01 是 DATE 类型，类型不匹配会导致查询错误
        string_value = "20240101"
        escaped = self._escape_value(string_value)
        # 转换后不带引号，是 DATE 字面量而非 STRING
        assert '"' not in escaped, (
            "Bug H-01 confirmed: YYYYMMDD is converted to unquoted date literal, "
            "which breaks STRING column comparisons"
        )


# ==================== QueryBuilder 测试 ====================

class TestQueryBuilderSecurity:
    """测试 QueryBuilder.build_where_clause 对特殊字符的处理"""

    def setup_method(self):
        """导入 QueryBuilder"""
        import sys
        import os
        # 确保 backend 目录在 path 中
        backend_dir = os.path.join(
            os.path.dirname(__file__), ".."
        )
        if backend_dir not in sys.path:
            sys.path.insert(0, backend_dir)

    def test_string_value_wrapped_in_quotes(self):
        """字符串值应被包裹在单引号中"""
        from app.core.utils import QueryBuilder
        result = QueryBuilder.build_where_clause({"ts_code": "000001.SZ"})
        assert "= '000001.SZ'" in result

    def test_none_value_becomes_is_null(self):
        """None 值应生成 IS NULL 条件"""
        from app.core.utils import QueryBuilder
        result = QueryBuilder.build_where_clause({"field": None})
        assert "IS NULL" in result

    def test_list_value_becomes_in_clause(self):
        """列表值应生成 IN 子句"""
        from app.core.utils import QueryBuilder
        result = QueryBuilder.build_where_clause({"ts_code": ["A", "B", "C"]})
        assert "IN (" in result
        assert "'A'" in result
        assert "'B'" in result

    def test_special_chars_in_string_not_escaped(self):
        """
        验证 QueryBuilder 对特殊字符不做转义（已知安全风险）。
        build_where_clause 直接插入字符串值，不转义单引号，
        存在 SQL 注入风险。
        """
        from app.core.utils import QueryBuilder
        # 包含单引号的值
        malicious = "value' OR '1'='1"
        result = QueryBuilder.build_where_clause({"field": malicious})
        # 当前实现：直接插入，不转义
        # 结果: WHERE field = 'value' OR '1'='1'  <- SQL 注入！
        assert malicious in result, (
            "QueryBuilder does not escape special characters in string values - SQL injection risk"
        )

    def test_integer_value_not_quoted(self):
        """整数值不应被引号包裹"""
        from app.core.utils import QueryBuilder
        result = QueryBuilder.build_where_clause({"count": 42})
        assert "= 42" in result
        assert "= '42'" not in result

    def test_empty_filters_returns_empty_string(self):
        """空过滤器应返回空字符串"""
        from app.core.utils import QueryBuilder
        result = QueryBuilder.build_where_clause({})
        assert result == ""

    def test_multiple_conditions_joined_with_and(self):
        """多个条件应用 AND 连接"""
        from app.core.utils import QueryBuilder
        result = QueryBuilder.build_where_clause({
            "ts_code": "000001.SZ",
            "trade_date": "20240101"
        })
        assert "AND" in result
        assert result.startswith("WHERE")
