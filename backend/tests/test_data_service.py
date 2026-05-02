"""
DataService 单元测试
验证 get_daily_data 的 end_date 过滤、日期范围过滤等逻辑
使用 mock 替代真实 DolphinDB 连接
"""
import pytest
import polars as pl
from unittest.mock import MagicMock, patch
from datetime import datetime


# ==================== Fixtures ====================

@pytest.fixture
def mock_repository():
    """模拟数据仓库"""
    repo = MagicMock()
    return repo


@pytest.fixture
def data_service(mock_repository):
    """创建 DataService 实例（使用 mock repository）"""
    import sys
    import os
    backend_dir = os.path.join(os.path.dirname(__file__), "..")
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
    from app.services.data_service import DataService
    return DataService(mock_repository)


@pytest.fixture
def sample_daily_data():
    """模拟日线数据"""
    return pl.DataFrame({
        "ts_code": ["000001.SZ"] * 5,
        "trade_date": ["20240101", "20240102", "20240103", "20240104", "20240105"],
        "open": [10.0, 10.1, 10.2, 10.3, 10.4],
        "high": [10.5, 10.6, 10.7, 10.8, 10.9],
        "low": [9.5, 9.6, 9.7, 9.8, 9.9],
        "close": [10.2, 10.3, 10.4, 10.5, 10.6],
        "vol": [1000.0, 1100.0, 1200.0, 1300.0, 1400.0],
        "amount": [10200.0, 11330.0, 12480.0, 13650.0, 14840.0],
        "pct_chg": [0.5, 1.0, 0.97, 0.96, 0.95],
    })


# ==================== get_daily_data 测试 ====================

class TestGetDailyData:
    def test_end_date_included_in_query(self, data_service, mock_repository, sample_daily_data):
        """验证 end_date 被包含在查询条件中"""
        mock_repository.query.return_value = sample_daily_data

        data_service.get_daily_data(
            ts_code="000001.SZ",
            start_date="20240101",
            end_date="20240105"
        )

        # 验证 query 被调用
        mock_repository.query.assert_called_once()
        call_args = mock_repository.query.call_args

        # 第一个参数是 SQL 字符串
        sql = call_args[0][0]
        assert "trade_date <= %s" in sql, (
            f"end_date condition 'trade_date <= %s' not found in SQL: {sql}"
        )

    def test_end_date_param_passed_to_query(self, data_service, mock_repository, sample_daily_data):
        """验证 end_date 值被传递给查询参数"""
        mock_repository.query.return_value = sample_daily_data

        data_service.get_daily_data(
            ts_code="000001.SZ",
            start_date="20240101",
            end_date="20240105"
        )

        call_args = mock_repository.query.call_args
        params = call_args[0][1]  # 第二个位置参数是 params tuple
        assert "20240105" in params, (
            f"end_date '20240105' not found in query params: {params}"
        )

    def test_start_date_only_no_end_date(self, data_service, mock_repository, sample_daily_data):
        """只传 start_date 时，不应有 end_date 条件"""
        mock_repository.query.return_value = sample_daily_data

        data_service.get_daily_data(
            ts_code="000001.SZ",
            start_date="20240101"
        )

        call_args = mock_repository.query.call_args
        sql = call_args[0][0]
        assert "trade_date <= %s" not in sql

    def test_both_dates_filter_correctly(self, data_service, mock_repository, sample_daily_data):
        """同时传 start_date 和 end_date 时，两个条件都应存在"""
        mock_repository.query.return_value = sample_daily_data

        data_service.get_daily_data(
            ts_code="000001.SZ",
            start_date="20240101",
            end_date="20240103"
        )

        call_args = mock_repository.query.call_args
        sql = call_args[0][0]
        params = call_args[0][1]

        assert "trade_date >= %s" in sql
        assert "trade_date <= %s" in sql
        assert "20240101" in params
        assert "20240103" in params

    def test_no_dates_returns_all_data(self, data_service, mock_repository, sample_daily_data):
        """不传日期时，不应有日期过滤条件"""
        mock_repository.query.return_value = sample_daily_data

        data_service.get_daily_data(ts_code="000001.SZ")

        call_args = mock_repository.query.call_args
        sql = call_args[0][0]
        assert "trade_date >= %s" not in sql
        assert "trade_date <= %s" not in sql

    def test_invalid_start_date_raises_error(self, data_service, mock_repository):
        """无效日期格式应抛出异常"""
        from app.core.exceptions import DataValidationError
        with pytest.raises(DataValidationError):
            data_service.get_daily_data(start_date="2024-13-01")  # 无效月份

    def test_invalid_end_date_raises_error(self, data_service, mock_repository):
        """无效 end_date 格式应抛出异常"""
        from app.core.exceptions import DataValidationError
        with pytest.raises(DataValidationError):
            data_service.get_daily_data(end_date="not-a-date")

    def test_limit_applied_to_query(self, data_service, mock_repository, sample_daily_data):
        """limit 参数应被应用到 SQL 查询"""
        mock_repository.query.return_value = sample_daily_data

        data_service.get_daily_data(ts_code="000001.SZ", limit=100)

        call_args = mock_repository.query.call_args
        sql = call_args[0][0]
        assert "LIMIT" in sql.upper()

    def test_empty_result_raises_not_found(self, data_service, mock_repository):
        """空结果应抛出 DataNotFoundError"""
        from app.core.exceptions import DataNotFoundError
        mock_repository.query.return_value = pl.DataFrame()

        with pytest.raises(DataNotFoundError):
            data_service.get_daily_data(ts_code="NONEXISTENT.SZ")


# ==================== 日期范围过滤测试 ====================

class TestDateRangeFiltering:
    def test_date_range_sql_construction(self, data_service, mock_repository, sample_daily_data):
        """验证日期范围 SQL 构造正确"""
        mock_repository.query.return_value = sample_daily_data

        data_service.get_daily_data(
            start_date="20240101",
            end_date="20240131"
        )

        call_args = mock_repository.query.call_args
        sql = call_args[0][0]
        params = call_args[0][1]

        # SQL 应包含两个日期条件
        assert sql.count("%s") >= 2
        assert "20240101" in params
        assert "20240131" in params

    def test_ts_code_filter_combined_with_dates(self, data_service, mock_repository, sample_daily_data):
        """ts_code 过滤应与日期过滤组合"""
        mock_repository.query.return_value = sample_daily_data

        data_service.get_daily_data(
            ts_code="000001.SZ",
            start_date="20240101",
            end_date="20240131"
        )

        call_args = mock_repository.query.call_args
        sql = call_args[0][0]
        params = call_args[0][1]

        assert "ts_code = %s" in sql
        assert "000001.SZ" in params
        assert "20240101" in params
        assert "20240131" in params

    def test_date_validation_yyyymmdd_format(self, data_service, mock_repository):
        """日期应为 YYYYMMDD 格式"""
        from app.core.exceptions import DataValidationError

        # 有效格式
        mock_repository.query.return_value = pl.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "open": [10.0], "high": [10.5], "low": [9.5],
            "close": [10.2], "vol": [1000.0], "amount": [10200.0], "pct_chg": [0.5]
        })
        # 不应抛出异常
        data_service.get_daily_data(start_date="20240101")

        # 无效格式应抛出异常
        with pytest.raises(DataValidationError):
            data_service.get_daily_data(start_date="2024/01/01")
