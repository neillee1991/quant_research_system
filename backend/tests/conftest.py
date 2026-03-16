"""
Pytest 配置文件
提供共享的 fixtures 和测试配置
"""
import pytest
from unittest.mock import Mock, MagicMock
import polars as pl
import pandas as pd
from datetime import datetime


@pytest.fixture
def mock_db_connection():
    """Mock DolphinDB 连接"""
    conn = Mock()
    conn.session = MagicMock()
    conn.lock = MagicMock()
    conn._ensure_connected = Mock()
    conn.close = Mock()
    return conn


@pytest.fixture
def mock_sql_adapter():
    """Mock SQL 适配器"""
    adapter = Mock()
    adapter.build_sql = Mock(side_effect=lambda sql, params: sql)
    adapter._substitute_params = Mock(side_effect=lambda sql, params: sql)
    adapter._adapt_sql_syntax = Mock(side_effect=lambda sql: sql)
    return adapter


@pytest.fixture
def mock_table_manager():
    """Mock 表管理器"""
    manager = Mock()
    manager._resolve_db_path = Mock(return_value="dfs://quant_ts")
    manager._META_TABLES = ["sync_log", "factor_metadata", "sync_task_config"]
    manager._TSDB_TABLES = ["daily_data", "factor_values"]
    manager.create_table = Mock()
    manager.table_exists = Mock(return_value=True)
    return manager


@pytest.fixture
def mock_data_operations():
    """Mock 数据操作"""
    data_ops = Mock()
    data_ops.query = Mock(return_value=pl.DataFrame())
    data_ops.execute = Mock()
    data_ops.upsert = Mock()
    data_ops.bulk_copy = Mock()
    data_ops.get_last_sync_date = Mock(return_value=None)
    data_ops.update_sync_log = Mock()
    return data_ops


@pytest.fixture
def mock_metadata_manager():
    """Mock 元数据管理器"""
    manager = Mock()
    manager.create_meta_table = Mock()
    manager.create_all_meta_tables = Mock()
    manager.create_task_version = Mock(return_value=1)
    manager.get_task_versions = Mock(return_value=[])
    manager.get_task_version = Mock(return_value=None)
    manager.rollback_task_version = Mock(return_value=True)
    manager.get_current_task_version = Mock(return_value=None)
    return manager


@pytest.fixture
def sample_daily_data():
    """示例日行情数据"""
    return pl.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ", "000001.SZ", "000002.SZ"],
        "trade_date": ["20240101", "20240101", "20240102", "20240102"],
        "open": [10.0, 20.0, 10.5, 20.5],
        "high": [11.0, 21.0, 11.5, 21.5],
        "low": [9.5, 19.5, 10.0, 20.0],
        "close": [10.5, 20.5, 11.0, 21.0],
        "volume": [1000000, 2000000, 1100000, 2100000],
        "amount": [10500000.0, 41000000.0, 12100000.0, 44100000.0],
    })


@pytest.fixture
def sample_factor_data():
    """示例因子数据"""
    return pl.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ", "000001.SZ", "000002.SZ"],
        "trade_date": ["20240101", "20240101", "20240102", "20240102"],
        "factor_value": [1.5, 2.0, 1.6, 2.1],
        "quality_flag": [0, 0, 0, 0],
    })


@pytest.fixture
def sample_status_data():
    """示例股票状态数据"""
    return pl.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": ["20240101", "20240101"],
        "is_st": [False, False],
        "is_limit_up": [False, False],
        "is_limit_down": [False, False],
        "is_new_stock": [False, False],
    })


@pytest.fixture
def sample_adj_factor():
    """示例复权因子数据"""
    return pl.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": ["20240101", "20240101"],
        "adj_factor": [1.0, 1.0],
    })


@pytest.fixture
def mock_factor_definition():
    """Mock 因子定义"""
    definition = Mock()
    definition.factor_id = "test_factor"
    definition.compute_mode = "incremental"
    definition.depends_on = ["daily"]
    definition.lookback_days = 20
    definition.params = {}
    definition.preprocess = None
    definition.func = Mock(return_value=pl.DataFrame({
        "ts_code": ["000001.SZ"],
        "trade_date": ["20240101"],
        "factor_value": [1.5]
    }))
    return definition


@pytest.fixture
def mock_trading_calendar():
    """Mock 交易日历"""
    calendar = Mock()
    calendar.get_latest_trade_date = Mock(return_value="20240105")
    calendar.get_previous_trade_date = Mock(return_value="20240104")
    calendar.offset_trade_date = Mock(return_value="20231201")
    calendar.is_trade_date = Mock(return_value=True)
    calendar.get_trade_dates = Mock(return_value=["20240101", "20240102", "20240103"])
    return calendar


@pytest.fixture
def mock_data_config():
    """Mock 数据配置"""
    config = Mock()
    config.load = Mock(return_value={
        "daily": {
            "table_name": "sync_daily_data",
            "columns": ["ts_code", "trade_date", "open", "high", "low", "close", "volume"],
            "date_column": "trade_date"
        },
        "basic": {
            "table_name": "sync_daily_basic",
            "columns": ["ts_code", "trade_date", "pe", "pb", "turnover_rate"],
            "date_column": "trade_date"
        }
    })
    config.get_table_name = Mock(return_value="sync_daily_data")
    config.get_columns = Mock(return_value=["ts_code", "trade_date", "close"])
    return config


@pytest.fixture
def mock_preprocess_loader():
    """Mock 预处理配置加载器"""
    loader = Mock()
    loader.get_profile = Mock(return_value={
        "adjust_price": "forward",
        "filter_st": True,
        "filter_new_stock": True,
        "new_stock_days": 60,
        "mark_limit": True,
    })
    loader.get_default_profile = Mock(return_value={
        "adjust_price": "forward",
        "filter_st": True,
        "filter_new_stock": True,
        "new_stock_days": 60,
        "mark_limit": True,
    })
    return loader


@pytest.fixture
def mock_pipeline():
    """Mock Pipeline"""
    pipeline = Mock()
    pipeline.add_stage = Mock(return_value=pipeline)
    pipeline.execute = Mock(return_value=pl.DataFrame({
        "ts_code": ["000001.SZ"],
        "trade_date": ["20240101"],
        "factor_value": [1.5]
    }))
    pipeline.get_stages = Mock(return_value=[])
    return pipeline


@pytest.fixture
def mock_pipeline_factory():
    """Mock Pipeline 工厂"""
    factory = Mock()
    factory.create_pipeline = Mock()
    return factory


@pytest.fixture
def process_context(mock_factor_definition):
    """创建 ProcessContext"""
    from infrastructure.processor.pipeline import ProcessContext

    return ProcessContext(
        factor_id="test_factor",
        factor_definition=mock_factor_definition,
        calc_start="20240101",
        calc_end="20240131",
        data_start="20231201",
        preprocess_options={
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
        }
    )


# 测试数据库连接配置
@pytest.fixture(scope="session")
def test_db_config():
    """测试数据库配置"""
    return {
        "host": "localhost",
        "port": 8848,
        "username": "admin",
        "password": "123456",
    }


# 日期相关 fixtures
@pytest.fixture
def date_range():
    """日期范围"""
    return {
        "start": "20240101",
        "end": "20240131",
        "data_start": "20231201",
    }


@pytest.fixture
def trade_dates():
    """交易日列表"""
    return [
        "20240102", "20240103", "20240104", "20240105", "20240108",
        "20240109", "20240110", "20240111", "20240112", "20240115",
    ]


# 清理 fixtures
@pytest.fixture(autouse=True)
def reset_singletons():
    """每个测试后重置单例"""
    yield
    # 在这里可以添加单例重置逻辑
    # 例如: DolphinDBConnection._instance = None


# Pytest 配置
def pytest_configure(config):
    """Pytest 配置"""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"
    )


# 测试收集钩子
def pytest_collection_modifyitems(config, items):
    """修改测试收集"""
    for item in items:
        # 为所有测试添加 unit 标记（如果没有其他标记）
        if not any(mark.name in ["integration", "slow"] for mark in item.iter_markers()):
            item.add_marker(pytest.mark.unit)
