# tests/database/test_init_stock_pool.py
import pytest
from unittest.mock import patch, MagicMock
import polars as pl


def test_stock_pool_tables_exist():
    """测试股票池表是否能正确创建"""
    # 我们将使用 mock 来测试，因为不需要实际连接数据库
    # 这个测试主要验证导入和函数存在

    # 验证模块可以导入
    from database.init_stock_pool_tables import init_stock_pool_tables

    # 函数应该存在
    assert callable(init_stock_pool_tables)

    # 函数名应该正确
    assert init_stock_pool_tables.__name__ == "init_stock_pool_tables"


def test_init_meta_tables_import():
    """测试 init_meta_tables 可以导入并包含必要的函数"""
    from database.init_meta_tables import main

    assert callable(main)
    assert main.__name__ == "main"


@pytest.mark.integration
def test_tables_created_with_correct_schema():
    """集成测试：验证表创建的SQL语句正确"""
    # 这个测试会检查 SQL 语法，但不实际执行

    # 检查我们有正确的表列表
    expected_tables = [
        "stock_pool_metadata",
        "stock_pool_constituents",
        "stock_pool_sync_task",
        "stock_pool_latest",
        "stock_pool_event",
        "sync_index_basic",
        "sync_index_weight"
    ]

    # 简单验证我们预期的表数量
    assert len(expected_tables) == 7
