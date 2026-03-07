"""
DolphinDB 数据库客户端（向后兼容入口）

此文件保持向后兼容，所有功能已重构到 store/dolphindb/ 模块
原有的 `from store.dolphindb_client import db_client` 仍然可用
"""
from store.dolphindb import (
    DolphinDBClient,
    DolphinDBConnection,
    QueryBuilder,
    MetadataManager,
    SeedDataManager,
    DataOperations,
    db_client,
)

__all__ = [
    "DolphinDBClient",
    "DolphinDBConnection",
    "QueryBuilder",
    "MetadataManager",
    "SeedDataManager",
    "DataOperations",
    "db_client",
]
