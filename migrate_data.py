#!/usr/bin/env python3
"""
DuckDB 到 PostgreSQL 数据迁移脚本
"""
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent / "backend"))

import duckdb
import polars as pl
from store.postgres_client import db_client
from app.core.logger import logger


def migrate_table(duckdb_conn, table_name: str, primary_keys: list):
    """迁移单个表"""
    try:
        logger.info(f"开始迁移表: {table_name}")

        # 从 DuckDB 读取数据
        df = duckdb_conn.execute(f"SELECT * FROM {table_name}").pl()

        if df.is_empty():
            logger.warning(f"表 {table_name} 为空，跳过")
            return 0

        # 写入 PostgreSQL
        db_client.upsert(table_name, df, primary_keys)

        logger.info(f"✓ 成功迁移 {table_name}: {len(df)} 行")
        return len(df)

    except Exception as e:
        logger.error(f"✗ 迁移 {table_name} 失败: {e}")
        return 0


def main():
    """主函数"""
    # DuckDB 数据库路径
    duckdb_path = Path(__file__).parent / "data" / "quant.duckdb"

    if not duckdb_path.exists():
        logger.error(f"DuckDB 数据库不存在: {duckdb_path}")
        logger.info("如果这是新安装，无需迁移数据")
        return

    logger.info("=" * 60)
    logger.info("  DuckDB → PostgreSQL 数据迁移")
    logger.info("=" * 60)
    logger.info("")

    # 连接到 DuckDB
    logger.info(f"连接到 DuckDB: {duckdb_path}")
    duckdb_conn = duckdb.connect(str(duckdb_path), read_only=True)

    # 获取所有表
    tables_df = duckdb_conn.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
    """).pl()

    all_tables = tables_df["table_name"].to_list()
    logger.info(f"发现 {len(all_tables)} 个表: {', '.join(all_tables)}")
    logger.info("")

    # 定义表和主键映射
    table_configs = {
        "sync_log": ["source", "data_type"],
        "stock_basic": ["ts_code"],
        "daily_basic": ["ts_code", "trade_date"],
        "adj_factor": ["ts_code", "trade_date"],
        "index_daily": ["ts_code", "trade_date"],
        "moneyflow": ["ts_code", "trade_date"],
        "daily_data": ["trade_date", "ts_code"],
    }

    # 迁移每个表
    total_rows = 0
    migrated_tables = 0

    for table_name in all_tables:
        if table_name in table_configs:
            primary_keys = table_configs[table_name]
            rows = migrate_table(duckdb_conn, table_name, primary_keys)
            if rows > 0:
                total_rows += rows
                migrated_tables += 1
        else:
            logger.warning(f"跳过未知表: {table_name}")

    # 关闭连接
    duckdb_conn.close()

    # 总结
    logger.info("")
    logger.info("=" * 60)
    logger.info("  迁移完成")
    logger.info("=" * 60)
    logger.info(f"✓ 成功迁移 {migrated_tables} 个表")
    logger.info(f"✓ 总计 {total_rows:,} 行数据")
    logger.info("")
    logger.info("💡 下一步:")
    logger.info("   1. 验证数据: 访问 http://localhost:5050 (pgAdmin)")
    logger.info("   2. 启动后端: cd backend && python main.py")
    logger.info("")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n迁移已取消")
        sys.exit(1)
    except Exception as e:
        logger.error(f"迁移失败: {e}")
        sys.exit(1)
