#!/usr/bin/env python3
"""
执行 Alphalens 集成 DolphinDB 数据库迁移
创建指数股票池表、指数元数据表、因子分析结果扩展表
"""
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from store.dolphindb_client import db_client
from app.core.logger import logger


def run_migration():
    """执行 DolphinDB 数据库迁移"""
    migration_file = os.path.join(os.path.dirname(__file__), 'migrations', 'add_alphalens_tables.dos')

    logger.info(f"Reading DolphinDB migration script: {migration_file}")

    with open(migration_file, 'r', encoding='utf-8') as f:
        dos_script = f.read()

    logger.info("Executing DolphinDB script...")

    try:
        # DolphinDB 可以一次执行整个脚本
        result = db_client._session.run(dos_script)
        logger.info("✓ Migration script executed successfully")
        return True

    except Exception as e:
        logger.error(f"✗ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def verify_tables():
    """验证表是否创建成功"""
    logger.info("\nVerifying tables...")

    tables_to_check = [
        'index_constituents',
        'index_metadata',
        'factor_analysis_extended'
    ]

    for table in tables_to_check:
        try:
            # 检查表是否存在
            exists = db_client._session.run(f'existsTable("dfs://quant", "{table}")')
            if exists:
                logger.info(f"✓ Table '{table}' exists")

                # 获取表结构
                schema = db_client._session.run(f'schema(loadTable("dfs://quant", "{table}"))')
                col_count = len(schema['colDefs'])
                logger.info(f"  Columns: {col_count} fields")
            else:
                logger.error(f"✗ Table '{table}' not found")
        except Exception as e:
            logger.error(f"✗ Error checking table '{table}': {e}")

    # 验证 factor_data_config 中的配置
    logger.info("\nVerifying factor_data_config entries...")
    try:
        # 检查表是否存在
        exists = db_client._session.run('existsTable("dfs://quant", "factor_data_config")')
        if exists:
            result = db_client.query("""
                SELECT field_key, description, table_name, column_name
                FROM factor_data_config
                WHERE field_key IN ('industry', 'market_cap')
            """)

            if not result.is_empty():
                for row in result.to_dicts():
                    logger.info(f"✓ Config entry: {row['field_key']} - {row['description']}")
            else:
                logger.warning("⚠ No config entries found for industry/market_cap")
        else:
            logger.warning("⚠ Table factor_data_config does not exist yet")
    except Exception as e:
        logger.error(f"✗ Error checking config: {e}")


if __name__ == '__main__':
    logger.info("Starting Alphalens integration DolphinDB migration...")
    logger.info("=" * 60)

    try:
        # 执行迁移
        success = run_migration()

        if success:
            # 验证表
            verify_tables()
            logger.info("\n✓ Migration and verification completed successfully!")
            sys.exit(0)
        else:
            logger.error("\n✗ Migration failed!")
            sys.exit(1)

    except Exception as e:
        logger.error(f"\n✗ Migration error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
