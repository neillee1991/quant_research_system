#!/usr/bin/env python3
"""
执行 Alphalens 集成数据库迁移
创建指数股票池表、指数元数据表、因子分析结果扩展表
"""
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from store.dolphindb_client import db_client
from app.core.logger import logger


def run_migration():
    """执行数据库迁移"""
    migration_file = os.path.join(os.path.dirname(__file__), 'migrations', 'add_alphalens_tables.sql')

    logger.info(f"Reading migration file: {migration_file}")

    with open(migration_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    # 分割 SQL 语句（按分号分割，忽略注释）
    statements = []
    current_statement = []

    for line in sql_content.split('\n'):
        # 跳过纯注释行
        stripped = line.strip()
        if stripped.startswith('--') or not stripped:
            continue

        current_statement.append(line)

        # 如果行以分号结尾，表示一个完整的语句
        if stripped.endswith(';'):
            stmt = '\n'.join(current_statement)
            statements.append(stmt)
            current_statement = []

    # 执行每个语句
    success_count = 0
    error_count = 0

    for i, stmt in enumerate(statements, 1):
        # 跳过 SHOW 和 DESCRIBE 语句（仅用于手动验证）
        if stmt.strip().upper().startswith(('SHOW', 'DESCRIBE')):
            logger.info(f"Skipping verification statement {i}")
            continue

        try:
            logger.info(f"Executing statement {i}/{len(statements)}...")
            logger.debug(f"SQL: {stmt[:100]}...")

            db_client.execute(stmt)
            success_count += 1
            logger.info(f"✓ Statement {i} executed successfully")

        except Exception as e:
            error_count += 1
            error_msg = str(e)

            # 如果是表已存在的错误，视为成功
            if 'already exists' in error_msg.lower() or 'duplicate' in error_msg.lower():
                logger.warning(f"⚠ Statement {i}: Table already exists (skipped)")
                success_count += 1
                error_count -= 1
            else:
                logger.error(f"✗ Statement {i} failed: {error_msg}")

    # 汇总结果
    logger.info("=" * 60)
    logger.info(f"Migration completed: {success_count} succeeded, {error_count} failed")
    logger.info("=" * 60)

    if error_count > 0:
        logger.error("Migration completed with errors!")
        return False
    else:
        logger.info("Migration completed successfully!")
        return True


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
            result = db_client.query(f"SHOW TABLES LIKE '{table}'")
            if not result.is_empty():
                logger.info(f"✓ Table '{table}' exists")

                # 显示表结构
                desc = db_client.query(f"DESCRIBE {table}")
                logger.info(f"  Columns: {len(desc)} fields")
            else:
                logger.error(f"✗ Table '{table}' not found")
        except Exception as e:
            logger.error(f"✗ Error checking table '{table}': {e}")

    # 验证 factor_data_config 中的配置
    logger.info("\nVerifying factor_data_config entries...")
    try:
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
    except Exception as e:
        logger.error(f"✗ Error checking config: {e}")


if __name__ == '__main__':
    logger.info("Starting Alphalens integration database migration...")
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
