#!/usr/bin/env python3
"""
数据库初始化脚本
检查并创建所有必需的数据库表
"""
import sys
import os
from pathlib import Path

# 添加项目路径
backend_dir = Path(__file__).parent
sys.path.insert(0, str(backend_dir))

# 检查是否在虚拟环境中
if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
    print("警告: 未检测到虚拟环境")
    print("请先激活虚拟环境:")
    print("  source .venv/bin/activate  # 或 source venv/bin/activate")
    print()

try:
    from store.postgres_client import db_client
    from app.core.logger import logger
    from data_manager.sync_components import SyncConfigManager, TableManager
except ImportError as e:
    print(f"错误: 无法导入必需的模块: {e}")
    print("\n请确保:")
    print("  1. 已激活虚拟环境")
    print("  2. 已安装依赖: pip install -r requirements.txt")
    sys.exit(1)


def check_database_connection():
    """检查数据库连接"""
    try:
        df = db_client.query("SELECT 1 as test")
        logger.info("✓ 数据库连接成功")
        return True
    except Exception as e:
        logger.error(f"✗ 数据库连接失败: {e}")
        return False


def get_existing_tables():
    """获取现有表列表"""
    try:
        df = db_client.query("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = df['table_name'].to_list() if not df.is_empty() else []
        return tables
    except Exception as e:
        logger.error(f"获取表列表失败: {e}")
        return []


def init_sync_log_table():
    """初始化同步日志表"""
    # 1. 创建 sync_log 表
    table_name = "sync_log"

    if not db_client.table_exists(table_name):
        try:
            schema = {
                "source": {"type": "VARCHAR(50)", "nullable": False},
                "data_type": {"type": "VARCHAR(50)", "nullable": False},
                "last_sync_date": {"type": "VARCHAR(20)", "nullable": True},
                "last_sync_time": {"type": "TIMESTAMP", "nullable": True},
                "status": {"type": "VARCHAR(20)", "nullable": True},
                "message": {"type": "TEXT", "nullable": True}
            }

            db_client.create_table(table_name, schema, ["source", "data_type"])
            logger.info(f"✓ 创建表 {table_name} 成功")
        except Exception as e:
            logger.error(f"✗ 创建表 {table_name} 失败: {e}")
            return False
    else:
        logger.info(f"✓ 表 {table_name} 已存在")

    # 2. 创建 sync_log_history 表
    history_table = "sync_log_history"

    if not db_client.table_exists(history_table):
        try:
            schema = {
                "id": {"type": "SERIAL", "nullable": False},
                "source": {"type": "VARCHAR(100)", "nullable": False},
                "data_type": {"type": "VARCHAR(100)", "nullable": False},
                "last_date": {"type": "VARCHAR(8)", "nullable": True},
                "sync_date": {"type": "VARCHAR(8)", "nullable": True},
                "rows_synced": {"type": "INTEGER", "nullable": True},
                "status": {"type": "VARCHAR(20)", "nullable": True},
                "created_at": {"type": "TIMESTAMP", "nullable": True}
            }

            db_client.create_table(history_table, schema, ["id"])
            logger.info(f"✓ 创建表 {history_table} 成功")
        except Exception as e:
            logger.error(f"✗ 创建表 {history_table} 失败: {e}")
            return False
    else:
        logger.info(f"✓ 表 {history_table} 已存在")

    return True


def init_sync_config_tables():
    """根据同步配置初始化所有数据表"""
    try:
        # 加载同步配置
        config_manager = SyncConfigManager()
        tasks = config_manager.get_all_tasks()

        if not tasks:
            logger.warning("未找到同步任务配置")
            return True

        table_manager = TableManager(db_client)
        created_count = 0
        skipped_count = 0
        failed_count = 0

        for task in tasks:
            table_name = task.get("table_name")
            if not table_name:
                continue

            # 检查表是否存在
            if db_client.table_exists(table_name):
                logger.info(f"✓ 表 {table_name} 已存在")
                skipped_count += 1
                continue

            # 创建表
            try:
                schema = task.get("schema", {})
                primary_keys = task.get("primary_keys", [])

                if not schema:
                    logger.warning(f"⚠ 任务 {task.get('task_id')} 没有定义 schema，跳过")
                    continue

                table_manager.create_table_if_not_exists(table_name, schema, primary_keys)
                logger.info(f"✓ 创建表 {table_name} 成功")
                created_count += 1
            except Exception as e:
                logger.error(f"✗ 创建表 {table_name} 失败: {e}")
                failed_count += 1

        logger.info(f"\n数据表初始化完成:")
        logger.info(f"  - 新建: {created_count} 个")
        logger.info(f"  - 已存在: {skipped_count} 个")
        logger.info(f"  - 失败: {failed_count} 个")

        return failed_count == 0

    except Exception as e:
        logger.error(f"初始化同步配置表失败: {e}")
        return False


def main():
    """主函数"""
    print("=" * 60)
    print("  数据库初始化检查")
    print("=" * 60)
    print()

    # 1. 检查数据库连接
    print("1. 检查数据库连接...")
    if not check_database_connection():
        print("\n✗ 数据库连接失败，请检查:")
        print("  - PostgreSQL 是否运行: docker ps | grep quant_postgres")
        print("  - 配置是否正确: backend/.env")
        print("  - 启动数据库: docker-compose up -d")
        sys.exit(1)
    print()

    # 2. 显示现有表
    print("2. 检查现有表...")
    existing_tables = get_existing_tables()
    if existing_tables:
        print(f"   找到 {len(existing_tables)} 个表:")
        for table in existing_tables:
            print(f"   - {table}")
    else:
        print("   未找到任何表，将创建所有表")
    print()

    # 3. 初始化同步日志表
    print("3. 初始化同步日志表...")
    if not init_sync_log_table():
        print("\n✗ 同步日志表初始化失败")
        sys.exit(1)
    print()

    # 4. 初始化同步配置表
    print("4. 根据同步配置初始化数据表...")
    if not init_sync_config_tables():
        print("\n⚠ 部分表初始化失败，请检查日志")
    print()

    # 5. 显示最终表列表
    print("5. 最终表列表:")
    final_tables = get_existing_tables()
    if final_tables:
        print(f"   共 {len(final_tables)} 个表:")
        for table in final_tables:
            # 获取行数
            try:
                count_df = db_client.query(f"SELECT COUNT(*) as count FROM {table}")
                count = count_df['count'][0] if not count_df.is_empty() else 0
                print(f"   - {table:<30} ({count:>10,} 行)")
            except:
                print(f"   - {table}")
    print()

    print("=" * 60)
    print("  数据库初始化完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
