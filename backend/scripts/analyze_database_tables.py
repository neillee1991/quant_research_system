"""
分析数据库表并识别可删除的表
"""
import sys
from pathlib import Path

# 添加 backend 目录到 Python 路径
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from typing import Dict, List, Set
from infrastructure.database.dolphindb_client import db_client
from app.core.logger import logger


def get_all_tables() -> List[Dict]:
    """获取数据库中所有表"""
    try:
        tables = db_client.list_tables()
        logger.info(f"数据库中共有 {len(tables)} 张表")
        return tables
    except Exception as e:
        logger.error(f"获取表列表失败: {e}")
        return []


def get_used_tables() -> Set[str]:
    """
    获取项目中实际使用的表列表
    基于代码分析和配置文件
    """
    # 核心元数据表（必须保留）
    core_meta_tables = {
        "factor_metadata",
        "factor_task_run",
        "sync_task_config",
        "etl_task_config",
        "factor_data_config",
        "trade_cal",
        "stock_basic",
        "index_metadata",
        "user_sync_preference",
        "task_runs",
    }

    # TSDB 时序数据表（必须保留）
    tsdb_tables = {
        "sync_daily_data",
        "sync_daily_basic",
        "sync_adj_factor",
        "sync_index_daily",
        "sync_moneyflow",
        "factor_values",
    }

    # 因子分析相关表
    factor_analysis_tables = {
        "factor_analysis",
        "factor_analysis_extended",
    }

    # 指数成分股相关表
    index_tables = {
        "index_constituents",
    }

    # ETL 中间表
    etl_tables = {
        "etl_index_member",
        "etl_index_member_daily",
        "etl_stock_daily_info",
    }

    # 数据同步表（来自 sync_tasks.json）
    sync_tables = {
        "sync_stock_basic",
        "sync_trade_cal",
        "sync_stk_limit",
        "sync_stock_st",
        "sync_index_basic",
        "sync_index_weight",
        "sync_sw_index_member_Y",  # ETL 源表，保留
        "sync_ci_index_member_Y",  # ETL 源表，保留
    }

    # 旧的 DAG 相关表（可能已废弃）
    old_dag_tables = {
        "dag_run_log",
        "dag_task_log",
        "production_task_run",
    }

    # 合并所有使用的表
    used_tables = (
        core_meta_tables
        | tsdb_tables
        | factor_analysis_tables
        | index_tables
        | etl_tables
        | sync_tables
    )

    return used_tables


def analyze_tables():
    """分析表并生成报告"""
    print("=" * 80)
    print("数据库表分析报告")
    print("=" * 80)

    # 获取所有表
    all_tables = get_all_tables()
    if not all_tables:
        print("未找到任何表")
        return

    # 获取使用的表
    used_tables = get_used_tables()

    # 分类
    existing_table_names = {t["table_name"] for t in all_tables}

    # 可删除的表：存在但不在使用列表中
    tables_to_delete = existing_table_names - used_tables

    # 应该存在但缺失的表
    missing_tables = used_tables - existing_table_names

    # 打印报告
    print(f"\n1. 数据库中现有表: {len(existing_table_names)} 张")
    print("-" * 80)
    for table in sorted(all_tables, key=lambda x: x["table_name"]):
        print(f"  - {table['table_name']:40s} ({table['row_count']:>8} 行, {table['column_count']:2} 列)")

    print(f"\n2. 项目中使用的表: {len(used_tables)} 张")
    print("-" * 80)
    for table in sorted(used_tables):
        status = "✓" if table in existing_table_names else "✗ (缺失)"
        print(f"  {status} {table}")

    print(f"\n3. 可以删除的表: {len(tables_to_delete)} 张")
    print("-" * 80)
    if tables_to_delete:
        for table in sorted(tables_to_delete):
            table_info = next((t for t in all_tables if t["table_name"] == table), None)
            if table_info:
                print(f"  - {table:40s} ({table_info['row_count']:>8} 行, {table_info['column_count']:2} 列)")
            else:
                print(f"  - {table}")
    else:
        print("  没有可以删除的表")

    print(f"\n4. 缺失的表: {len(missing_tables)} 张")
    print("-" * 80)
    if missing_tables:
        for table in sorted(missing_tables):
            print(f"  - {table}")
    else:
        print("  没有缺失的表")

    return tables_to_delete, all_tables


def delete_tables(tables_to_delete: Set[str], all_tables: List[Dict], force: bool = False):
    """删除指定的表"""
    if not tables_to_delete:
        print("\n没有需要删除的表")
        return

    print("\n" + "=" * 80)
    print("警告：即将删除以下表")
    print("=" * 80)
    for table in sorted(tables_to_delete):
        table_info = next((t for t in all_tables if t["table_name"] == table), None)
        if table_info:
            print(f"  - {table:40s} ({table_info['row_count']:>8} 行)")

    if not force:
        confirm = input("\n确认删除这些表？(yes/no): ")
        if confirm.lower() != "yes":
            print("操作已取消")
            return
    else:
        print("\n--force 参数指定，跳过确认，直接删除")

    print("\n开始删除表...")
    deleted_count = 0
    failed_count = 0

    for table in sorted(tables_to_delete):
        try:
            print(f"  删除 {table}...", end="")
            db_client.drop_table(table)
            print("  ✓ 成功")
            deleted_count += 1
        except Exception as e:
            print(f"  ✗ 失败: {e}")
            failed_count += 1

    print(f"\n删除完成: 成功 {deleted_count} 张, 失败 {failed_count} 张")


if __name__ == "__main__":
    tables_to_delete, all_tables = analyze_tables()

    if tables_to_delete:
        print("\n" + "=" * 80)
        print("下一步操作")
        print("=" * 80)
        print("1. 查看上面的分析报告")
        print("2. 确认 '可以删除的表' 列表是否正确")
        print("3. 如果确认删除，重新运行脚本并传入 --delete 参数")
        print("\n示例:")
        print("  python scripts/analyze_database_tables.py --delete")

    # 检查是否有 --delete 参数
    if "--delete" in sys.argv:
        force = "--force" in sys.argv
        delete_tables(tables_to_delete, all_tables, force=force)
