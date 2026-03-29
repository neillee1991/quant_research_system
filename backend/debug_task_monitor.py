#!/usr/bin/env python3
"""
调试任务监控功能
检查数据库表结构和数据
"""
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from store.dolphindb_client import db_client
from app.core.logger import logger

def check_table_structure():
    """检查表结构"""
    print("=" * 60)
    print("检查数据库表结构")
    print("=" * 60)

    tables = [
        "factor_run_log",
        "sync_log_history",
        "factor_analysis_extended"
    ]

    for table_name in tables:
        print(f"\n--- 表: {table_name} ---")
        try:
            if db_client.table_exists(table_name):
                columns = db_client.get_table_columns(table_name)
                print(f"  列: {columns}")

                # 查询最近的几条记录
                df = db_client.query(f"SELECT * FROM {table_name} ORDER BY created_at DESC LIMIT 5")
                print(f"  记录数: {len(df)}")
                if not df.is_empty():
                    print(f"  最近记录:")
                    for row in df.to_dicts():
                        print(f"    {row}")
            else:
                print(f"  表不存在")
        except Exception as e:
            print(f"  错误: {e}")
            import traceback
            traceback.print_exc()

def check_running_tasks():
    """检查正在运行的任务"""
    print("\n" + "=" * 60)
    print("检查正在运行的任务")
    print("=" * 60)

    # 1. 检查 factor_run_log
    print("\n1. factor_run_log (pending/running):")
    try:
        df = db_client.query("""
            SELECT * FROM factor_run_log
            WHERE status IN ('pending', 'running')
            ORDER BY created_at DESC
        """)
        print(f"   找到 {len(df)} 条记录")
        if not df.is_empty():
            for row in df.to_dicts():
                print(f"   - {row}")
    except Exception as e:
        print(f"   错误: {e}")

    # 2. 检查 sync_log_history
    print("\n2. sync_log_history (pending/running):")
    try:
        df = db_client.query("""
            SELECT * FROM sync_log_history
            WHERE status IN ('pending', 'running')
            ORDER BY created_at DESC
        """)
        print(f"   找到 {len(df)} 条记录")
        if not df.is_empty():
            for row in df.to_dicts():
                print(f"   - {row}")
    except Exception as e:
        print(f"   错误: {e}")

    # 3. 检查 factor_analysis_extended
    print("\n3. factor_analysis_extended (pending/running):")
    try:
        df = db_client.query("""
            SELECT * FROM factor_analysis_extended
            WHERE task_status IN ('pending', 'running')
            ORDER BY created_at DESC
        """)
        print(f"   找到 {len(df)} 条记录")
        if not df.is_empty():
            for row in df.to_dicts():
                print(f"   - {row}")
    except Exception as e:
        print(f"   错误: {e}")

def insert_test_data():
    """插入测试数据"""
    print("\n" + "=" * 60)
    print("插入测试数据")
    print("=" * 60)

    from datetime import datetime
    import polars as pl

    # 1. 插入测试同步任务
    print("\n1. 插入测试同步任务...")
    try:
        db_client.bulk_copy("sync_log_history", pl.DataFrame({
            "source": ["tushare_config"],
            "data_type": ["test_sync_task"],
            "last_date": ["20240101"],
            "sync_date": ["20240101"],
            "rows_synced": [0],
            "status": ["running"],
            "error_message": [""],
            "params": ["{}"],
            "created_at": [datetime.now()]
        }))
        print("   成功")
    except Exception as e:
        print(f"   错误: {e}")
        import traceback
        traceback.print_exc()

    # 2. 插入测试ETL任务
    print("\n2. 插入测试ETL任务...")
    try:
        db_client.bulk_copy("sync_log_history", pl.DataFrame({
            "source": ["etl"],
            "data_type": ["test_etl_task"],
            "last_date": ["20240101"],
            "sync_date": ["20240101"],
            "rows_synced": [0],
            "status": ["running"],
            "error_message": [""],
            "params": ["{}"],
            "created_at": [datetime.now()]
        }))
        print("   成功")
    except Exception as e:
        print(f"   错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("任务监控调试工具\n")

    import argparse
    parser = argparse.ArgumentParser(description="调试任务监控功能")
    parser.add_argument("--insert-test", action="store_true", help="插入测试数据")
    args = parser.parse_args()

    check_table_structure()

    if args.insert_test:
        insert_test_data()

    check_running_tasks()

    print("\n完成!")