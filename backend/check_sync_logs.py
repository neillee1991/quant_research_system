#!/usr/bin/env python3
"""检查同步日志数据"""
import sys
sys.path.insert(0, '/Users/lisheng/Code/quantsystem/quant_research_system/backend')

from store.dolphindb_client import db_client

# 检查 sync_log_history 表
print("=== 检查 sync_log_history 表 ===")
try:
    df = db_client.query("SELECT * FROM sync_log_history ORDER BY created_at DESC LIMIT 10")
    print(f"记录数: {len(df)}")
    if not df.is_empty():
        print("\n最近10条记录:")
        print(df)
    else:
        print("表为空，没有数据")
except Exception as e:
    print(f"查询失败: {e}")

# 检查 sync_log 表
print("\n=== 检查 sync_log 表 ===")
try:
    df = db_client.query("SELECT * FROM sync_log ORDER BY updated_at DESC LIMIT 10")
    print(f"记录数: {len(df)}")
    if not df.is_empty():
        print("\n最近10条记录:")
        print(df)
    else:
        print("表为空，没有数据")
except Exception as e:
    print(f"查询失败: {e}")

# 检查表是否存在
print("\n=== 检查表是否存在 ===")
print(f"sync_log_history 存在: {db_client.table_exists('sync_log_history')}")
print(f"sync_log 存在: {db_client.table_exists('sync_log')}")
