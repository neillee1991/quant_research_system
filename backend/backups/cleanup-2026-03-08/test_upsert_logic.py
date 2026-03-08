#!/usr/bin/env python3
"""测试新的 upsert 逻辑"""
import sys
sys.path.insert(0, '/Users/lisheng/Code/quantsystem/quant_research_system/backend')

import polars as pl
from store.dolphindb_client import db_client

print("=== 测试新的 upsert 逻辑 ===\n")

# 测试表名
test_table = "test_upsert_logic"

# 1. 创建测试表
print("1. 创建测试表...")
try:
    if db_client.table_exists(test_table):
        try:
            db_client.execute(f'dropTable(database("dfs://quant_meta"), "{test_table}")')
            print(f"   已删除旧表 {test_table}")
        except Exception:
            pass  # 表不存在，忽略错误

    schema = {
        "ts_code": {"type": "STRING", "nullable": False},
        "trade_date": {"type": "DATE", "nullable": False},
        "value": {"type": "DOUBLE", "nullable": True},
    }
    db_client.create_table(test_table, schema, ["ts_code", "trade_date"])
    print(f"   ✓ 创建表 {test_table} 成功\n")
except Exception as e:
    print(f"   ✗ 创建表失败: {e}\n")
    sys.exit(1)

# 2. 测试全量同步
print("2. 测试全量同步...")
try:
    # 第一次全量写入
    df1 = pl.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": ["20240101", "20240101"],
        "value": [10.0, 20.0],
    })
    db_client.upsert(test_table, df1, ["ts_code", "trade_date"], is_full_sync=True)

    result = db_client.query(f"SELECT * FROM {test_table} ORDER BY ts_code")
    print(f"   第一次全量写入后: {len(result)} 行")
    print(result)

    # 第二次全量写入（应该清空旧数据）
    df2 = pl.DataFrame({
        "ts_code": ["000003.SZ"],
        "trade_date": ["20240102"],
        "value": [30.0],
    })
    db_client.upsert(test_table, df2, ["ts_code", "trade_date"], is_full_sync=True)

    result = db_client.query(f"SELECT * FROM {test_table} ORDER BY ts_code")
    print(f"   第二次全量写入后: {len(result)} 行（应该只有1行）")
    print(result)

    if len(result) == 1 and result["ts_code"][0] == "000003.SZ":
        print("   ✓ 全量同步测试通过\n")
    else:
        print("   ✗ 全量同步测试失败：旧数据未清空\n")
        sys.exit(1)
except Exception as e:
    print(f"   ✗ 全量同步测试失败: {e}\n")
    sys.exit(1)

# 3. 测试增量同步
print("3. 测试增量同步...")
try:
    # 先写入多个日期的数据
    df_init = pl.DataFrame({
        "ts_code": ["000001.SZ", "000001.SZ", "000002.SZ", "000002.SZ"],
        "trade_date": ["20240101", "20240102", "20240101", "20240102"],
        "value": [10.0, 11.0, 20.0, 21.0],
    })
    db_client.upsert(test_table, df_init, ["ts_code", "trade_date"], is_full_sync=True)

    result = db_client.query(f"SELECT * FROM {test_table} ORDER BY ts_code, trade_date")
    print(f"   初始数据: {len(result)} 行")
    print(result)

    # 增量更新 20240101 的数据
    df_inc = pl.DataFrame({
        "ts_code": ["000003.SZ"],
        "trade_date": ["20240101"],
        "value": [30.0],
    })
    db_client.upsert(test_table, df_inc, ["ts_code", "trade_date"], is_full_sync=False, trade_date="20240101")

    result = db_client.query(f"SELECT * FROM {test_table} ORDER BY ts_code, trade_date")
    print(f"   增量更新 20240101 后: {len(result)} 行")
    print(result)

    # 验证：20240101 的数据应该只有 000003.SZ，20240102 的数据应该保留
    from datetime import datetime
    date_20240101 = datetime(2024, 1, 1)
    date_20240102 = datetime(2024, 1, 2)

    result_20240101 = result.filter(pl.col("trade_date") == date_20240101)
    result_20240102 = result.filter(pl.col("trade_date") == date_20240102)

    if len(result_20240101) == 1 and result_20240101["ts_code"][0] == "000003.SZ" and len(result_20240102) == 2:
        print("   ✓ 增量同步测试通过\n")
    else:
        print("   ✗ 增量同步测试失败：数据不符合预期\n")
        sys.exit(1)
except Exception as e:
    print(f"   ✗ 增量同步测试失败: {e}\n")
    sys.exit(1)

# 4. 清理测试表
print("4. 清理测试表...")
try:
    db_client.execute(f'dropTable(database("dfs://quant_meta"), "{test_table}")')
    print(f"   ✓ 已删除测试表 {test_table}\n")
except Exception as e:
    print(f"   ✗ 清理失败: {e}\n")

print("=== 所有测试通过 ===")
