#!/usr/bin/env python3
"""
验证元数据表创建状态
"""
import sys
sys.path.insert(0, '/Users/bytedance/code/quant_research_system/backend')

from store.dolphindb_client import db_client

def main():
    print("=" * 60)
    print("元数据表创建状态验证")
    print("=" * 60)

    db_path = "dfs://quant"

    # 需要验证的表
    tables = [
        "sync_log",
        "sync_log_history",
        "sync_task_config",
        "etl_task_config",
        "factor_metadata",
        "factor_analysis",
        "factor_task_run",
        "factor_data_config",
    ]

    print("\n检查表是否存在:")
    print("-" * 60)

    all_exist = True
    for table in tables:
        try:
            exists = db_client._session.run(f"existsTable('{db_path}', '{table}')")
            status = "✅ 存在" if exists else "❌ 不存在"
            print(f"{table:30s} {status}")
            if not exists:
                all_exist = False
        except Exception as e:
            print(f"{table:30s} ⚠️  检查失败: {e}")
            all_exist = False

    print("\n" + "=" * 60)

    if all_exist:
        print("✅ 所有元数据表已成功创建！")
        return 0
    else:
        print("❌ 部分元数据表缺失")
        return 1

if __name__ == "__main__":
    sys.exit(main())
