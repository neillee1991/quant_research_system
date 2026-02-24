#!/usr/bin/env python
"""
系统健康检查脚本
验证 DolphinDB 和各组件是否正常运行
"""
import sys
from pathlib import Path

# 添加后端目录到路径
backend_dir = Path(__file__).parent
sys.path.insert(0, str(backend_dir))

from store.dolphindb_client import db_client
from app.core.config import settings

def check_database():
    """检查 DolphinDB 连接"""
    print("\n" + "="*60)
    print("1. DolphinDB 数据库检查")
    print("="*60)

    try:
        df = db_client.query("SELECT 1 as test")
        print("✅ DolphinDB 连接正常")
        print(f"✅ 连接地址: {settings.database.dolphindb_host}:{settings.database.dolphindb_port}")

        # 检查关键表是否存在
        key_tables = [
            'daily_data', 'daily_basic', 'adj_factor',
            'index_daily', 'moneyflow', 'factor_values',
            'stock_basic', 'sync_log', 'factor_metadata'
        ]

        existing = 0
        for table in key_tables:
            if db_client.table_exists(table):
                print(f"  ✓ {table}")
                existing += 1
            else:
                print(f"  ✗ {table} 缺失")

        print(f"\n  表状态: {existing}/{len(key_tables)} 个表已创建")
        return True
    except Exception as e:
        print(f"❌ DolphinDB 检查失败: {e}")
        return False

def check_config():
    """检查配置"""
    print("\n" + "="*60)
    print("2. 配置检查")
    print("="*60)

    print(f"✅ DolphinDB: {settings.database.dolphindb_host}:{settings.database.dolphindb_port}")
    print(f"✅ 数据库路径: {settings.database.db_path}")
    print(f"✅ 元数据库路径: {settings.database.meta_db_path}")
    print(f"✅ Prefect API: {settings.prefect_api_url}")
    return True

def check_performance():
    """检查性能"""
    print("\n" + "="*60)
    print("3. 性能测试")
    print("="*60)

    import time

    try:
        # 测试简单查询
        start = time.time()
        df = db_client.query("SELECT 1 as test")
        query_time = (time.time() - start) * 1000
        print(f"{'✅' if query_time < 100 else '⚠️'} 简单查询: {query_time:.2f}ms")

        # 测试数据查询
        start = time.time()
        df = db_client.query(
            "SELECT * FROM daily_data WHERE ts_code=%s AND trade_date>=%s LIMIT 100",
            ('000001.SZ', '20240101')
        )
        query_time = (time.time() - start) * 1000
        print(f"{'✅' if query_time < 200 else '⚠️'} 数据查询: {query_time:.2f}ms ({len(df)}行)")

        return True
    except Exception as e:
        print(f"❌ 性能测试失败: {e}")
        return False

def main():
    print("=" * 60)
    print("  量化研究系统 - 健康检查")
    print("=" * 60)

    results = {
        "DolphinDB": check_database(),
        "配置": check_config(),
        "性能": check_performance(),
    }

    print("\n" + "=" * 60)
    print("  检查结果汇总")
    print("=" * 60)

    all_pass = True
    for name, passed in results.items():
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"  {name}: {status}")
        if not passed:
            all_pass = False

    if all_pass:
        print("\n🎉 所有检查通过！")
        return 0
    else:
        print("\n⚠️  部分检查未通过，请查看上方详情")
        return 1


if __name__ == '__main__':
    sys.exit(main())
