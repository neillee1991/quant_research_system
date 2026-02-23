#!/usr/bin/env python
"""
系统健康检查脚本
验证所有优化措施是否正确生效
"""
import sys
from pathlib import Path

# 添加后端目录到路径
backend_dir = Path(__file__).parent
sys.path.insert(0, str(backend_dir))

from store.postgres_client import db_client
from store.redis_client import redis_client
from app.core.config import settings

def check_database():
    """检查数据库连接和索引"""
    print("\n" + "="*60)
    print("1. 数据库检查")
    print("="*60)

    try:
        # 测试连接
        db_client.query("SELECT 1")
        print("✅ 数据库连接正常")

        # 检查连接池配置
        print(f"✅ 连接池配置: min={settings.database.connection_pool_min}, max={settings.database.connection_pool_size}")

        # 检查索引数量
        df = db_client.query("""
            SELECT COUNT(*) as cnt
            FROM pg_indexes
            WHERE schemaname = 'public' AND indexname LIKE 'idx_%'
        """)
        index_count = df['cnt'][0]

        if index_count >= 20:
            print(f"✅ 性能索引已创建: {index_count} 个")
        else:
            print(f"⚠️  索引数量较少: {index_count} 个（建议 >= 20）")

        # 检查关键索引
        key_indexes = [
            'idx_daily_data_ts_code_trade_date',
            'idx_factor_values_factor_id_trade_date',
            'idx_daily_basic_ts_code_trade_date'
        ]

        for idx_name in key_indexes:
            df = db_client.query(
                "SELECT COUNT(*) as cnt FROM pg_indexes WHERE indexname = %s",
                (idx_name,)
            )
            if df['cnt'][0] > 0:
                print(f"  ✓ {idx_name}")
            else:
                print(f"  ✗ {idx_name} 缺失")

        return True
    except Exception as e:
        print(f"❌ 数据库检查失败: {e}")
        return False

def check_redis():
    """检查Redis缓存"""
    print("\n" + "="*60)
    print("2. Redis缓存检查")
    print("="*60)

    if redis_client.is_available():
        print("✅ Redis连接正常")

        # 获取统计信息
        stats = redis_client.get_stats()
        print(f"  内存使用: {stats.get('used_memory', 'N/A')}")
        print(f"  连接数: {stats.get('connected_clients', 0)}")
        print(f"  命中率: {stats.get('hit_rate', 'N/A')}")

        # 测试缓存功能
        test_key = "health_check:test"
        redis_client.set(test_key, "test_value", ttl=10)
        value = redis_client.get(test_key)
        redis_client.delete(test_key)

        if value == "test_value":
            print("✅ 缓存读写功能正常")
        else:
            print("⚠️  缓存读写测试失败")

        return True
    else:
        print("⚠️  Redis不可用（系统将使用降级模式）")
        print("  建议: docker-compose up -d redis")
        return False

def check_config():
    """检查配置"""
    print("\n" + "="*60)
    print("3. 配置检查")
    print("="*60)

    print(f"✅ 数据库: {settings.database.postgres_host}:{settings.database.postgres_port}/{settings.database.postgres_db}")
    print(f"✅ Redis: {settings.redis.redis_host}:{settings.redis.redis_port}/{settings.redis.redis_db}")
    print(f"✅ 连接池: min={settings.database.connection_pool_min}, max={settings.database.connection_pool_size}")
    print(f"✅ 缓存TTL:")
    print(f"  - 股票列表: {settings.redis.cache_ttl_stock_list}秒")
    print(f"  - 日线数据: {settings.redis.cache_ttl_daily_data}秒")
    print(f"  - 因子元数据: {settings.redis.cache_ttl_factor_metadata}秒")
    print(f"  - 因子分析: {settings.redis.cache_ttl_factor_analysis}秒")

    return True

def check_performance():
    """检查性能"""
    print("\n" + "="*60)
    print("4. 性能测试")
    print("="*60)

    import time

    try:
        # 测试简单查询
        start = time.time()
        df = db_client.query("SELECT 1")
        query_time = (time.time() - start) * 1000

        if query_time < 100:
            print(f"✅ 简单查询: {query_time:.2f}ms")
        else:
            print(f"⚠️  简单查询较慢: {query_time:.2f}ms")

        # 测试索引查询
        start = time.time()
        df = db_client.query("""
            SELECT * FROM daily_data
            WHERE ts_code = %s AND trade_date >= %s
            LIMIT 100
        """, ('000001.SZ', '20240101'))
        query_time = (time.time() - start) * 1000

        if query_time < 100:
            print(f"✅ 索引查询: {query_time:.2f}ms ({len(df)}行)")
        else:
            print(f"⚠️  索引查询较慢: {query_time:.2f}ms ({len(df)}行)")

        return True
    except Exception as e:
        print(f"❌ 性能测试失败: {e}")
        return False

def check_imports():
    """检查关键模块导入"""
    print("\n" + "="*60)
    print("5. 模块导入检查")
    print("="*60)

    modules = [
        ('fastapi', 'FastAPI'),
        ('redis', 'Redis'),
        ('polars', 'Polars'),
        ('psycopg2', 'PostgreSQL驱动'),
        ('apscheduler', '任务调度器'),
        ('pydantic', 'Pydantic'),
        ('loguru', 'Loguru日志'),
    ]

    all_ok = True
    for module_name, display_name in modules:
        try:
            __import__(module_name)
            print(f"✅ {display_name} ({module_name})")
        except ImportError:
            print(f"❌ {display_name} ({module_name}) 未安装")
            all_ok = False

    return all_ok

def main():
    """主函数"""
    print("\n" + "="*60)
    print("量化研究系统 - 健康检查")
    print("="*60)

    results = {
        '数据库': check_database(),
        'Redis缓存': check_redis(),
        '配置': check_config(),
        '性能': check_performance(),
        '模块导入': check_imports(),
    }

    print("\n" + "="*60)
    print("检查结果汇总")
    print("="*60)

    for name, result in results.items():
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{name}: {status}")

    all_passed = all(results.values())

    print("\n" + "="*60)
    if all_passed:
        print("🎉 所有检查通过！系统运行正常")
    else:
        print("⚠️  部分检查未通过，请查看上述详情")
    print("="*60)

    return 0 if all_passed else 1

if __name__ == '__main__':
    sys.exit(main())
