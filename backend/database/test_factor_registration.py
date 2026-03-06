#!/usr/bin/env python3
"""
测试新的因子注册机制
验证从数据库加载因子是否正常工作
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from store.dolphindb_client import db_client
from engine.production.registry import discover_factors, get_registry, get_factor


def test_factor_registration():
    """测试因子注册机制"""
    print("=" * 60)
    print("测试因子注册机制")
    print("=" * 60)

    # 1. 清空内存注册表
    print("\n1. 清空内存注册表...")
    from engine.production.registry import _factor_registry
    _factor_registry.clear()
    print(f"   注册表已清空，当前因子数: {len(_factor_registry)}")

    # 2. 从数据库加载因子
    print("\n2. 从数据库加载因子...")
    discover_factors(db_client=db_client)
    registry = get_registry()
    print(f"   从数据库加载了 {len(registry)} 个因子")

    if not registry:
        print("   ✗ 没有加载到因子")
        return False

    # 3. 验证因子定义
    print("\n3. 验证因子定义:")
    success_count = 0
    for factor_id, definition in list(registry.items())[:5]:  # 只显示前5个
        print(f"\n   因子: {factor_id}")
        print(f"     描述: {definition.description}")
        print(f"     分类: {definition.category}")
        print(f"     计算模式: {definition.compute_mode}")
        print(f"     依赖: {definition.depends_on}")
        print(f"     参数: {definition.params}")
        print(f"     计算函数: {definition.func.__name__ if definition.func else 'None'}")

        # 验证计算函数是否可调用
        if callable(definition.func):
            print(f"     ✓ 计算函数可调用")
            success_count += 1
        else:
            print(f"     ✗ 计算函数不可调用")

    # 4. 测试获取单个因子
    print("\n4. 测试获取单个因子...")
    test_factor_id = list(registry.keys())[0] if registry else None
    if test_factor_id:
        factor = get_factor(test_factor_id)
        if factor:
            print(f"   ✓ 成功获取因子: {test_factor_id}")
        else:
            print(f"   ✗ 获取因子失败: {test_factor_id}")
    else:
        print("   ✗ 没有可测试的因子")

    # 5. 总结
    print("\n" + "=" * 60)
    print(f"测试完成: {success_count}/{min(5, len(registry))} 个因子验证成功")
    print("=" * 60)

    return success_count > 0


if __name__ == "__main__":
    success = test_factor_registration()
    sys.exit(0 if success else 1)
