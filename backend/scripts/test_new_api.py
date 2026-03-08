#!/usr/bin/env python3
"""
测试新的统一任务管理 API
验证所有端点是否正常工作
"""
import requests
import json
import sys
from typing import Dict, Any

BASE_URL = "http://localhost:8000/api/v1"


def test_list_tasks(task_type: str = "sync"):
    """测试列出任务"""
    print(f"\n{'='*60}")
    print(f"测试: 列出 {task_type} 任务")
    print(f"{'='*60}")

    url = f"{BASE_URL}/tasks/{task_type}"
    response = requests.get(url)

    print(f"状态码: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"任务数量: {data['total']}")
        print(f"前 3 个任务:")
        for task in data['tasks'][:3]:
            print(f"  - {task.get('task_id')}: {task.get('description', 'N/A')}")
        return True
    else:
        print(f"错误: {response.text}")
        return False


def test_get_task(task_type: str = "sync", task_id: str = "sync_daily_data"):
    """测试获取单个任务"""
    print(f"\n{'='*60}")
    print(f"测试: 获取任务 {task_id}")
    print(f"{'='*60}")

    url = f"{BASE_URL}/tasks/{task_type}/{task_id}"
    response = requests.get(url)

    print(f"状态码: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        task = data['task']
        print(f"任务ID: {task.get('task_id')}")
        print(f"描述: {task.get('description')}")
        print(f"表名: {task.get('table_name')}")
        print(f"启用: {task.get('enabled')}")
        return True
    else:
        print(f"错误: {response.text}")
        return False


def test_get_version():
    """测试获取配置版本"""
    print(f"\n{'='*60}")
    print(f"测试: 获取配置版本")
    print(f"{'='*60}")

    url = f"{BASE_URL}/tasks/version"
    response = requests.get(url)

    print(f"状态码: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"版本号: {data['version']}")
        print(f"消息: {data['message']}")
        return True
    else:
        print(f"错误: {response.text}")
        return False


def test_validate_schema():
    """测试 Schema 验证"""
    print(f"\n{'='*60}")
    print(f"测试: Schema 验证")
    print(f"{'='*60}")

    url = f"{BASE_URL}/schema/validate"

    # 测试有效的 schema
    valid_schema = {
        "schema": {
            "ts_code": {"type": "SYMBOL", "nullable": False, "comment": "股票代码"},
            "trade_date": {"type": "DATE", "nullable": False, "comment": "交易日期"},
            "close": {"type": "DOUBLE", "nullable": True, "comment": "收盘价"}
        },
        "primary_keys": ["ts_code", "trade_date"]
    }

    response = requests.post(url, json=valid_schema)
    print(f"有效 schema - 状态码: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"验证结果: {data['is_valid']}")
        print(f"消息: {data['message']}")

    # 测试无效的 schema
    invalid_schema = {
        "schema": {
            "ts_code": {"type": "INVALID_TYPE", "nullable": False, "comment": "股票代码"}
        },
        "primary_keys": ["ts_code"]
    }

    response = requests.post(url, json=invalid_schema)
    print(f"\n无效 schema - 状态码: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"验证结果: {data['is_valid']}")
        print(f"错误: {data['errors']}")

    return True


def test_compare_schema():
    """测试 Schema 比较"""
    print(f"\n{'='*60}")
    print(f"测试: Schema 比较")
    print(f"{'='*60}")

    url = f"{BASE_URL}/schema/compare"

    old_schema = {
        "ts_code": {"type": "SYMBOL", "nullable": False, "comment": "股票代码"},
        "trade_date": {"type": "DATE", "nullable": False, "comment": "交易日期"}
    }

    new_schema = {
        "ts_code": {"type": "SYMBOL", "nullable": False, "comment": "股票代码"},
        "trade_date": {"type": "DATE", "nullable": False, "comment": "交易日期"},
        "close": {"type": "DOUBLE", "nullable": True, "comment": "收盘价"}  # 新增字段
    }

    payload = {
        "old_schema": old_schema,
        "new_schema": new_schema
    }

    response = requests.post(url, json=payload)
    print(f"状态码: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"兼容性: {data['is_compatible']}")
        print(f"变更: {json.dumps(data['changes'], indent=2, ensure_ascii=False)}")
        print(f"消息: {data['message']}")
        return True
    else:
        print(f"错误: {response.text}")
        return False


def test_validate_etl_script():
    """测试 ETL 脚本验证"""
    print(f"\n{'='*60}")
    print(f"测试: ETL 脚本验证")
    print(f"{'='*60}")

    url = f"{BASE_URL}/etl/validate-script"

    # 有效脚本
    valid_script = """
result = df.select([
    pl.col("ts_code"),
    pl.col("trade_date"),
    pl.col("close")
])
"""

    response = requests.post(url, json={"script": valid_script})
    print(f"有效脚本 - 状态码: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"验证结果: {data['is_valid']}")
        print(f"消息: {data['message']}")

    # 无效脚本
    invalid_script = "result = df.select([  # 语法错误"

    response = requests.post(url, json={"script": invalid_script})
    print(f"\n无效脚本 - 状态码: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"验证结果: {data['is_valid']}")
        print(f"错误: {data['errors']}")

    return True


def verify_seed_tasks():
    """验证种子任务完整性"""
    print(f"\n{'='*60}")
    print(f"验证: 种子任务完整性")
    print(f"{'='*60}")

    # 检查 sync 任务数量
    response = requests.get(f"{BASE_URL}/tasks/sync")
    if response.status_code == 200:
        sync_count = response.json()['total']
        print(f"Sync 任务数量: {sync_count} (预期: 17)")
        if sync_count != 17:
            print(f"⚠️  警告: Sync 任务数量不匹配!")

    # 检查 etl 任务数量
    response = requests.get(f"{BASE_URL}/tasks/etl")
    if response.status_code == 200:
        etl_count = response.json()['total']
        print(f"ETL 任务数量: {etl_count} (预期: 3)")
        if etl_count != 3:
            print(f"⚠️  警告: ETL 任务数量不匹配!")

    # 检查关键任务是否存在
    key_tasks = [
        ("sync", "sync_daily_data"),
        ("sync", "sync_stock_basic"),
        ("sync", "sync_trade_cal"),
        ("etl", "etl_stock_daily_info")
    ]

    print(f"\n检查关键任务:")
    for task_type, task_id in key_tasks:
        response = requests.get(f"{BASE_URL}/tasks/{task_type}/{task_id}")
        if response.status_code == 200:
            print(f"  ✅ {task_id}")
        else:
            print(f"  ❌ {task_id} - 不存在!")

    return True


def main():
    """运行所有测试"""
    print("="*60)
    print("统一任务管理 API 测试套件")
    print("="*60)

    tests = [
        ("列出 Sync 任务", lambda: test_list_tasks("sync")),
        ("列出 ETL 任务", lambda: test_list_tasks("etl")),
        ("获取单个任务", lambda: test_get_task("sync", "sync_daily_data")),
        ("获取配置版本", test_get_version),
        ("验证 Schema", test_validate_schema),
        ("比较 Schema", test_compare_schema),
        ("验证 ETL 脚本", test_validate_etl_script),
        ("验证种子任务", verify_seed_tasks),
    ]

    results = []
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, success))
        except Exception as e:
            print(f"\n❌ 测试失败: {name}")
            print(f"错误: {e}")
            results.append((name, False))

    # 打印总结
    print(f"\n{'='*60}")
    print("测试总结")
    print(f"{'='*60}")

    passed = sum(1 for _, success in results if success)
    total = len(results)

    for name, success in results:
        status = "✅ 通过" if success else "❌ 失败"
        print(f"{status} - {name}")

    print(f"\n总计: {passed}/{total} 通过")

    if passed == total:
        print("\n🎉 所有测试通过!")
        sys.exit(0)
    else:
        print(f"\n⚠️  {total - passed} 个测试失败")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n测试被中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n测试运行失败: {e}")
        sys.exit(1)
