#!/usr/bin/env python3
"""
API健康检查脚本
验证后端API端点的连通性
"""
import sys
import requests
import json
from datetime import datetime

sys.path.insert(0, '.')

from app.core.config import settings

BASE_URL = "http://localhost:8000"
API_V1 = f"{BASE_URL}/api/v1"

print("=" * 70)
print("QuantSystem API 健康检查")
print("=" * 70)
print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"API基础地址: {API_V1}")
print()

results = []

def check_endpoint(method, url, name, params=None, json_data=None):
    """检查单个API端点"""
    try:
        if method == "GET":
            response = requests.get(url, params=params, timeout=10)
        elif method == "POST":
            response = requests.post(url, json=json_data, timeout=10)
        else:
            return (name, False, "不支持的方法")

        status = "✅" if response.status_code < 400 else "❌"
        message = f"HTTP {response.status_code}"

        if response.status_code < 400:
            try:
                data = response.json()
                if isinstance(data, dict) and data.get("status") == "success":
                    message += " (success)"
            except:
                pass

        results.append((name, status, message))
        print(f"  {status} {name}: {message}")
        return True

    except requests.exceptions.ConnectionError:
        results.append((name, "❌", "连接失败"))
        print(f"  ❌ {name}: 连接失败")
        return False
    except Exception as e:
        results.append((name, "❌", str(e)))
        print(f"  ❌ {name}: {e}")
        return False

print("[1/5] 检查服务基础连接...")
try:
    response = requests.get(f"{BASE_URL}/docs", timeout=5)
    print(f"  ✅ Swagger UI 可访问: HTTP {response.status_code}")
except Exception as e:
    print(f"  ❌ Swagger UI 访问失败: {e}")

print()
print("[2/5] 检查配置管理API...")
check_endpoint("GET", f"{API_V1}/config/types", "获取配置类型")

print()
print("[3/5] 检查数据中心API...")
check_endpoint("GET", f"{API_V1}/data/tables", "列出数据库表")
check_endpoint("GET", f"{API_V1}/tasks/sync", "列出同步任务", params={"enabled_only": "true"})
check_endpoint("GET", f"{API_V1}/tasks/etl", "列出ETL任务", params={"enabled_only": "true"})

print()
print("[4/5] 检查因子中心API...")
check_endpoint("GET", f"{API_V1}/factor/factors", "列出因子")
check_endpoint("GET", f"{API_V1}/factor/data-config", "获取数据配置")
check_endpoint("GET", f"{API_V1}/factor/available-tables", "获取可用表列表")

print()
print("[5/5] 检查调度中心API...")
check_endpoint("GET", f"{API_V1}/flows", "列出Flows")
check_endpoint("GET", f"{API_V1}/tasks/running", "获取运行中任务")

print()
print("=" * 70)
print("检查结果汇总")
print("=" * 70)

total = len(results)
passed = sum(1 for _, status, _ in results if status == "✅")
failed = total - passed

print(f"总检查数: {total}")
print(f"通过: {passed}")
print(f"失败: {failed}")
print(f"通过率: {passed/total*100:.1f}%" if total > 0 else "N/A")

if failed > 0:
    print()
    print("失败的检查:")
    for name, status, msg in results:
        if status != "✅":
            print(f"  - {name}: {msg}")

print()
print("=" * 70)

if failed == 0:
    print("✅ 所有API检查通过!")
    sys.exit(0)
else:
    print("⚠️  部分API检查失败，请检查上述错误")
    sys.exit(1)
