#!/usr/bin/env python3
"""
测试异常类的问题
复现 "got multiple values for keyword argument 'status_code'" 错误
"""
import sys
sys.path.insert(0, '/Users/lisheng/Code/quantsystem/quant_research_system/backend')

from app.core.exceptions import DataCollectionError, SyncException, QuantException, DataException

print("=== 测试异常类 ===")

# 测试 1: 直接测试 DataCollectionError
print("\n1. 测试 DataCollectionError:")
try:
    raise DataCollectionError("tushare", "test error")
except Exception as e:
    print(f"   成功: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# 测试 2: 查看异常的 MRO
print("\n2. 异常类的 MRO:")
print(f"   DataCollectionError: {DataCollectionError.__mro__}")
print(f"   DataException: {DataException.__mro__}")
print(f"   SyncException: {SyncException.__mro__}")
print(f"   QuantException: {QuantException.__mro__}")

# 测试 3: 检查 __init__ 签名
print("\n3. __init__ 签名:")
import inspect
print(f"   QuantException.__init__: {inspect.signature(QuantException.__init__)}")
print(f"   DataException.__init__: {inspect.signature(DataException.__init__)}")
print(f"   DataCollectionError.__init__: {inspect.signature(DataCollectionError.__init__)}")

# 测试 4: 模拟可能的冲突情况
print("\n4. 模拟各种调用方式:")

# 方式 1: 正常调用
print("\n   方式 1: DataCollectionError('source', 'reason')")
try:
    exc = DataCollectionError("source", "reason")
    print(f"      成功: {exc.message}")
except TypeError as e:
    print(f"      失败: {e}")

# 方式 2: 带 status_code
print("\n   方式 2: DataCollectionError('source', 'reason', status_code=400)")
try:
    exc = DataCollectionError("source", "reason", status_code=400)
    print(f"      成功: {exc.message}, status_code={exc.status_code}")
except TypeError as e:
    print(f"      失败: {e}")

# 方式 3: DataException
print("\n   方式 3: DataException('message', status_code=400)")
try:
    exc = DataException("message", status_code=400)
    print(f"      成功: {exc.message}, status_code={exc.status_code}")
except TypeError as e:
    print(f"      失败: {e}")

# 方式 4: DataException 带位置参数 status_code
print("\n   方式 4: DataException('message', 400)")
try:
    exc = DataException("message", 400)
    print(f"      成功: {exc.message}, status_code={exc.status_code}")
except TypeError as e:
    print(f"      失败: {e}")

# 测试 5: 检查是否有重复定义
print("\n5. 检查模块:")
print(f"   QuantException 来自: {QuantException.__module__}")
print(f"   DataException 来自: {DataException.__module__}")
print(f"   DataCollectionError 来自: {DataCollectionError.__module__}")

# 测试 6: 检查是否有其他导入路径
print("\n6. 检查 sys.modules:")
for key in list(sys.modules.keys()):
    if 'exception' in key.lower() and 'app' in key:
        print(f"   {key}")