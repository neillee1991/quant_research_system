#!/usr/bin/env python3
"""
验证脚本 - 检查重构后的代码是否可正常导入
"""
import sys
from pathlib import Path

# 添加项目路径
backend_dir = Path(__file__).parent
sys.path.insert(0, str(backend_dir))

print("=" * 60)
print("QuantSystem 重构验证")
print("=" * 60)

success_count = 0
fail_count = 0

def test_import(name: str, import_path: str):
    """测试模块导入"""
    global success_count, fail_count
    try:
        __import__(import_path)
        print(f"✓ {name}")
        success_count += 1
        return True
    except Exception as e:
        print(f"✗ {name} - 错误: {e}")
        fail_count += 1
        return False

print("\n1. 测试核心模块导入:")
print("-" * 40)
test_import("app.core.utils", "app.core.utils")
test_import("app.core.config", "app.core.config")
test_import("app.core.logger", "app.core.logger")

print("\n2. 测试新的 schema_utils 模块:")
print("-" * 40)
if test_import("schema_utils", "app.api.v1.data.schema_utils"):
    from app.api.v1.data import schema_utils
    print(f"  - POLARS_TO_DDB_TYPE_MAP: {len(schema_utils.POLARS_TO_DDB_TYPE_MAP)} 个类型映射")
    print(f"  - compare_schemas: 可用")
    print(f"  - get_schema_changes: 可用")
    print(f"  - get_field_types_from_dataframe: 可用")

print("\n3. 测试更新后的 etl_api 模块:")
print("-" * 40)
test_import("etl_api", "app.api.v1.data.etl_api")

print("\n4. 测试新的 seed 模块:")
print("-" * 40)
test_import("seed.loader", "infrastructure.seed.loader")
test_import("seed.manager", "infrastructure.seed.manager")
test_import("seed (package)", "infrastructure.seed")

print("\n5. 检查配置文件:")
print("-" * 40)
config_dir = backend_dir / "config" / "seed_data"
if config_dir.exists():
    json_files = list(config_dir.glob("*.json"))
    print(f"✓ 配置目录: {config_dir}")
    print(f"✓ JSON 配置文件: {len(json_files)} 个")
    for f in json_files:
        print(f"  - {f.name}")
    success_count += 1
else:
    print(f"✗ 配置目录不存在: {config_dir}")
    fail_count += 1

print("\n6. 测试 schema_utils 功能:")
print("-" * 40)
try:
    from app.api.v1.data.schema_utils import (
        compare_schemas,
        get_schema_changes,
        infer_ddb_type_from_polars,
    )
    import polars as pl

    # 测试 compare_schemas
    schema1 = {"col1": "INT", "col2": "STRING"}
    schema2 = {"col1": "INT", "col2": "STRING"}
    schema3 = {"col1": "INT", "col3": "DOUBLE"}

    assert compare_schemas(schema1, schema2) is False, "相同 schema 应返回 False"
    assert compare_schemas(schema1, schema3) is True, "不同 schema 应返回 True"
    print("✓ compare_schemas 工作正常")

    # 测试 get_schema_changes
    changes = get_schema_changes(schema1, schema3)
    assert changes["added"] == ["col3"], "应检测到新增字段"
    assert changes["removed"] == ["col2"], "应检测到删除字段"
    print("✓ get_schema_changes 工作正常")

    # 测试类型推断
    assert infer_ddb_type_from_polars(pl.Int64) == "LONG"
    assert infer_ddb_type_from_polars(pl.Utf8) == "STRING"
    assert infer_ddb_type_from_polars(pl.Float64) == "DOUBLE"
    print("✓ infer_ddb_type_from_polars 工作正常")

    success_count += 1
except Exception as e:
    print(f"✗ schema_utils 功能测试失败: {e}")
    import traceback
    traceback.print_exc()
    fail_count += 1

print("\n" + "=" * 60)
print("验证总结")
print("=" * 60)
print(f"成功: {success_count}")
print(f"失败: {fail_count}")
print(f"总计: {success_count + fail_count}")

if fail_count == 0:
    print("\n🎉 所有验证通过！重构成功！")
    sys.exit(0)
else:
    print("\n⚠️  部分验证失败，请检查错误信息")
    sys.exit(1)
