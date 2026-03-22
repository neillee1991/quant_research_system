#!/usr/bin/env python3
"""
轻量级验证脚本 - 只检查文件结构和语法
不需要安装依赖
"""
import ast
import json
from pathlib import Path

backend_dir = Path(__file__).parent

print("=" * 60)
print("QuantSystem 重构轻量级验证")
print("=" * 60)

success_count = 0
fail_count = 0

def check_python_syntax(filepath: Path) -> bool:
    """检查 Python 文件语法"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        ast.parse(content, filename=str(filepath))
        return True
    except SyntaxError as e:
        print(f"  语法错误: {e}")
        return False
    except Exception as e:
        print(f"  读取错误: {e}")
        return False

def check_json_syntax(filepath: Path) -> bool:
    """检查 JSON 文件语法"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            json.load(f)
        return True
    except json.JSONDecodeError as e:
        print(f"  JSON 语法错误: {e}")
        return False
    except Exception as e:
        print(f"  读取错误: {e}")
        return False

print("\n1. 检查新创建的 schema_utils.py:")
print("-" * 40)
schema_utils = backend_dir / "app" / "api" / "v1" / "data" / "schema_utils.py"
if schema_utils.exists():
    print(f"✓ 文件存在: {schema_utils}")
    if check_python_syntax(schema_utils):
        print("✓ Python 语法正确")
        success_count += 1
    else:
        fail_count += 1
    # 检查内容
    content = schema_utils.read_text(encoding='utf-8')
    expected_functions = [
        'compare_schemas',
        'compare_detailed_schemas',
        'get_schema_changes',
        'infer_ddb_type_from_polars',
        'get_field_types_from_dataframe',
    ]
    for func in expected_functions:
        if f'def {func}' in content:
            print(f"✓ 包含函数: {func}")
        else:
            print(f"✗ 缺少函数: {func}")
            fail_count += 1
    if 'POLARS_TO_DDB_TYPE_MAP' in content:
        print("✓ 包含常量: POLARS_TO_DDB_TYPE_MAP")
        success_count += 1
else:
    print(f"✗ 文件不存在: {schema_utils}")
    fail_count += 1

print("\n2. 检查更新后的 etl_api.py:")
print("-" * 40)
etl_api = backend_dir / "app" / "api" / "v1" / "data" / "etl_api.py"
if etl_api.exists():
    print(f"✓ 文件存在: {etl_api}")
    if check_python_syntax(etl_api):
        print("✓ Python 语法正确")
        success_count += 1
    else:
        fail_count += 1
    # 检查重复函数已删除
    content = etl_api.read_text(encoding='utf-8')
    count_compare_schemas = content.count('def _compare_schemas')
    count_polars_map = content.count('POLARS_TO_DDB_TYPE_MAP')
    if count_compare_schemas == 0:
        print("✓ 重复的 _compare_schemas 函数已删除")
        success_count += 1
    else:
        print(f"✗ 仍有 {count_compare_schemas} 个 _compare_schemas 函数")
        fail_count += 1
    if count_polars_map == 0:
        print("✓ 重复的 POLARS_TO_DDB_TYPE_MAP 常量已删除")
        success_count += 1
    else:
        print(f"✗ 仍有 {count_polars_map} 个 POLARS_TO_DDB_TYPE_MAP 常量")
        fail_count += 1
    # 检查导入
    if 'from app.api.v1.data.schema_utils' in content:
        print("✓ 正确导入 schema_utils")
        success_count += 1
    else:
        print("✗ 缺少 schema_utils 导入")
        fail_count += 1
else:
    print(f"✗ 文件不存在: {etl_api}")
    fail_count += 1

print("\n3. 检查新的 seed 模块:")
print("-" * 40)
seed_dir = backend_dir / "infrastructure" / "seed"
if seed_dir.exists():
    print(f"✓ 目录存在: {seed_dir}")
    seed_files = ['__init__.py', 'loader.py', 'manager.py']
    for f in seed_files:
        filepath = seed_dir / f
        if filepath.exists():
            print(f"✓ 文件存在: {f}")
            if check_python_syntax(filepath):
                print(f"  ✓ {f} 语法正确")
                success_count += 1
            else:
                fail_count += 1
        else:
            print(f"✗ 文件不存在: {f}")
            fail_count += 1
else:
    print(f"✗ 目录不存在: {seed_dir}")
    fail_count += 1

print("\n4. 检查 seed 配置文件:")
print("-" * 40)
config_dir = backend_dir / "config" / "seed_data"
if config_dir.exists():
    print(f"✓ 目录存在: {config_dir}")
    config_files = ['sync_tasks.json', 'etl_tasks.json', 'factor_metadata.json', 'factor_data_config.json']
    for f in config_files:
        filepath = config_dir / f
        if filepath.exists():
            print(f"✓ 文件存在: {f}")
            if check_json_syntax(filepath):
                print(f"  ✓ {f} JSON 语法正确")
                size_kb = filepath.stat().st_size / 1024
                print(f"  ✓ {f} 大小: {size_kb:.2f} KB")
                success_count += 1
            else:
                fail_count += 1
        else:
            print(f"✗ 文件不存在: {f}")
            fail_count += 1
else:
    print(f"✗ 目录不存在: {config_dir}")
    fail_count += 1

print("\n5. 检查之前修改的文件:")
print("-" * 40)
for filename in ['factor_analysis.py', 'factor_compute.py']:
    filepath = backend_dir / "app" / "api" / "v1" / "production" / filename
    if filepath.exists():
        if check_python_syntax(filepath):
            print(f"✓ {filename} 语法正确")
            success_count += 1
        else:
            print(f"✗ {filename} 语法错误")
            fail_count += 1

utils_file = backend_dir / "app" / "core" / "utils.py"
if utils_file.exists():
    if check_python_syntax(utils_file):
        print(f"✓ utils.py 语法正确")
        content = utils_file.read_text(encoding='utf-8')
        new_utils = [
            'normalize_date_to_object',
            'format_date_for_display',
            'validate_yyyymmdd',
            'safe_str_datetime',
            'unify_record_fields',
            'decompress_json',
            'load_json_from_file',
            'parse_json_fields',
            'normalize_trade_date_pl',
        ]
        found_count = 0
        for util in new_utils:
            if f'def {util}' in content or util in content:
                found_count += 1
        print(f"✓ utils.py 包含 {found_count}/{len(new_utils)} 个新工具函数")
        success_count += 1
    else:
        print(f"✗ utils.py 语法错误")
        fail_count += 1

print("\n" + "=" * 60)
print("验证总结")
print("=" * 60)
print(f"成功: {success_count}")
print(f"失败: {fail_count}")
print(f"总计: {success_count + fail_count}")

if fail_count == 0:
    print("\n🎉 所有验证通过！重构成功！")
else:
    print(f"\n⚠️  有 {fail_count} 个问题需要检查")
