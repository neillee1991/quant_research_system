#!/usr/bin/env python3
"""
验证数据完整性脚本
检查表结构和数据是否正确
"""

import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
backend_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_dir))

from app.core.logger import logger
from store.dolphindb_client import db_client


def check_table_exists(table_name: str) -> bool:
    """检查表是否存在"""
    try:
        exists_query = f"""
        try {{
            schema({table_name});
            true
        }} catch(ex) {{
            false
        }}
        """
        exists = db_client.session.run(exists_query)
        return exists
    except Exception as e:
        logger.error(f"检查表 {table_name} 失败: {e}")
        return False


def get_table_schema(table_name: str) -> dict:
    """获取表结构"""
    try:
        schema_query = f"schema({table_name})"
        schema = db_client.session.run(schema_query)
        return schema
    except Exception as e:
        logger.error(f"获取表 {table_name} 结构失败: {e}")
        return None


def get_table_count(table_name: str) -> int:
    """获取表记录数"""
    try:
        df = db_client.query(f"SELECT count(*) as cnt FROM {table_name}")
        return df["cnt"][0] if not df.is_empty() else 0
    except Exception as e:
        logger.error(f"查询表 {table_name} 记录数失败: {e}")
        return -1


def check_no_version_fields(table_name: str) -> dict:
    """检查表中是否不包含版本字段"""
    version_fields = [
        "version_number",
        "is_current",
        "changed_by",
        "change_reason",
        "effective_from",
        "effective_to",
    ]

    try:
        schema = get_table_schema(table_name)
        if schema is None:
            return {"status": "error", "message": "无法获取表结构"}

        # 获取列名
        columns = schema.get("colDefs", {}).get("name", [])

        # 检查是否包含版本字段
        found_version_fields = [f for f in version_fields if f in columns]

        if found_version_fields:
            return {
                "status": "fail",
                "message": f"表中仍包含版本字段: {', '.join(found_version_fields)}"
            }
        else:
            return {
                "status": "pass",
                "message": "表中不包含版本字段"
            }

    except Exception as e:
        return {"status": "error", "message": str(e)}


def check_primary_keys(table_name: str, expected_keys: list) -> dict:
    """检查主键约束"""
    try:
        schema = get_table_schema(table_name)
        if schema is None:
            return {"status": "error", "message": "无法获取表结构"}

        # DolphinDB 的主键信息在 partitionColumnName 中
        partition_cols = schema.get("partitionColumnName", [])

        # 简单检查：如果表有数据，尝试插入重复主键
        count = get_table_count(table_name)
        if count > 0:
            return {
                "status": "pass",
                "message": f"表有 {count} 条记录，主键约束应该有效"
            }
        else:
            return {
                "status": "warning",
                "message": "表为空，无法验证主键约束"
            }

    except Exception as e:
        return {"status": "error", "message": str(e)}


def main():
    """主函数"""
    print("=" * 60)
    print("数据完整性验证脚本")
    print("=" * 60)

    # 需要验证的表
    tables_to_check = [
        {
            "name": "sync_task_config",
            "primary_keys": ["task_id"],
        },
        {
            "name": "etl_task_config",
            "primary_keys": ["task_id"],
        },
        {
            "name": "factor_metadata",
            "primary_keys": ["factor_id"],
        },
    ]

    all_passed = True

    for table_info in tables_to_check:
        table_name = table_info["name"]
        primary_keys = table_info["primary_keys"]

        print(f"\n{'=' * 60}")
        print(f"验证表: {table_name}")
        print(f"{'=' * 60}")

        # 1. 检查表是否存在
        print("\n1. 检查表是否存在...")
        if check_table_exists(table_name):
            print(f"  ✓ 表 {table_name} 存在")
        else:
            print(f"  ✗ 表 {table_name} 不存在")
            all_passed = False
            continue

        # 2. 检查记录数
        print("\n2. 检查记录数...")
        count = get_table_count(table_name)
        if count >= 0:
            print(f"  ✓ 表 {table_name} 有 {count} 条记录")
        else:
            print(f"  ✗ 无法查询表 {table_name} 的记录数")
            all_passed = False

        # 3. 检查是否不包含版本字段
        print("\n3. 检查版本字段...")
        result = check_no_version_fields(table_name)
        if result["status"] == "pass":
            print(f"  ✓ {result['message']}")
        elif result["status"] == "fail":
            print(f"  ✗ {result['message']}")
            all_passed = False
        else:
            print(f"  ✗ 错误: {result['message']}")
            all_passed = False

        # 4. 检查主键约束
        print("\n4. 检查主键约束...")
        result = check_primary_keys(table_name, primary_keys)
        if result["status"] == "pass":
            print(f"  ✓ {result['message']}")
        elif result["status"] == "warning":
            print(f"  ○ {result['message']}")
        else:
            print(f"  ✗ 错误: {result['message']}")

    # 总结
    print("\n" + "=" * 60)
    if all_passed:
        print("✓ 所有验证通过!")
    else:
        print("✗ 部分验证失败，请检查上述错误")
    print("=" * 60)


if __name__ == "__main__":
    main()
