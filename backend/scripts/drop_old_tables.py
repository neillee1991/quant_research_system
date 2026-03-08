#!/usr/bin/env python3
"""
删除旧表脚本
删除包含版本管理字段的旧表
"""

import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
backend_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_dir))

from app.core.logger import logger
from store.dolphindb_client import db_client


def drop_table(table_name: str) -> dict:
    """
    删除单个表

    Args:
        table_name: 表名

    Returns:
        删除结果
    """
    try:
        logger.info(f"正在删除表: {table_name}")

        # 检查表是否存在
        exists_query = f"""
        try {{
            schema({table_name});
            true
        }} catch(ex) {{
            false
        }}
        """
        exists = db_client.session.run(exists_query)

        if not exists:
            logger.warning(f"表 {table_name} 不存在，跳过删除")
            return {"table": table_name, "status": "not_exists"}

        # 删除表
        drop_query = f"dropTable(database('dfs://quant'), '{table_name}')"
        db_client.session.run(drop_query)

        logger.info(f"✓ 已删除表: {table_name}")
        return {"table": table_name, "status": "success"}

    except Exception as e:
        logger.error(f"✗ 删除表 {table_name} 失败: {e}")
        return {"table": table_name, "status": "error", "error": str(e)}


def main():
    """主函数"""
    print("=" * 60)
    print("删除旧表脚本")
    print("=" * 60)
    print("\n警告: 此操作将删除以下表:")
    print("  - sync_task_config")
    print("  - etl_task_config")
    print("  - factor_metadata")
    print("  - task_version_history (如果存在)")
    print("\n请确保已经备份数据!")

    # 确认操作
    response = input("\n是否继续? (yes/no): ").strip().lower()
    if response != "yes":
        print("操作已取消")
        return

    # 需要删除的表
    tables_to_drop = [
        "sync_task_config",
        "etl_task_config",
        "factor_metadata",
        "task_version_history",
    ]

    # 执行删除
    results = []
    for table_name in tables_to_drop:
        result = drop_table(table_name)
        results.append(result)

    # 生成删除报告
    print("\n" + "=" * 60)
    print("删除报告")
    print("=" * 60)

    success_count = 0
    not_exists_count = 0

    for result in results:
        if result["status"] == "success":
            print(f"✓ {result['table']}: 已删除")
            success_count += 1
        elif result["status"] == "not_exists":
            print(f"○ {result['table']}: 不存在")
            not_exists_count += 1
        else:
            print(f"✗ {result['table']}: 删除失败")
            print(f"  错误: {result['error']}")

    print(f"\n总计: {success_count} 个表已删除, {not_exists_count} 个表不存在")
    print("=" * 60)


if __name__ == "__main__":
    main()
