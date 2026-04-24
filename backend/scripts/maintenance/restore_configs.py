#!/usr/bin/env python3
"""
恢复配置数据脚本
从 JSON 备份文件恢复配置到新表结构
"""

import json
import sys
from datetime import datetime
from pathlib import Path

# 添加项目根目录到 Python 路径
backend_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_dir))

import polars as pl
from app.core.logger import logger
from infrastructure.database.dolphindb_client import db_client


def find_latest_backup(backup_dir: Path, table_name: str) -> Path:
    """
    查找指定表的最新备份文件

    Args:
        backup_dir: 备份目录
        table_name: 表名

    Returns:
        备份文件路径
    """
    backup_files = list(backup_dir.glob(f"{table_name}_*.json"))
    if not backup_files:
        raise FileNotFoundError(f"未找到表 {table_name} 的备份文件")

    # 按文件名排序，最新的在最后
    backup_files.sort()
    return backup_files[-1]


def restore_table(table_name: str, backup_file: Path, primary_keys: list) -> dict:
    """
    恢复单个表的数据

    Args:
        table_name: 表名
        backup_file: 备份文件路径
        primary_keys: 主键列表

    Returns:
        恢复统计信息
    """
    try:
        logger.info(f"正在恢复表: {table_name} <- {backup_file.name}")

        # 读取备份文件
        with open(backup_file, "r", encoding="utf-8") as f:
            records = json.load(f)

        if not records:
            logger.warning(f"备份文件为空，跳过恢复: {backup_file}")
            return {"table": table_name, "rows": 0, "status": "empty"}

        # 移除版本管理字段
        version_fields = [
            "version_number",
            "is_current",
            "changed_by",
            "change_reason",
            "effective_from",
            "effective_to",
        ]

        cleaned_records = []
        for record in records:
            cleaned_record = {
                k: v for k, v in record.items()
                if k not in version_fields
            }
            cleaned_records.append(cleaned_record)

        # 转换为 Polars DataFrame
        df = pl.DataFrame(cleaned_records)

        # 处理日期时间字段
        for col in df.columns:
            if df[col].dtype == pl.Utf8:
                # 尝试解析 ISO 格式的日期时间
                try:
                    if any(df[col].str.contains("T").fill_null(False)):
                        df = df.with_columns(
                            pl.col(col).str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f")
                        )
                except:
                    pass

        # 使用 upsert 写入数据
        db_client.upsert(table_name, df, primary_keys)

        logger.info(f"✓ 已恢复 {len(cleaned_records)} 条记录到表: {table_name}")
        return {
            "table": table_name,
            "rows": len(cleaned_records),
            "file": str(backup_file),
            "status": "success"
        }

    except Exception as e:
        logger.error(f"✗ 恢复表 {table_name} 失败: {e}")
        return {"table": table_name, "rows": 0, "status": "error", "error": str(e)}


def main():
    """主函数"""
    print("=" * 60)
    print("配置数据恢复脚本")
    print("=" * 60)

    # 备份目录
    backup_dir = backend_dir / "backups"
    if not backup_dir.exists():
        print(f"错误: 备份目录不存在: {backup_dir}")
        return

    # 需要恢复的表及其主键
    tables_to_restore = [
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

    # 查找备份文件
    print("\n查找备份文件:")
    restore_plan = []
    for table_info in tables_to_restore:
        try:
            backup_file = find_latest_backup(backup_dir, table_info["name"])
            print(f"  ✓ {table_info['name']}: {backup_file.name}")
            restore_plan.append({
                "table": table_info["name"],
                "file": backup_file,
                "primary_keys": table_info["primary_keys"],
            })
        except FileNotFoundError as e:
            print(f"  ✗ {table_info['name']}: {e}")

    if not restore_plan:
        print("\n错误: 未找到任何备份文件")
        return

    # 确认操作
    print(f"\n将恢复 {len(restore_plan)} 个表的数据")
    response = input("是否继续? (yes/no): ").strip().lower()
    if response != "yes":
        print("操作已取消")
        return

    # 执行恢复
    print("\n开始恢复...")
    results = []
    for plan in restore_plan:
        result = restore_table(
            plan["table"],
            plan["file"],
            plan["primary_keys"]
        )
        results.append(result)

    # 生成恢复报告
    print("\n" + "=" * 60)
    print("恢复报告")
    print("=" * 60)

    total_rows = 0
    success_count = 0

    for result in results:
        status_icon = "✓" if result["status"] == "success" else "✗" if result["status"] == "error" else "○"
        print(f"{status_icon} {result['table']}: {result['rows']} 条记录")
        if result["status"] == "success":
            success_count += 1
            total_rows += result["rows"]
        elif result["status"] == "error":
            print(f"  错误: {result['error']}")

    print(f"\n总计: {success_count}/{len(restore_plan)} 个表恢复成功，共 {total_rows} 条记录")
    print("=" * 60)


if __name__ == "__main__":
    main()
