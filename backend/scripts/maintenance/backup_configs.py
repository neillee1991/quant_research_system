#!/usr/bin/env python3
"""
备份配置数据脚本
从 DolphinDB 导出当前配置到 JSON 文件
"""

import json
import sys
from datetime import datetime
from pathlib import Path

# 添加项目根目录到 Python 路径
backend_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_dir))

from app.core.logger import logger
from store.dolphindb_client import db_client


def backup_table(table_name: str, backup_dir: Path) -> dict:
    """
    备份单个表的数据

    Args:
        table_name: 表名
        backup_dir: 备份目录

    Returns:
        备份统计信息
    """
    try:
        logger.info(f"正在备份表: {table_name}")

        # 查询所有数据
        df = db_client.query(f"SELECT * FROM {table_name}")

        if df.is_empty():
            logger.warning(f"表 {table_name} 为空，跳过备份")
            return {"table": table_name, "rows": 0, "status": "empty"}

        # 转换为字典列表
        records = df.to_dicts()

        # 处理日期时间类型
        for record in records:
            for key, value in record.items():
                if isinstance(value, datetime):
                    record[key] = value.isoformat()

        # 保存到 JSON 文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_dir / f"{table_name}_{timestamp}.json"

        with open(backup_file, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        logger.info(f"✓ 已备份 {len(records)} 条记录到: {backup_file}")
        return {
            "table": table_name,
            "rows": len(records),
            "file": str(backup_file),
            "status": "success"
        }

    except Exception as e:
        logger.error(f"✗ 备份表 {table_name} 失败: {e}")
        return {"table": table_name, "rows": 0, "status": "error", "error": str(e)}


def main():
    """主函数"""
    print("=" * 60)
    print("配置数据备份脚本")
    print("=" * 60)

    # 创建备份目录
    backup_dir = backend_dir / "backups"
    backup_dir.mkdir(exist_ok=True)

    # 需要备份的表
    tables_to_backup = [
        "sync_task_config",
        "etl_task_config",
        "factor_metadata",
    ]

    # 执行备份
    results = []
    for table_name in tables_to_backup:
        result = backup_table(table_name, backup_dir)
        results.append(result)

    # 生成备份报告
    print("\n" + "=" * 60)
    print("备份报告")
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

    print(f"\n总计: {success_count}/{len(tables_to_backup)} 个表备份成功，共 {total_rows} 条记录")
    print(f"备份目录: {backup_dir}")
    print("=" * 60)

    # 保存备份元数据
    metadata = {
        "backup_time": datetime.now().isoformat(),
        "tables": results,
        "total_rows": total_rows,
    }

    metadata_file = backup_dir / f"backup_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    print(f"\n备份元数据已保存: {metadata_file}")


if __name__ == "__main__":
    main()
