#!/usr/bin/env python3
"""
验证备份文件脚本
读取备份 JSON 文件并验证数据格式、完整性
"""

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# 添加项目根目录到 Python 路径
backend_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_dir))

from app.core.logger import logger


class BackupValidator:
    """备份文件验证器"""

    # 必需字段定义
    REQUIRED_FIELDS = {
        "sync_task_config": [
            "task_id",
            "api_name",
            "description",
            "sync_type",
            "table_name",
            "enabled",
        ],
        "etl_task_config": [
            "task_id",
            "description",
            "source_tables",
            "target_table",
            "enabled",
        ],
        "factor_metadata": [
            "factor_id",
            "description",
            "category",
            "compute_mode",
            "depends_on",
            "enabled",
        ],
    }

    def __init__(self, backup_file: Path):
        self.backup_file = backup_file
        self.table_name = self._extract_table_name()
        self.data: List[Dict[str, Any]] = []
        self.validation_results: List[Dict[str, Any]] = []

    def _extract_table_name(self) -> str:
        """从文件名提取表名"""
        filename = self.backup_file.stem
        # 格式: table_name_YYYYMMDD_HHMMSS
        parts = filename.split("_")
        if len(parts) >= 3:
            # 去掉最后两个部分（日期和时间）
            return "_".join(parts[:-2])
        return filename

    def load_backup(self) -> bool:
        """加载备份文件"""
        try:
            logger.info(f"加载备份文件: {self.backup_file}")
            with open(self.backup_file, "r", encoding="utf-8") as f:
                self.data = json.load(f)

            if not isinstance(self.data, list):
                logger.error("备份文件格式错误: 应为 JSON 数组")
                return False

            logger.info(f"成功加载 {len(self.data)} 条记录")
            return True

        except json.JSONDecodeError as e:
            logger.error(f"JSON 解析失败: {e}")
            return False
        except Exception as e:
            logger.error(f"加载备份文件失败: {e}")
            return False

    def validate_format(self) -> Dict[str, Any]:
        """验证数据格式"""
        result = {
            "check": "数据格式",
            "status": "pass",
            "message": "",
            "details": [],
        }

        if not self.data:
            result["status"] = "warning"
            result["message"] = "备份文件为空"
            return result

        # 检查每条记录是否为字典
        for idx, record in enumerate(self.data):
            if not isinstance(record, dict):
                result["status"] = "fail"
                result["details"].append(f"记录 {idx} 不是字典类型")

        if result["status"] == "pass":
            result["message"] = f"所有 {len(self.data)} 条记录格式正确"

        return result

    def validate_required_fields(self) -> Dict[str, Any]:
        """验证必需字段"""
        result = {
            "check": "必需字段",
            "status": "pass",
            "message": "",
            "details": [],
        }

        if not self.data:
            result["status"] = "skip"
            result["message"] = "无数据可验证"
            return result

        # 获取该表的必需字段
        required_fields = self.REQUIRED_FIELDS.get(self.table_name, [])
        if not required_fields:
            result["status"] = "skip"
            result["message"] = f"未定义表 {self.table_name} 的必需字段"
            return result

        # 检查每条记录
        missing_fields_count = 0
        for idx, record in enumerate(self.data):
            missing = [f for f in required_fields if f not in record]
            if missing:
                missing_fields_count += 1
                if len(result["details"]) < 5:  # 只显示前5个错误
                    result["details"].append(
                        f"记录 {idx} 缺少字段: {', '.join(missing)}"
                    )

        if missing_fields_count > 0:
            result["status"] = "fail"
            result["message"] = f"{missing_fields_count} 条记录缺少必需字段"
        else:
            result["message"] = f"所有记录包含必需字段: {', '.join(required_fields)}"

        return result

    def validate_data_types(self) -> Dict[str, Any]:
        """验证数据类型"""
        result = {
            "check": "数据类型",
            "status": "pass",
            "message": "",
            "details": [],
        }

        if not self.data:
            result["status"] = "skip"
            result["message"] = "无数据可验证"
            return result

        # 检查常见字段的数据类型
        type_errors = 0
        for idx, record in enumerate(self.data):
            # 检查 enabled 字段应为布尔值
            if "enabled" in record and not isinstance(record["enabled"], bool):
                type_errors += 1
                if len(result["details"]) < 5:
                    result["details"].append(
                        f"记录 {idx}: enabled 应为布尔值，实际为 {type(record['enabled']).__name__}"
                    )

            # 检查 params 字段应为字典
            if "params" in record and record["params"] is not None:
                if not isinstance(record["params"], (dict, str)):
                    type_errors += 1
                    if len(result["details"]) < 5:
                        result["details"].append(
                            f"记录 {idx}: params 应为字典或字符串，实际为 {type(record['params']).__name__}"
                        )

        if type_errors > 0:
            result["status"] = "fail"
            result["message"] = f"{type_errors} 条记录存在类型错误"
        else:
            result["message"] = "数据类型验证通过"

        return result

    def validate_unique_keys(self) -> Dict[str, Any]:
        """验证主键唯一性"""
        result = {
            "check": "主键唯一性",
            "status": "pass",
            "message": "",
            "details": [],
        }

        if not self.data:
            result["status"] = "skip"
            result["message"] = "无数据可验证"
            return result

        # 确定主键字段
        primary_key_map = {
            "sync_task_config": "task_id",
            "etl_task_config": "task_id",
            "factor_metadata": "factor_id",
        }

        primary_key = primary_key_map.get(self.table_name)
        if not primary_key:
            result["status"] = "skip"
            result["message"] = f"未定义表 {self.table_name} 的主键"
            return result

        # 检查主键唯一性
        seen_keys = set()
        duplicates = []

        for idx, record in enumerate(self.data):
            if primary_key not in record:
                continue

            key_value = record[primary_key]
            if key_value in seen_keys:
                duplicates.append(key_value)
                if len(result["details"]) < 5:
                    result["details"].append(f"重复的 {primary_key}: {key_value}")
            else:
                seen_keys.add(key_value)

        if duplicates:
            result["status"] = "fail"
            result["message"] = f"发现 {len(duplicates)} 个重复的主键"
        else:
            result["message"] = f"所有 {len(seen_keys)} 个主键唯一"

        return result

    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        if not self.data:
            return {"total_records": 0}

        stats = {
            "total_records": len(self.data),
            "enabled_count": sum(1 for r in self.data if r.get("enabled", False)),
            "disabled_count": sum(1 for r in self.data if not r.get("enabled", True)),
        }

        # 按类别统计（如果有 category 字段）
        if self.data and "category" in self.data[0]:
            categories = {}
            for record in self.data:
                cat = record.get("category", "unknown")
                categories[cat] = categories.get(cat, 0) + 1
            stats["by_category"] = categories

        return stats

    def run_all_validations(self) -> List[Dict[str, Any]]:
        """运行所有验证"""
        validations = [
            self.validate_format(),
            self.validate_required_fields(),
            self.validate_data_types(),
            self.validate_unique_keys(),
        ]

        self.validation_results = validations
        return validations


def find_latest_backup(backup_dir: Path, table_name: str) -> Path:
    """查找指定表的最新备份文件"""
    backup_files = list(backup_dir.glob(f"{table_name}_*.json"))
    if not backup_files:
        raise FileNotFoundError(f"未找到表 {table_name} 的备份文件")

    # 按文件名排序，最新的在最后
    backup_files.sort()
    return backup_files[-1]


def print_validation_result(result: Dict[str, Any]):
    """打印验证结果"""
    status_icons = {
        "pass": "✓",
        "fail": "✗",
        "warning": "⚠",
        "skip": "○",
    }

    icon = status_icons.get(result["status"], "?")
    print(f"\n{icon} {result['check']}: {result['message']}")

    if result.get("details"):
        for detail in result["details"]:
            print(f"    - {detail}")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="验证备份文件")
    parser.add_argument(
        "--file",
        type=str,
        help="备份文件路径（如果不指定，则验证最新的备份）",
    )
    parser.add_argument(
        "--table",
        type=str,
        choices=["sync_task_config", "etl_task_config", "factor_metadata", "all"],
        default="all",
        help="要验证的表（默认: all）",
    )
    parser.add_argument(
        "--backup-dir",
        type=str,
        default=str(backend_dir / "backups"),
        help="备份目录（默认: backend/backups）",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("备份文件验证脚本")
    print("=" * 60)

    backup_dir = Path(args.backup_dir)
    if not backup_dir.exists():
        print(f"错误: 备份目录不存在: {backup_dir}")
        sys.exit(1)

    # 确定要验证的文件
    files_to_validate = []

    if args.file:
        # 验证指定文件
        file_path = Path(args.file)
        if not file_path.exists():
            print(f"错误: 文件不存在: {file_path}")
            sys.exit(1)
        files_to_validate.append(file_path)
    else:
        # 验证最新备份
        tables = (
            ["sync_task_config", "etl_task_config", "factor_metadata"]
            if args.table == "all"
            else [args.table]
        )

        for table_name in tables:
            try:
                latest_file = find_latest_backup(backup_dir, table_name)
                files_to_validate.append(latest_file)
            except FileNotFoundError as e:
                print(f"警告: {e}")

    if not files_to_validate:
        print("错误: 未找到任何备份文件")
        sys.exit(1)

    # 验证每个文件
    all_passed = True
    for backup_file in files_to_validate:
        print(f"\n{'=' * 60}")
        print(f"验证文件: {backup_file.name}")
        print(f"{'=' * 60}")

        validator = BackupValidator(backup_file)

        # 加载备份
        if not validator.load_backup():
            print("✗ 加载备份文件失败")
            all_passed = False
            continue

        # 运行验证
        results = validator.run_all_validations()

        # 打印结果
        for result in results:
            print_validation_result(result)
            if result["status"] == "fail":
                all_passed = False

        # 打印统计信息
        stats = validator.get_statistics()
        print(f"\n统计信息:")
        print(f"  总记录数: {stats['total_records']}")
        if "enabled_count" in stats:
            print(f"  启用: {stats['enabled_count']}")
            print(f"  禁用: {stats['disabled_count']}")
        if "by_category" in stats:
            print(f"  按类别:")
            for cat, count in stats["by_category"].items():
                print(f"    - {cat}: {count}")

    # 总结
    print("\n" + "=" * 60)
    if all_passed:
        print("✓ 所有验证通过!")
    else:
        print("✗ 部分验证失败，请检查上述错误")
    print("=" * 60)

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
