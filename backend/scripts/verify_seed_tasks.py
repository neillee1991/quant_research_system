#!/usr/bin/env python3
"""
验证种子任务完整性脚本
检查 sync_task_config 和 etl_task_config 表中的任务配置
"""
import sys
import json
from pathlib import Path
from typing import Dict, List, Tuple

# 添加项目路径
backend_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(backend_dir))

from store.dolphindb_client import db_client
from app.core.logger import logger


class TaskVerifier:
    """任务验证器"""

    def __init__(self):
        self.errors = []
        self.warnings = []

    def verify_sync_tasks(self) -> Tuple[bool, List[Dict]]:
        """验证 sync_task_config 表"""
        print("\n" + "=" * 80)
        print("验证 sync_task_config 表")
        print("=" * 80)

        try:
            # 查询所有任务
            query = """
            SELECT task_id, description, sync_type, table_name,
                   date_field, enabled, schema_json, primary_keys_json
            FROM sync_task_config
            ORDER BY task_id
            """
            df = db_client.query(query)

            if df.is_empty():
                print("❌ sync_task_config 表为空")
                self.errors.append("sync_task_config 表为空")
                return False, []

            tasks = df.to_dicts()
            print(f"\n✅ 找到 {len(tasks)} 个同步任务")

            # 验证任务数量
            expected_count = 17
            if len(tasks) != expected_count:
                print(f"⚠️  预期 {expected_count} 个任务，实际 {len(tasks)} 个")
                self.warnings.append(f"任务数量不匹配: 预期 {expected_count}, 实际 {len(tasks)}")

            # 验证每个任务
            print("\n" + "-" * 80)
            print("任务详情:")
            print("-" * 80)

            for i, task in enumerate(tasks, 1):
                task_id = task.get("task_id", "")
                description = task.get("description", "")
                sync_type = task.get("sync_type", "")
                table_name = task.get("table_name", "")
                date_field = task.get("date_field", "")
                enabled = task.get("enabled", False)

                # 打印任务信息
                status = "✅" if enabled else "⏸️ "
                print(f"\n{i}. {status} {task_id}")
                print(f"   描述: {description}")
                print(f"   类型: {sync_type}")
                print(f"   表名: {table_name}")
                if date_field:
                    print(f"   日期字段: {date_field}")

                # 验证关键字段
                self._verify_task_fields(task, "sync")

            return True, tasks

        except Exception as e:
            print(f"❌ 查询失败: {e}")
            self.errors.append(f"查询 sync_task_config 失败: {e}")
            return False, []

    def verify_etl_tasks(self) -> Tuple[bool, List[Dict]]:
        """验证 etl_task_config 表"""
        print("\n" + "=" * 80)
        print("验证 etl_task_config 表")
        print("=" * 80)

        try:
            # 查询所有任务
            query = """
            SELECT task_id, description, source_table, target_table,
                   enabled, schema_json, primary_keys_json
            FROM etl_task_config
            ORDER BY task_id
            """
            df = db_client.query(query)

            if df.is_empty():
                print("❌ etl_task_config 表为空")
                self.errors.append("etl_task_config 表为空")
                return False, []

            tasks = df.to_dicts()
            print(f"\n✅ 找到 {len(tasks)} 个 ETL 任务")

            # 验证任务数量
            expected_count = 3
            if len(tasks) != expected_count:
                print(f"⚠️  预期 {expected_count} 个任务，实际 {len(tasks)} 个")
                self.warnings.append(f"ETL 任务数量不匹配: 预期 {expected_count}, 实际 {len(tasks)}")

            # 验证每个任务
            print("\n" + "-" * 80)
            print("任务详情:")
            print("-" * 80)

            for i, task in enumerate(tasks, 1):
                task_id = task.get("task_id", "")
                description = task.get("description", "")
                source_table = task.get("source_table", "")
                target_table = task.get("target_table", "")
                enabled = task.get("enabled", False)

                # 打印任务信息
                status = "✅" if enabled else "⏸️ "
                print(f"\n{i}. {status} {task_id}")
                print(f"   描述: {description}")
                print(f"   源表: {source_table}")
                print(f"   目标表: {target_table}")

                # 验证关键字段
                self._verify_task_fields(task, "etl")

            return True, tasks

        except Exception as e:
            print(f"❌ 查询失败: {e}")
            self.errors.append(f"查询 etl_task_config 失败: {e}")
            return False, []

    def _verify_task_fields(self, task: Dict, task_type: str) -> None:
        """验证任务字段"""
        task_id = task.get("task_id", "")

        # 验证 schema_json
        schema_json = task.get("schema_json", "")
        if not schema_json:
            print(f"   ⚠️  缺少 schema_json")
            self.warnings.append(f"{task_id}: 缺少 schema_json")
        else:
            try:
                schema = json.loads(schema_json)
                print(f"   Schema: {len(schema)} 个字段")
            except json.JSONDecodeError as e:
                print(f"   ❌ schema_json 格式错误: {e}")
                self.errors.append(f"{task_id}: schema_json 格式错误")

        # 验证 primary_keys_json
        pk_json = task.get("primary_keys_json", "")
        if not pk_json:
            print(f"   ⚠️  缺少 primary_keys_json")
            self.warnings.append(f"{task_id}: 缺少 primary_keys_json")
        else:
            try:
                pks = json.loads(pk_json)
                print(f"   主键: {', '.join(pks)}")
            except json.JSONDecodeError as e:
                print(f"   ❌ primary_keys_json 格式错误: {e}")
                self.errors.append(f"{task_id}: primary_keys_json 格式错误")

        # 验证表名
        if task_type == "sync":
            table_name = task.get("table_name", "")
            if not table_name:
                print(f"   ❌ 缺少 table_name")
                self.errors.append(f"{task_id}: 缺少 table_name")
        elif task_type == "etl":
            source_table = task.get("source_table", "")
            target_table = task.get("target_table", "")
            if not source_table:
                print(f"   ❌ 缺少 source_table")
                self.errors.append(f"{task_id}: 缺少 source_table")
            if not target_table:
                print(f"   ❌ 缺少 target_table")
                self.errors.append(f"{task_id}: 缺少 target_table")

    def print_summary(self) -> bool:
        """打印验证摘要"""
        print("\n" + "=" * 80)
        print("验证摘要")
        print("=" * 80)

        if not self.errors and not self.warnings:
            print("\n✅ 所有检查通过！")
            return True

        if self.warnings:
            print(f"\n⚠️  发现 {len(self.warnings)} 个警告:")
            for warning in self.warnings:
                print(f"   - {warning}")

        if self.errors:
            print(f"\n❌ 发现 {len(self.errors)} 个错误:")
            for error in self.errors:
                print(f"   - {error}")
            return False

        return True


def main():
    """主函数"""
    print("=" * 80)
    print("种子任务验证脚本")
    print("=" * 80)

    try:
        # 测试数据库连接
        print("\n检查数据库连接...")
        db_client.query("SELECT 1 as test")
        print("✅ 数据库连接正常")

        # 创建验证器
        verifier = TaskVerifier()

        # 验证同步任务
        sync_ok, sync_tasks = verifier.verify_sync_tasks()

        # 验证 ETL 任务
        etl_ok, etl_tasks = verifier.verify_etl_tasks()

        # 打印摘要
        success = verifier.print_summary()

        # 返回状态码
        sys.exit(0 if success else 1)

    except Exception as e:
        print(f"\n❌ 验证失败: {e}")
        logger.exception("验证失败")
        sys.exit(1)


if __name__ == "__main__":
    main()
