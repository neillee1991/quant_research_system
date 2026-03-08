#!/usr/bin/env python3
"""
测试任务执行脚本
验证同步任务配置正确性，不实际同步数据
"""
import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional

# 添加项目路径
backend_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(backend_dir))

from store.dolphindb_client import db_client
from data_manager.sync_components import SyncConfigManager
from app.core.logger import logger


class TaskExecutionTester:
    """任务执行测试器"""

    def __init__(self):
        self.config_manager = SyncConfigManager()
        self.test_results = []

    def test_task_config(self, task_id: str) -> bool:
        """测试任务配置"""
        print("\n" + "=" * 80)
        print(f"测试任务: {task_id}")
        print("=" * 80)

        try:
            # 获取任务配置
            task = self.config_manager.get_task(task_id)
            if not task:
                print(f"❌ 任务不存在: {task_id}")
                return False

            print(f"\n✅ 任务配置加载成功")
            print(f"   描述: {task.get('description', '')}")
            print(f"   类型: {task.get('sync_type', '')}")
            print(f"   表名: {task.get('table_name', '')}")
            print(f"   API: {task.get('api_name', '')}")

            # 验证配置字段
            success = True
            success &= self._verify_schema(task)
            success &= self._verify_primary_keys(task)
            success &= self._verify_params(task)
            success &= self._verify_table_exists(task)

            if success:
                print(f"\n✅ 任务 {task_id} 配置验证通过")
            else:
                print(f"\n❌ 任务 {task_id} 配置验证失败")

            return success

        except Exception as e:
            print(f"❌ 测试失败: {e}")
            logger.exception(f"测试任务 {task_id} 失败")
            return False

    def _verify_schema(self, task: Dict) -> bool:
        """验证 schema 配置"""
        print("\n检查 Schema 配置...")

        schema_json = task.get("schema_json", "")
        if not schema_json:
            print("   ❌ 缺少 schema_json")
            return False

        try:
            schema = json.loads(schema_json)
            if not schema:
                print("   ❌ schema 为空")
                return False

            print(f"   ✅ Schema 包含 {len(schema)} 个字段:")
            for field_name, field_def in list(schema.items())[:5]:
                field_type = field_def.get("type", "UNKNOWN")
                print(f"      - {field_name}: {field_type}")

            if len(schema) > 5:
                print(f"      ... 还有 {len(schema) - 5} 个字段")

            return True

        except json.JSONDecodeError as e:
            print(f"   ❌ schema_json 格式错误: {e}")
            return False

    def _verify_primary_keys(self, task: Dict) -> bool:
        """验证主键配置"""
        print("\n检查主键配置...")

        pk_json = task.get("primary_keys_json", "")
        if not pk_json:
            print("   ❌ 缺少 primary_keys_json")
            return False

        try:
            pks = json.loads(pk_json)
            if not pks:
                print("   ❌ 主键列表为空")
                return False

            print(f"   ✅ 主键: {', '.join(pks)}")

            # 验证主键字段在 schema 中存在
            schema_json = task.get("schema_json", "")
            if schema_json:
                schema = json.loads(schema_json)
                for pk in pks:
                    if pk not in schema:
                        print(f"   ⚠️  主键 {pk} 不在 schema 中")

            return True

        except json.JSONDecodeError as e:
            print(f"   ❌ primary_keys_json 格式错误: {e}")
            return False

    def _verify_params(self, task: Dict) -> bool:
        """验证参数配置"""
        print("\n检查参数配置...")

        params_json = task.get("params_json", "")
        if not params_json:
            print("   ⚠️  缺少 params_json")
            return True  # params 可以为空

        try:
            params = json.loads(params_json)
            print(f"   ✅ 参数配置:")
            for key, value in params.items():
                # 截断长值
                value_str = str(value)
                if len(value_str) > 50:
                    value_str = value_str[:47] + "..."
                print(f"      - {key}: {value_str}")

            return True

        except json.JSONDecodeError as e:
            print(f"   ❌ params_json 格式错误: {e}")
            return False

    def _verify_table_exists(self, task: Dict) -> bool:
        """验证目标表是否存在"""
        print("\n检查目标表...")

        table_name = task.get("table_name", "")
        if not table_name:
            print("   ❌ 缺少 table_name")
            return False

        try:
            # 尝试查询表（限制 1 行）
            query = f"SELECT * FROM {table_name} LIMIT 1"
            df = db_client.query(query)

            print(f"   ✅ 表 {table_name} 存在")
            print(f"   表结构: {len(df.columns)} 列")

            # 验证表结构与 schema 匹配
            schema_json = task.get("schema_json", "")
            if schema_json:
                schema = json.loads(schema_json)
                missing_cols = set(schema.keys()) - set(df.columns)
                extra_cols = set(df.columns) - set(schema.keys())

                if missing_cols:
                    print(f"   ⚠️  表中缺少字段: {', '.join(missing_cols)}")
                if extra_cols:
                    print(f"   ⚠️  表中多余字段: {', '.join(extra_cols)}")

            return True

        except Exception as e:
            print(f"   ❌ 表不存在或查询失败: {e}")
            return False

    def test_sync_log(self, task_id: str) -> None:
        """测试同步日志"""
        print("\n" + "-" * 80)
        print("检查同步历史...")

        try:
            query = f"""
            SELECT task_id, last_date, record_count, updated_at
            FROM sync_log
            WHERE task_id = '{task_id}'
            ORDER BY updated_at DESC
            LIMIT 5
            """
            df = db_client.query(query)

            if df.is_empty():
                print("   ℹ️  暂无同步记录")
            else:
                print(f"   ✅ 找到 {len(df)} 条同步记录:")
                for row in df.to_dicts():
                    last_date = row.get("last_date", "")
                    record_count = row.get("record_count", 0)
                    updated_at = row.get("updated_at", "")
                    print(f"      - {last_date}: {record_count} 条记录 (同步于 {updated_at})")

        except Exception as e:
            print(f"   ⚠️  查询同步日志失败: {e}")

    def run_all_tests(self) -> bool:
        """运行所有测试"""
        print("=" * 80)
        print("任务执行测试")
        print("=" * 80)

        # 测试数据库连接
        print("\n检查数据库连接...")
        try:
            db_client.query("SELECT 1 as test")
            print("✅ 数据库连接正常")
        except Exception as e:
            print(f"❌ 数据库连接失败: {e}")
            return False

        # 测试简单任务
        test_tasks = [
            "sync_trade_cal",      # 全量同步，数据量小
            "sync_stock_basic",    # 全量同步，基础数据
            "sync_daily_data",     # 增量同步，日线数据
        ]

        print(f"\n将测试 {len(test_tasks)} 个任务:")
        for task_id in test_tasks:
            print(f"   - {task_id}")

        all_success = True
        for task_id in test_tasks:
            success = self.test_task_config(task_id)
            self.test_sync_log(task_id)
            all_success &= success

        # 打印摘要
        print("\n" + "=" * 80)
        print("测试摘要")
        print("=" * 80)

        if all_success:
            print("\n✅ 所有任务配置验证通过！")
            print("\n提示:")
            print("   - 配置验证通过，可以执行实际同步")
            print("   - 使用 API 执行同步: POST /api/v1/data/sync/task/{task_id}")
            print("   - 或使用命令行: python -m data_manager.refactored_sync_engine")
        else:
            print("\n❌ 部分任务配置验证失败")
            print("\n建议:")
            print("   - 检查数据库表结构")
            print("   - 验证 schema_json 和 primary_keys_json 格式")
            print("   - 运行 verify_seed_tasks.py 查看详细信息")

        return all_success


def main():
    """主函数"""
    tester = TaskExecutionTester()

    try:
        success = tester.run_all_tests()
        sys.exit(0 if success else 1)

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        logger.exception("测试失败")
        sys.exit(1)


if __name__ == "__main__":
    main()
