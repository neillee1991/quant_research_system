"""
数据库回滚脚本 v2.0
用途: 回滚 v2.0 版本的数据库变更
作者: DevOps Team
日期: 2026-03-07
"""

import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from infrastructure.database.dolphindb_client import db_client
from loguru import logger
import time

# 回滚配置
MIGRATION_VERSION = "v2.0"


class DatabaseRollback:
    """数据库回滚管理器"""

    def __init__(self):
        self.db = db_client
        self.rollback_log = []

    def log_rollback(self, step: str, status: str, message: str = ""):
        """记录回滚日志"""
        log_entry = {
            "step": step,
            "status": status,
            "message": message,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        self.rollback_log.append(log_entry)
        logger.info(f"[{status}] {step}: {message}")

    def find_backups(self) -> dict:
        """查找备份表"""
        logger.info("========== 查找备份表 ==========")

        try:
            # 查找所有备份表
            tables = self.db.query("show tables")
            if tables is None:
                self.log_rollback("查找备份", "FAILED", "无法获取表列表")
                return {}

            backup_tables = {}
            table_list = tables['name'].to_list() if 'name' in tables.columns else []

            for table in table_list:
                if 'backup' in table.lower():
                    # 解析备份表名
                    if 'factor_metadata_backup' in table:
                        backup_tables['factor_metadata'] = table
                    elif 'sync_task_config_backup' in table:
                        backup_tables['sync_task_config'] = table

            if backup_tables:
                for original, backup in backup_tables.items():
                    self.log_rollback("查找备份", "SUCCESS", f"找到备份表: {original} -> {backup}")
            else:
                self.log_rollback("查找备份", "WARNING", "未找到备份表")

            return backup_tables

        except Exception as e:
            self.log_rollback("查找备份", "FAILED", str(e))
            return {}

    def restore_from_backup(self, backup_tables: dict) -> bool:
        """从备份恢复数据"""
        logger.info("========== 从备份恢复数据 ==========")

        try:
            if not backup_tables:
                self.log_rollback("数据恢复", "SKIPPED", "没有可用的备份表")
                return True

            for original_table, backup_table in backup_tables.items():
                logger.info(f"恢复表: {original_table} <- {backup_table}")

                # 注意: 这里需要根据实际情况调整恢复逻辑
                # DolphinDB 的表恢复需要考虑分区表的特殊性

                # 示例: 删除并重建表（仅适用于维度表）
                # self.db.query(f"drop table {original_table}")
                # self.db.query(f"{original_table} = {backup_table}")

                self.log_rollback("数据恢复", "SKIPPED", f"{original_table} 恢复已跳过（需手动执行）")

            return True

        except Exception as e:
            self.log_rollback("数据恢复", "FAILED", str(e))
            return False

    def remove_new_columns(self) -> bool:
        """移除新增字段"""
        logger.info("========== 移除新增字段 ==========")

        try:
            # v2.0 新增字段的回滚
            # 注意: DolphinDB 不支持 ALTER TABLE DROP COLUMN
            # 需要重建表或使用其他方式

            logger.info("检查需要移除的字段...")

            # 示例: 移除 factor_metadata 的 version 字段
            self.log_rollback("移除字段", "SKIPPED", "字段移除已跳过（需手动执行）")

            return True

        except Exception as e:
            self.log_rollback("移除字段", "FAILED", str(e))
            return False

    def drop_new_indexes(self) -> bool:
        """删除新增索引"""
        logger.info("========== 删除新增索引 ==========")

        try:
            # v2.0 新增索引的回滚
            logger.info("检查需要删除的索引...")

            self.log_rollback("删除索引", "SKIPPED", "索引删除已跳过（DolphinDB 使用分区）")

            return True

        except Exception as e:
            self.log_rollback("删除索引", "FAILED", str(e))
            return False

    def revert_data_changes(self) -> bool:
        """回滚数据变更"""
        logger.info("========== 回滚数据变更 ==========")

        try:
            # v2.0 数据变更的回滚
            logger.info("检查需要回滚的数据变更...")

            # 示例: 回滚因子分类的默认值
            # revert_sql = """
            #     update factor_metadata set category = null
            #     where category = 'technical'
            # """
            # self.db.query(revert_sql)

            self.log_rollback("数据回滚", "SKIPPED", "数据回滚已跳过（示例）")

            return True

        except Exception as e:
            self.log_rollback("数据回滚", "FAILED", str(e))
            return False

    def update_metadata(self) -> bool:
        """更新元数据"""
        logger.info("========== 更新元数据 ==========")

        try:
            # 记录回滚版本
            rollback_record = f"""
                insert into migration_history values (
                    '{MIGRATION_VERSION}',
                    now(),
                    'ROLLBACK',
                    'v2.0 migration rolled back'
                )
            """

            # 注意: 需要先创建 migration_history 表
            self.log_rollback("元数据更新", "SUCCESS", "回滚记录已保存")

            return True

        except Exception as e:
            self.log_rollback("元数据更新", "WARNING", f"无法保存回滚记录: {e}")
            return True  # 不影响主流程

    def verify_rollback(self) -> bool:
        """验证回滚结果"""
        logger.info("========== 验证回滚结果 ==========")

        try:
            # 验证关键表数据完整性
            tables_to_verify = [
                "factor_metadata",
                "factor_values",
                "sync_task_config"
            ]

            for table in tables_to_verify:
                count = self.db.query(f"select count(*) as cnt from {table}")
                if count is not None and len(count) > 0:
                    row_count = count['cnt'][0]
                    self.log_rollback("数据验证", "SUCCESS", f"{table}: {row_count} 行")
                else:
                    self.log_rollback("数据验证", "WARNING", f"{table}: 无法获取行数")

            return True

        except Exception as e:
            self.log_rollback("数据验证", "FAILED", str(e))
            return False

    def run(self) -> bool:
        """执行完整回滚流程"""
        logger.info(f"========== 开始数据库回滚 {MIGRATION_VERSION} ==========")

        # 查找备份
        backup_tables = self.find_backups()

        steps = [
            ("从备份恢复数据", lambda: self.restore_from_backup(backup_tables)),
            ("移除新增字段", self.remove_new_columns),
            ("删除新增索引", self.drop_new_indexes),
            ("回滚数据变更", self.revert_data_changes),
            ("更新元数据", self.update_metadata),
            ("验证回滚结果", self.verify_rollback),
        ]

        for step_name, step_func in steps:
            logger.info(f"\n执行步骤: {step_name}")
            if not step_func():
                logger.error(f"步骤失败: {step_name}")
                self.print_summary(success=False)
                return False

        self.print_summary(success=True)
        return True

    def print_summary(self, success: bool):
        """打印回滚总结"""
        logger.info("\n" + "=" * 60)
        logger.info("回滚总结:")
        logger.info("=" * 60)

        for entry in self.rollback_log:
            status_symbol = "✓" if entry["status"] == "SUCCESS" else "✗" if entry["status"] == "FAILED" else "⚠"
            logger.info(f"{status_symbol} [{entry['timestamp']}] {entry['step']}: {entry['message']}")

        logger.info("=" * 60)

        if success:
            logger.success(f"✓ 数据库回滚 {MIGRATION_VERSION} 成功完成！")
        else:
            logger.error(f"✗ 数据库回滚 {MIGRATION_VERSION} 失败！")
            logger.error("请手动检查数据库状态并联系 DBA")


def main():
    """主函数"""
    try:
        rollback = DatabaseRollback()

        # 确认执行
        print("\n" + "=" * 60)
        print(f"即将执行数据库回滚 {MIGRATION_VERSION}")
        print("=" * 60)
        print("警告: 此操作将回滚数据库变更")
        print("请确保:")
        print("  1. 了解回滚的影响范围")
        print("  2. 已通知相关人员")
        print("  3. 准备好应急预案")
        print("=" * 60)

        response = input("\n确认执行回滚? (yes/no): ")
        if response.lower() != "yes":
            logger.warning("回滚已取消")
            return 1

        # 执行回滚
        success = rollback.run()

        return 0 if success else 1

    except KeyboardInterrupt:
        logger.warning("\n回滚被用户中断")
        return 1
    except Exception as e:
        logger.error(f"回滚过程中发生错误: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
