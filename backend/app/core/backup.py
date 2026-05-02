"""
自动化备份策略

实现数据库定期备份和恢复功能
"""
import os
import time
import shutil
import subprocess
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

from app.core.logger import logger
from app.core.config import settings


class BackupManager:
    """备份管理器"""

    def __init__(self, backup_dir: Optional[Path] = None):
        self.backup_dir = backup_dir or settings.data_dir / "backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.pg_config = settings.postgresql

    def create_backup(
        self,
        backup_type: str = "full",
        databases: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        创建备份

        Args:
            backup_type: 备份类型 ("full", "config", "data")
            databases: 要备份的数据库列表

        Returns:
            备份信息字典
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_info = {
            "timestamp": timestamp,
            "type": backup_type,
            "status": "in_progress",
            "files": [],
            "errors": [],
        }

        try:
            # 备份 PostgreSQL
            pg_backup_file = self._backup_postgresql(timestamp)
            if pg_backup_file:
                backup_info["files"].append(str(pg_backup_file))
                logger.info(f"PostgreSQL backup created: {pg_backup_file}")

            # 保存备份元数据
            metadata_file = self.backup_dir / f"backup_{timestamp}_metadata.json"
            backup_info["status"] = "completed"
            backup_info["completed_at"] = datetime.now().isoformat()

            with open(metadata_file, "w") as f:
                json.dump(backup_info, f, indent=2)

            logger.info(f"Backup completed successfully: {backup_info}")
            return backup_info

        except Exception as e:
            logger.error(f"Backup failed: {e}")
            backup_info["status"] = "failed"
            backup_info["errors"].append(str(e))
            return backup_info

    def _backup_postgresql(self, timestamp: str) -> Optional[Path]:
        """
        备份 PostgreSQL 数据库

        使用 pg_dump 命令进行备份
        """
        try:
            output_file = self.backup_dir / f"postgresql_{timestamp}.sql"

            # 构建 pg_dump 命令
            cmd = [
                "pg_dump",
                "-h", self.pg_config.postgres_host,
                "-p", str(self.pg_config.postgres_port),
                "-U", self.pg_config.postgres_user,
                "-d", self.pg_config.postgres_db,
                "-F", "plain",  # 纯文本格式
                "-v",  # 详细输出
            ]

            # 设置密码环境变量
            env = os.environ.copy()
            env["PGPASSWORD"] = self.pg_config.postgres_password

            # 执行备份
            with open(output_file, "w") as f:
                result = subprocess.run(
                    cmd,
                    stdout=f,
                    stderr=subprocess.PIPE,
                    env=env,
                    timeout=300,  # 5分钟超时
                )

            if result.returncode != 0:
                error_msg = result.stderr.decode() if result.stderr else "Unknown error"
                logger.error(f"pg_dump failed: {error_msg}")
                output_file.unlink()  # 删除失败的备份文件
                return None

            # 验证备份文件
            if output_file.stat().st_size == 0:
                logger.warning("Backup file is empty")
                output_file.unlink()
                return None

            logger.info(f"PostgreSQL backup created: {output_file} ({output_file.stat().st_size} bytes)")
            return output_file

        except subprocess.TimeoutExpired:
            logger.error("PostgreSQL backup timed out")
            return None
        except FileNotFoundError:
            logger.error("pg_dump command not found. Please install PostgreSQL client tools.")
            return None
        except Exception as e:
            logger.error(f"PostgreSQL backup failed: {e}")
            return None

    def list_backups(self, backup_type: Optional[str] = None) -> List[Path]:
        """列出所有备份"""
        backups = []
        for backup_file in self.backup_dir.glob("*.sql"):
            if backup_type is None or backup_file.name.startswith(backup_type):
                backups.append(backup_file)
        return sorted(backups, key=lambda p: p.stat().st_mtime, reverse=True)

    def cleanup_old_backups(
        self,
        keep_days: int = 30,
        keep_count: int = 10
    ) -> int:
        """
        清理旧备份

        Args:
            keep_days: 保留天数
            keep_count: 保留数量

        Returns:
            删除的备份数量
        """
        backups = self.list_backups()
        deleted = 0
        cutoff_date = datetime.now() - timedelta(days=keep_days)

        for i, backup in enumerate(backups):
            # 检查是否超过保留数量或超过保留天数
            if i >= keep_count:
                mtime = datetime.fromtimestamp(backup.stat().st_mtime)
                if mtime < cutoff_date:
                    try:
                        backup.unlink()
                        deleted += 1
                        logger.info(f"Deleted old backup: {backup}")
                    except Exception as e:
                        logger.error(f"Failed to delete backup {backup}: {e}")

        return deleted

    def restore_backup(self, backup_path: Path) -> bool:
        """
        从备份恢复

        Args:
            backup_path: 备份文件路径

        Returns:
            是否恢复成功
        """
        if not backup_path.exists():
            logger.error(f"Backup file not found: {backup_path}")
            return False

        try:
            logger.info(f"Restoring backup from: {backup_path}")

            # 构建 psql 命令
            cmd = [
                "psql",
                "-h", self.pg_config.postgres_host,
                "-p", str(self.pg_config.postgres_port),
                "-U", self.pg_config.postgres_user,
                "-d", self.pg_config.postgres_db,
            ]

            # 设置密码环境变量
            env = os.environ.copy()
            env["PGPASSWORD"] = self.pg_config.postgres_password

            # 执行恢复
            with open(backup_path, "r") as f:
                result = subprocess.run(
                    cmd,
                    stdin=f,
                    stderr=subprocess.PIPE,
                    env=env,
                    timeout=600,  # 10分钟超时
                )

            if result.returncode != 0:
                error_msg = result.stderr.decode() if result.stderr else "Unknown error"
                logger.error(f"Restore failed: {error_msg}")
                return False

            logger.info(f"Backup restored successfully from: {backup_path}")
            return True

        except subprocess.TimeoutExpired:
            logger.error("Restore operation timed out")
            return False
        except FileNotFoundError:
            logger.error("psql command not found. Please install PostgreSQL client tools.")
            return False
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False

    def get_backup_info(self) -> Dict[str, Any]:
        """获取备份统计信息"""
        backups = self.list_backups()
        total_size = sum(b.stat().st_size for b in backups)

        return {
            "backup_dir": str(self.backup_dir),
            "total_backups": len(backups),
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "latest_backup": str(backups[0]) if backups else None,
            "oldest_backup": str(backups[-1]) if backups else None,
        }


# 全局备份管理器实例
_backup_manager: Optional[BackupManager] = None


def get_backup_manager() -> BackupManager:
    """获取备份管理器单例"""
    global _backup_manager
    if _backup_manager is None:
        _backup_manager = BackupManager()
    return _backup_manager


def schedule_backups():
    """
    配置定期备份任务

    建议在应用启动时调用，或者使用 cron 调度:
    - 每日备份: 0 2 * * * (每天凌晨2点)
    - 每周备份: 0 3 * * 0 (每周日凌晨3点)
    - 每月备份: 0 4 1 * * (每月1号凌晨4点)
    """
    logger.info("Backup scheduling configured")
    # 这里可以集成调度器来定期调用 create_backup()

