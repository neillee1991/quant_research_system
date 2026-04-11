#!/usr/bin/env python3
"""
数据库备份管理脚本

用法:
    python backup_manager.py create      # 创建备份
    python backup_manager.py list        # 列出备份
    python backup_manager.py cleanup     # 清理旧备份
    python backup_manager.py restore <file>  # 恢复备份
    python backup_manager.py info        # 显示备份信息
"""
import sys
import os
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

# 设置环境变量
os.environ.setdefault("ENVIRONMENT", "production")

import argparse
from app.core.backup import get_backup_manager
from app.core.logger import logger


def main():
    parser = argparse.ArgumentParser(description="数据库备份管理工具")
    subparsers = parser.add_subparsers(dest="command", help="命令")

    # create 命令
    create_parser = subparsers.add_parser("create", help="创建备份")
    create_parser.add_argument(
        "--type",
        default="full",
        choices=["full", "config", "data"],
        help="备份类型"
    )

    # list 命令
    subparsers.add_parser("list", help="列出所有备份")

    # cleanup 命令
    cleanup_parser = subparsers.add_parser("cleanup", help="清理旧备份")
    cleanup_parser.add_argument(
        "--keep-days",
        type=int,
        default=30,
        help="保留天数"
    )
    cleanup_parser.add_argument(
        "--keep-count",
        type=int,
        default=10,
        help="保留数量"
    )

    # restore 命令
    restore_parser = subparsers.add_parser("restore", help="恢复备份")
    restore_parser.add_argument("file", help="备份文件路径")

    # info 命令
    subparsers.add_parser("info", help="显示备份信息")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    manager = get_backup_manager()

    try:
        if args.command == "create":
            logger.info(f"Creating {args.type} backup...")
            result = manager.create_backup(backup_type=args.type)
            print(f"✓ Backup created successfully")
            print(f"  Status: {result['status']}")
            print(f"  Files: {', '.join(result['files'])}")
            return 0

        elif args.command == "list":
            backups = manager.list_backups()
            if not backups:
                print("No backups found")
                return 0

            print(f"Found {len(backups)} backup(s):")
            for i, backup in enumerate(backups, 1):
                size_mb = backup.stat().st_size / (1024 * 1024)
                print(f"  {i}. {backup.name} ({size_mb:.2f} MB)")
            return 0

        elif args.command == "cleanup":
            logger.info(f"Cleaning up backups older than {args.keep_days} days...")
            deleted = manager.cleanup_old_backups(
                keep_days=args.keep_days,
                keep_count=args.keep_count
            )
            print(f"✓ Deleted {deleted} old backup(s)")
            return 0

        elif args.command == "restore":
            backup_path = Path(args.file)
            if not backup_path.exists():
                print(f"✗ Backup file not found: {backup_path}")
                return 1

            logger.info(f"Restoring from {backup_path}...")
            success = manager.restore_backup(backup_path)
            if success:
                print(f"✓ Backup restored successfully")
                return 0
            else:
                print(f"✗ Restore failed")
                return 1

        elif args.command == "info":
            info = manager.get_backup_info()
            print("Backup Information:")
            print(f"  Directory: {info['backup_dir']}")
            print(f"  Total backups: {info['total_backups']}")
            print(f"  Total size: {info['total_size_mb']} MB")
            if info['latest_backup']:
                print(f"  Latest: {Path(info['latest_backup']).name}")
            if info['oldest_backup']:
                print(f"  Oldest: {Path(info['oldest_backup']).name}")
            return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        print(f"✗ Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
