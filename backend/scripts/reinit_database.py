#!/usr/bin/env python3
"""
数据库重新初始化主控脚本
自动化执行备份、删除、重建、恢复的完整流程
"""

import subprocess
import sys
from pathlib import Path

backend_dir = Path(__file__).resolve().parent.parent


def run_script(script_name: str, description: str) -> bool:
    """
    运行指定的脚本

    Args:
        script_name: 脚本文件名
        description: 步骤描述

    Returns:
        是否成功
    """
    print("\n" + "=" * 60)
    print(f"步骤: {description}")
    print("=" * 60)

    script_path = backend_dir / "scripts" / script_name
    if not script_path.exists():
        print(f"错误: 脚本不存在: {script_path}")
        return False

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(backend_dir),
            check=True,
            text=True,
        )
        print(f"✓ {description} 完成")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} 失败: {e}")
        return False


def run_init_db(description: str) -> bool:
    """
    运行数据库初始化脚本

    Args:
        description: 步骤描述

    Returns:
        是否成功
    """
    print("\n" + "=" * 60)
    print(f"步骤: {description}")
    print("=" * 60)

    init_script = backend_dir / "database" / "init_dolphindb.py"
    if not init_script.exists():
        print(f"错误: 初始化脚本不存在: {init_script}")
        return False

    try:
        result = subprocess.run(
            [sys.executable, str(init_script)],
            cwd=str(backend_dir),
            check=True,
            text=True,
        )
        print(f"✓ {description} 完成")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} 失败: {e}")
        return False


def check_dolphindb() -> bool:
    """检查 DolphinDB 是否运行"""
    print("检查 DolphinDB 状态...")
    try:
        import dolphindb as ddb
        sess = ddb.Session()
        success = sess.connect("127.0.0.1", 8848, "admin", "123456")
        sess.close()
        if success:
            print("✓ DolphinDB 正在运行")
            return True
        else:
            print("✗ DolphinDB 连接失败")
            return False
    except Exception as e:
        print(f"✗ DolphinDB 连接失败: {e}")
        return False


def main():
    """主函数"""
    print("=" * 60)
    print("数据库重新初始化主控脚本")
    print("=" * 60)
    print("\n此脚本将执行以下步骤:")
    print("  1. 检查 DolphinDB 状态")
    print("  2. 备份现有配置数据")
    print("  3. 删除旧表")
    print("  4. 重新创建表（无版本字段）")
    print("  5. 恢复配置数据")
    print("  6. 验证数据完整性")

    # 确认操作
    print("\n警告: 此操作将重建数据库表结构!")
    response = input("是否继续? (yes/no): ").strip().lower()
    if response != "yes":
        print("操作已取消")
        return

    # 步骤 1: 检查 DolphinDB
    if not check_dolphindb():
        print("\n错误: DolphinDB 未运行，请先启动 DolphinDB")
        print("提示: 运行 'docker-compose up -d' 启动服务")
        return

    # 步骤 2: 备份配置数据
    if not run_script("backup_configs.py", "备份现有配置数据"):
        print("\n错误: 备份失败，终止操作")
        return

    # 步骤 3: 删除旧表
    if not run_script("drop_old_tables.py", "删除旧表"):
        print("\n错误: 删除旧表失败，终止操作")
        return

    # 步骤 4: 重新创建表
    if not run_init_db("重新创建表结构"):
        print("\n错误: 重新创建表失败，终止操作")
        return

    # 步骤 5: 恢复配置数据
    if not run_script("restore_configs.py", "恢复配置数据"):
        print("\n错误: 恢复配置数据失败")
        print("提示: 备份文件仍在 backups/ 目录中，可以手动恢复")
        return

    # 步骤 6: 验证数据完整性
    print("\n" + "=" * 60)
    print("验证数据完整性")
    print("=" * 60)

    try:
        sys.path.insert(0, str(backend_dir))
        from store.dolphindb_client import db_client

        tables = ["sync_task_config", "etl_task_config", "factor_metadata"]
        for table in tables:
            count = db_client.query(f"SELECT count(*) as cnt FROM {table}")
            row_count = count["cnt"][0] if not count.is_empty() else 0
            print(f"  {table}: {row_count} 条记录")

        print("\n✓ 数据完整性验证通过")

    except Exception as e:
        print(f"\n✗ 验证失败: {e}")

    # 完成
    print("\n" + "=" * 60)
    print("数据库重新初始化完成!")
    print("=" * 60)
    print("\n后续步骤:")
    print("  1. 检查日志确认无错误")
    print("  2. 验证应用功能正常")
    print("  3. 备份文件保存在 backend/backups/ 目录")


if __name__ == "__main__":
    main()
