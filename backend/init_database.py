#!/usr/bin/env python3
"""
数据库初始化脚本
使用 DolphinDB 初始化脚本创建所有必需的表
"""
import sys
from pathlib import Path

# 添加项目路径
backend_dir = Path(__file__).parent
sys.path.insert(0, str(backend_dir))

# 检查是否在虚拟环境中
if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
    print("警告: 未检测到虚拟环境")
    print("请先激活虚拟环境:")
    print("  source .venv/bin/activate")
    print()

try:
    from store.dolphindb_client import db_client
    from app.core.logger import logger
except ImportError as e:
    print(f"错误: 无法导入必需的模块: {e}")
    print("\n请确保:")
    print("  1. 已激活虚拟环境")
    print("  2. 已安装依赖: pip install -r requirements.txt")
    sys.exit(1)


def check_connection():
    """检查 DolphinDB 连接"""
    try:
        df = db_client.query("SELECT 1 as test")
        logger.info("✓ DolphinDB 连接成功")
        return True
    except Exception as e:
        logger.error(f"✗ DolphinDB 连接失败: {e}")
        return False


def run_init_script():
    """执行 DolphinDB 初始化脚本"""
    init_script = backend_dir / "database" / "init_dolphindb.py"
    if not init_script.exists():
        logger.error(f"初始化脚本不存在: {init_script}")
        return False

    try:
        import subprocess
        result = subprocess.run(
            [sys.executable, str(init_script)],
            capture_output=True, text=True, cwd=str(backend_dir)
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            return False
        return True
    except Exception as e:
        logger.error(f"执行初始化脚本失败: {e}")
        return False


def verify_tables():
    """验证关键表是否存在"""
    key_tables = [
        'daily_data', 'daily_basic', 'adj_factor',
        'index_daily', 'moneyflow', 'factor_values',
        'stock_basic', 'sync_log', 'sync_log_history',
        'factor_metadata', 'factor_analysis',
    ]

    existing = []
    missing = []
    for table in key_tables:
        if db_client.table_exists(table):
            existing.append(table)
        else:
            missing.append(table)

    print(f"\n已创建的表 ({len(existing)}):")
    for t in existing:
        print(f"  ✓ {t}")

    if missing:
        print(f"\n缺失的表 ({len(missing)}):")
        for t in missing:
            print(f"  ✗ {t}")

    return len(missing) == 0


def main():
    print("=" * 60)
    print("  量化研究系统 - DolphinDB 数据库初始化")
    print("=" * 60)
    print()

    # 1. 检查连接
    print("1. 检查 DolphinDB 连接...")
    if not check_connection():
        print("\n✗ 无法连接到 DolphinDB")
        print("请确保:")
        print("  - DolphinDB 已启动: docker-compose up -d dolphindb")
        print("  - 配置正确: .env 中的 DOLPHINDB_HOST/PORT")
        sys.exit(1)
    print()

    # 2. 执行初始化脚本
    print("2. 执行 DolphinDB 初始化脚本...")
    if not run_init_script():
        print("\n✗ 初始化脚本执行失败")
        sys.exit(1)
    print()

    # 3. 验证表
    print("3. 验证表结构...")
    if verify_tables():
        print("\n✅ 所有表已创建成功")
    else:
        print("\n⚠️  部分表创建失败，请检查日志")
    print()

    print("=" * 60)
    print("  数据库初始化完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
