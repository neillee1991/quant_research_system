#!/usr/bin/env python3
"""
手动初始化 DolphinDB 元数据表
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from store.dolphindb_client import db_client
from app.core.logger import logger

def main():
    print("开始初始化元数据表...")

    try:
        # 创建所有元数据表
        db_client.ensure_meta_tables()
        print("✓ 元数据表创建成功")

        # 写入默认同步任务配置
        db_client.seed_sync_task_config()
        print("✓ 同步任务配置种子数据已写入")

        # 写入因子数据配置
        db_client.seed_factor_data_config()
        print("✓ 因子数据配置种子数据已写入")

        # 写入默认种子因子定义
        db_client.seed_factor_metadata()
        print("✓ 种子因子定义已写入")

        print("\n所有元数据表初始化完成！")

    except Exception as e:
        print(f"✗ 初始化失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
