#!/usr/bin/env python3
"""
手动初始化 PostgreSQL 元数据表（seed 数据）

用法：
    cd backend
    python database/init_meta_tables.py
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


async def main():
    print("开始初始化元数据表...")

    try:
        from scheduler.db import init_db, close_db
        await init_db()

        from infrastructure.seed import SeedDataLoader, SeedDataManager
        seed_manager = SeedDataManager(loader=SeedDataLoader())
        await seed_manager.seed_all()
        print("✓ 所有元数据表初始化完成")

    except Exception as e:
        print(f"✗ 初始化失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        try:
            from scheduler.db import close_db
            await close_db()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
