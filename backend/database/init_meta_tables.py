#!/usr/bin/env python3
"""
初始化 PostgreSQL 配置数据

全新部署时运行，通过 initial_config.json 导入所有配置。

用法：
    cd backend
    python database/init_meta_tables.py
"""
import asyncio
import base64
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

INITIAL_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config/initial_config.json"


async def main():
    print("开始导入初始配置...")

    if not INITIAL_CONFIG_PATH.exists():
        print(f"✗ 找不到初始配置文件: {INITIAL_CONFIG_PATH}")
        sys.exit(1)

    try:
        from scheduler.db import init_db, close_db
        await init_db()

        raw = INITIAL_CONFIG_PATH.read_bytes()
        content = base64.b64encode(raw).decode("utf-8")

        from app.services.config_import_service import ConfigImportService
        from app.models.config_import_export import ImportMode
        service = ConfigImportService()
        success, summary, errors = service.apply_import(content, ImportMode.FAST)

        if errors:
            for e in errors:
                print(f"  警告: {e}")

        if success:
            for config_type, result in summary.items():
                print(f"  ✓ {config_type}: 新增 {result.created}, 更新 {result.updated}, 跳过 {result.skipped}")
            print("✓ 初始配置导入完成")
        else:
            print("✗ 导入失败")
            sys.exit(1)

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
