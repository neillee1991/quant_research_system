#!/usr/bin/env python3
"""
初始化 PostgreSQL 表结构（幂等）
全新部署时运行，执行 create_tables.sql 建表。
"""
import sys
import asyncio
from pathlib import Path

backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

import asyncpg
from app.core.config import settings

CREATE_TABLES_SQL = Path(__file__).parent / "create_tables.sql"

TABLES = [
    "flow_configs", "flow_runs", "task_runs",
    "sync_task_configs", "etl_task_configs",
    "factor_configs", "factor_field_mappings",
    "index_configs", "user_preferences",
    "factor_analysis_results", "schema_migrations",
]


async def main():
    print("Connecting to PostgreSQL...")
    conn = await asyncpg.connect(
        host=settings.postgresql.postgres_host,
        port=settings.postgresql.postgres_port,
        database=settings.postgresql.postgres_db,
        user=settings.postgresql.postgres_user,
        password=settings.postgresql.postgres_password,
    )

    try:
        sql = CREATE_TABLES_SQL.read_text()
        await conn.execute(sql)
        print("Tables created/verified.")

        print("\nVerifying tables:")
        for table in TABLES:
            try:
                await conn.fetch(f"SELECT 1 FROM {table} LIMIT 1")
                print(f"  ✓ {table}")
            except asyncpg.UndefinedTableError:
                print(f"  ✗ {table} - MISSING")

        print("\nMigration complete!")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
