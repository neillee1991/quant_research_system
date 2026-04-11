#!/usr/bin/env python3
"""
Fix schema issues:
1. etl_task_configs missing schema_json
2. task_runs column mismatch (error vs error_message)
"""
import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_dir))

import asyncio
import asyncpg
from app.core.config import settings


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
        # 1. Add schema_json to etl_task_configs
        print("Adding schema_json to etl_task_configs...")
        await conn.execute("""
            ALTER TABLE etl_task_configs
            ADD COLUMN IF NOT EXISTS schema_json TEXT DEFAULT '{}'
        """)
        print("  ✓ Done")

        # 2. Check task_runs columns
        print("\nChecking task_runs columns...")
        cols = await conn.fetch("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'task_runs'
            ORDER BY ordinal_position
        """)
        col_names = [c['column_name'] for c in cols]
        print(f"  Columns: {col_names}")

        has_error = 'error' in col_names
        has_error_message = 'error_message' in col_names

        if has_error_message and not has_error:
            print("  Adding error column as alias for error_message...")
            await conn.execute("""
                ALTER TABLE task_runs
                ADD COLUMN IF NOT EXISTS error TEXT GENERATED ALWAYS AS (error_message) STORED
            """)
            print("  ✓ Done")
        elif has_error and has_error_message:
            print("  Both error and error_message exist")
        elif has_error and not has_error_message:
            print("  Only error exists, adding error_message...")
            await conn.execute("""
                ALTER TABLE task_runs
                ADD COLUMN IF NOT EXISTS error_message TEXT GENERATED ALWAYS AS (error) STORED
            """)
            print("  ✓ Done")

        print("\nAll fixes applied!")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
