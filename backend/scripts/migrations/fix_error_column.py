#!/usr/bin/env python3
"""
Fix task_runs: replace generated error column with a real column
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
        # Drop generated error column
        print("Dropping generated error column...")
        await conn.execute("ALTER TABLE task_runs DROP COLUMN IF EXISTS error")
        print("  ✓ Done")

        # Add real error column, sync from error_message
        print("Adding real error column...")
        await conn.execute("""
            ALTER TABLE task_runs
            ADD COLUMN IF NOT EXISTS error TEXT
        """)
        print("  ✓ Done")

        # Copy existing data
        print("Copying data from error_message to error...")
        await conn.execute("UPDATE task_runs SET error = error_message WHERE error IS NULL")
        print("  ✓ Done")

        print("\nAll fixes applied!")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
