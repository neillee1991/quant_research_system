#!/usr/bin/env python3
"""
Fix migration 003 - force run all statements
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
        migration_file = backend_dir / "scripts" / "migrations" / "003_migrate_dolphindb_tables.sql"
        print(f"Running: {migration_file}")

        with open(migration_file, "r") as f:
            sql = f.read()

        # Execute entire file at once
        print("Executing migration...")
        await conn.execute(sql)
        print("Done!")

        # Verify tables
        tables = [
            "flow_configs", "flow_runs", "task_runs",
            "sync_task_configs", "etl_task_configs",
            "factor_configs", "factor_field_mappings",
            "stocks", "trading_calendar", "index_configs", "user_preferences",
            "factor_analysis_results", "backtest_results"
        ]

        print("\nVerifying tables:")
        all_good = True
        for table in tables:
            try:
                await conn.fetch(f"SELECT 1 FROM {table} LIMIT 1")
                print(f"  ✓ {table}")
            except asyncpg.UndefinedTableError:
                print(f"  ✗ {table} - MISSING")
                all_good = False

        if all_good:
            print("\nAll tables created successfully!")
        else:
            print("\nSome tables are missing!")
            sys.exit(1)

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
