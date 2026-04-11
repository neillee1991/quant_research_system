#!/usr/bin/env python3
"""
Run PostgreSQL migration scripts
"""
import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_dir))

import asyncio
import asyncpg
from app.core.config import settings


async def run_migration_file(conn, file_path: Path):
    """Run a migration SQL file"""
    print(f"Running migration: {file_path.name}")
    with open(file_path, "r") as f:
        sql = f.read()

    # Split by semicolon and execute each statement
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in statements:
        # Skip comments
        if stmt.startswith("--"):
            continue
        try:
            await conn.execute(stmt)
        except Exception as e:
            print(f"  Warning: {e}")


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
        # Check if flow_config exists (singular)
        try:
            await conn.fetch("SELECT 1 FROM flow_config LIMIT 1")
            has_singular = True
            print("Found singular tables (flow_config, etc.)")
        except asyncpg.UndefinedTableError:
            has_singular = False
            print("Singular tables not found")

        # Check if flow_configs exists (plural)
        try:
            await conn.fetch("SELECT 1 FROM flow_configs LIMIT 1")
            has_plural = True
            print("Found plural tables (flow_configs, etc.)")
        except asyncpg.UndefinedTableError:
            has_plural = False
            print("Plural tables not found")

        migrations_dir = backend_dir / "scripts" / "migrations"

        if not has_singular and not has_plural:
            # Fresh database: run all migrations
            print("Fresh database - running all migrations...")
            await run_migration_file(conn, migrations_dir / "001_create_scheduler_tables.sql")
            await run_migration_file(conn, migrations_dir / "002_add_target_date_to_task_run.sql")
            await run_migration_file(conn, migrations_dir / "003_migrate_dolphindb_tables.sql")
        elif has_singular and not has_plural:
            # Need to run 003 only
            print("Running migration 003 only...")
            await run_migration_file(conn, migrations_dir / "003_migrate_dolphindb_tables.sql")
        else:
            print("Migrations already applied - verifying all tables exist...")

        # Verify tables
        tables = [
            "flow_configs", "flow_runs", "task_runs",
            "sync_task_configs", "etl_task_configs",
            "factor_configs", "factor_field_mappings",
            "stocks", "trading_calendar", "index_configs", "user_preferences",
            "factor_analysis_results", "backtest_results"
        ]

        print("\nVerifying tables:")
        for table in tables:
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
