#!/usr/bin/env python3
"""
Run PostgreSQL migration scripts
"""
import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

import asyncio
import asyncpg
from app.core.config import settings


async def run_migration_file(conn, file_path: Path):
    """Run a migration SQL file"""
    print(f"Running migration: {file_path.name}")
    with open(file_path, "r") as f:
        sql = f.read()

    # 按 ; 分割，但跳过 $$...$$  dollar-quoted 块内的分号
    statements = []
    current = []
    in_dollar_quote = False
    for line in sql.splitlines():
        stripped = line.strip()
        # 切换 dollar-quote 状态
        if '$$' in stripped:
            count = stripped.count('$$')
            if count % 2 != 0:
                in_dollar_quote = not in_dollar_quote
        current.append(line)
        if not in_dollar_quote and stripped.endswith(';'):
            stmt = '\n'.join(current).strip().rstrip(';').strip()
            # 去掉开头的注释行
            lines = [l for l in stmt.splitlines() if not l.strip().startswith('--')]
            stmt = '\n'.join(lines).strip()
            if stmt:
                statements.append(stmt)
            current = []

    # 处理末尾没有分号的语句
    if current:
        stmt = '\n'.join(current).strip()
        lines = [l for l in stmt.splitlines() if not l.strip().startswith('--')]
        stmt = '\n'.join(lines).strip()
        if stmt:
            statements.append(stmt)

    for stmt in statements:
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

        # 按顺序执行的 SQL migration 文件列表
        sql_migrations = [
            "001_create_scheduler_tables.sql",
            "002_add_target_date_to_task_run.sql",
            "003_migrate_dolphindb_tables.sql",
            "004_add_run_id_to_flow_runs.sql",
            "005_fix_etl_task_configs.sql",
            "006_add_constraints_and_triggers.sql",
            "007_fix_missing_updated_at_columns.sql",
            "008_add_batch_fields_to_backtest_results.sql",  # 批量回测字段
        ]

        if not has_singular and not has_plural:
            print("Fresh database - running all migrations...")
            for f in sql_migrations:
                path = migrations_dir / f
                if path.exists():
                    await run_migration_file(conn, path)
        elif has_singular and not has_plural:
            print("Running migration 003+ only...")
            for f in sql_migrations[2:]:
                path = migrations_dir / f
                if path.exists():
                    await run_migration_file(conn, path)
        else:
            # 已有复数表，只补跑 004 以后的（幂等）
            print("Tables exist - applying incremental migrations...")
            for f in sql_migrations[3:]:
                path = migrations_dir / f
                if path.exists():
                    await run_migration_file(conn, path)

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
