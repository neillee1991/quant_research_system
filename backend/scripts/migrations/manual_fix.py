#!/usr/bin/env python3
"""
Check what tables exist in PostgreSQL
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
        # Get all tables
        rows = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)

        print("\nExisting tables:")
        for row in rows:
            print(f"  - {row['table_name']}")

        # Check for flow_config vs flow_configs
        has_flow_config = any(r['table_name'] == 'flow_config' for r in rows)
        has_flow_configs = any(r['table_name'] == 'flow_configs' for r in rows)

        print(f"\nflow_config exists: {has_flow_config}")
        print(f"flow_configs exists: {has_flow_configs}")

        if has_flow_config and not has_flow_configs:
            print("\nNeed to rename flow_config to flow_configs")
            # Let's do it manually
            print("Renaming tables...")

            # Rename flow_config → flow_configs
            try:
                await conn.execute("ALTER TABLE flow_config RENAME TO flow_configs")
                print("  ✓ flow_config → flow_configs")
            except Exception as e:
                print(f"  ✗ flow_config: {e}")

            # Rename flow_run → flow_runs
            try:
                await conn.execute("ALTER TABLE flow_run RENAME TO flow_runs")
                print("  ✓ flow_run → flow_runs")
            except Exception as e:
                print(f"  ✗ flow_run: {e}")

            # Rename task_run → task_runs
            try:
                await conn.execute("ALTER TABLE task_run RENAME TO task_runs")
                print("  ✓ task_run → task_runs")
            except Exception as e:
                print(f"  ✗ task_run: {e}")

            # Rename sequences
            try:
                await conn.execute("ALTER SEQUENCE flow_config_id_seq RENAME TO flow_configs_id_seq")
                print("  ✓ flow_config_id_seq → flow_configs_id_seq")
            except Exception as e:
                print(f"  ✗ flow_config_id_seq: {e}")

            try:
                await conn.execute("ALTER SEQUENCE flow_run_id_seq RENAME TO flow_runs_id_seq")
                print("  ✓ flow_run_id_seq → flow_runs_id_seq")
            except Exception as e:
                print(f"  ✗ flow_run_id_seq: {e}")

            try:
                await conn.execute("ALTER SEQUENCE task_run_id_seq RENAME TO task_runs_id_seq")
                print("  ✓ task_run_id_seq → task_runs_id_seq")
            except Exception as e:
                print(f"  ✗ task_run_id_seq: {e}")

            # Now create the missing tables
            print("\nCreating missing tables...")

            missing_tables_sql = """
            CREATE TABLE IF NOT EXISTS sync_task_configs (
              task_id           VARCHAR(255) PRIMARY KEY,
              api_name          VARCHAR(255) NOT NULL DEFAULT '',
              description       TEXT         DEFAULT '',
              sync_type         VARCHAR(50)  NOT NULL DEFAULT 'incremental',
              params_json       TEXT         DEFAULT '{}',
              date_field        VARCHAR(100) DEFAULT '',
              primary_keys_json TEXT         DEFAULT '[]',
              table_name        VARCHAR(255) NOT NULL DEFAULT '',
              schema_json       TEXT         DEFAULT '{}',
              enabled           BOOLEAN      DEFAULT TRUE,
              api_limit         INT          DEFAULT 5000,
              created_at        TIMESTAMPTZ  DEFAULT NOW(),
              updated_at        TIMESTAMPTZ  DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_sync_task_configs_enabled    ON sync_task_configs(enabled);
            CREATE INDEX IF NOT EXISTS idx_sync_task_configs_table_name ON sync_task_configs(table_name);

            CREATE TABLE IF NOT EXISTS stocks (
              ts_code     VARCHAR(20) PRIMARY KEY,
              symbol      VARCHAR(20)  DEFAULT '',
              name        VARCHAR(100) DEFAULT '',
              area        VARCHAR(100) DEFAULT '',
              industry    VARCHAR(100) DEFAULT '',
              market      VARCHAR(50)  DEFAULT '',
              list_date   DATE,
              list_status VARCHAR(10)  DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_stocks_list_status ON stocks(list_status);
            CREATE INDEX IF NOT EXISTS idx_stocks_industry    ON stocks(industry);

            CREATE TABLE IF NOT EXISTS factor_analysis_results (
              id            BIGSERIAL    PRIMARY KEY,
              factor_id     VARCHAR(255) NOT NULL,
              analysis_date TIMESTAMPTZ  NOT NULL,
              start_date    VARCHAR(8)   DEFAULT '',
              end_date      VARCHAR(8)   DEFAULT '',
              periods       TEXT         DEFAULT '[]',
              ic_mean       FLOAT,
              ic_std        FLOAT,
              rank_ic_mean  FLOAT,
              rank_ic_std   FLOAT,
              ic_ir         FLOAT,
              turnover_mean FLOAT,
              ic_summary    TEXT         DEFAULT '{}',
              ic_by_period  TEXT         DEFAULT '{}',
              config        TEXT         DEFAULT '{}',
              report_path   TEXT         DEFAULT '',
              task_status   VARCHAR(20)  DEFAULT '',
              task_id       VARCHAR(255) DEFAULT '',
              error_message TEXT         DEFAULT '',
              created_at    TIMESTAMPTZ  DEFAULT NOW(),
              UNIQUE (factor_id, analysis_date)
            );
            CREATE INDEX IF NOT EXISTS idx_factor_analysis_results_factor_id
              ON factor_analysis_results(factor_id);
            CREATE INDEX IF NOT EXISTS idx_factor_analysis_results_date
              ON factor_analysis_results(analysis_date DESC);
            """

            await conn.execute(missing_tables_sql)
            print("  ✓ Missing tables created")

            # Add columns to task_runs if needed
            print("\nAdding columns to task_runs...")
            add_columns_sql = """
            ALTER TABLE task_runs
              ADD COLUMN IF NOT EXISTS run_id      VARCHAR(255),
              ADD COLUMN IF NOT EXISTS task_name   TEXT DEFAULT '',
              ADD COLUMN IF NOT EXISTS rows        INT DEFAULT 0,
              ADD COLUMN IF NOT EXISTS elapsed_sec FLOAT,
              ADD COLUMN IF NOT EXISTS params      TEXT DEFAULT '',
              ADD COLUMN IF NOT EXISTS extra       TEXT DEFAULT '',
              ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ,
              ADD COLUMN IF NOT EXISTS target_date VARCHAR(8);

            CREATE UNIQUE INDEX IF NOT EXISTS idx_task_runs_run_id
              ON task_runs(run_id) WHERE run_id IS NOT NULL;
            """
            await conn.execute(add_columns_sql)
            print("  ✓ Columns added to task_runs")

            # Update FK constraints
            print("\nUpdating foreign keys...")
            try:
                await conn.execute("""
                    ALTER TABLE flow_runs
                      DROP CONSTRAINT IF EXISTS flow_run_parent_flow_run_id_fkey,
                      ADD CONSTRAINT flow_runs_parent_flow_run_id_fkey
                        FOREIGN KEY (parent_flow_run_id) REFERENCES flow_runs(id);
                """)
                print("  ✓ flow_runs FK updated")
            except Exception as e:
                print(f"  ✗ flow_runs FK: {e}")

            try:
                await conn.execute("""
                    ALTER TABLE task_runs
                      DROP CONSTRAINT IF EXISTS task_run_flow_run_id_fkey,
                      ADD CONSTRAINT task_runs_flow_run_id_fkey
                        FOREIGN KEY (flow_run_id) REFERENCES flow_runs(id) ON DELETE CASCADE;
                """)
                print("  ✓ task_runs FK updated")
            except Exception as e:
                print(f"  ✗ task_runs FK: {e}")

            # Rename indexes
            print("\nRenaming indexes...")
            index_renames = [
                ("idx_flow_config_enabled", "idx_flow_configs_enabled"),
                ("idx_flow_config_updated_at", "idx_flow_configs_updated_at"),
                ("idx_flow_run_flow_name", "idx_flow_runs_flow_name"),
                ("idx_flow_run_status", "idx_flow_runs_status"),
                ("idx_flow_run_created_at", "idx_flow_runs_created_at"),
                ("idx_task_run_flow_run_id", "idx_task_runs_flow_run_id"),
                ("idx_task_run_status", "idx_task_runs_status"),
            ]
            for old_name, new_name in index_renames:
                try:
                    await conn.execute(f'ALTER INDEX IF EXISTS "{old_name}" RENAME TO "{new_name}"')
                    print(f"  ✓ {old_name} → {new_name}")
                except Exception as e:
                    print(f"  ✗ {old_name}: {e}")

            print("\nDone! Verifying final state...")
            rows = await conn.fetch("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)

            print("\nFinal tables:")
            for row in rows:
                print(f"  - {row['table_name']}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
