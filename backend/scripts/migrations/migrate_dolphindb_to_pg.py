"""
One-time data migration: DolphinDB config/operational tables → PostgreSQL.

Run AFTER 003_migrate_dolphindb_tables.sql has been applied.

Usage:
    cd backend
    python -m scripts.migrations.migrate_dolphindb_to_pg

The script is idempotent (ON CONFLICT DO NOTHING / DO UPDATE).
"""
import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Optional

import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _get_pg_dsn() -> str:
    from app.core.config import settings
    pg = settings.postgresql
    return (
        f"postgresql://{pg.postgres_user}:{pg.postgres_password}"
        f"@{pg.postgres_host}:{pg.postgres_port}/{pg.postgres_db}"
    )


def _str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def _dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v))
    except Exception:
        return None


async def migrate_sync_task_configs(pg: asyncpg.Connection) -> int:
    from store.dolphindb_client import db_client
    df = db_client.query("SELECT * FROM sync_task_config")
    if df.is_empty():
        log.info("sync_task_config: no data")
        return 0
    count = 0
    for row in df.to_dicts():
        await pg.execute("""
            INSERT INTO sync_task_configs
              (task_id, api_name, description, sync_type, params_json,
               date_field, primary_keys_json, table_name, schema_json,
               enabled, api_limit, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (task_id) DO NOTHING
        """,
            _str(row.get("task_id")), _str(row.get("api_name")),
            _str(row.get("description")), _str(row.get("sync_type")),
            _str(row.get("params_json")), _str(row.get("date_field")),
            _str(row.get("primary_keys_json")), _str(row.get("table_name")),
            _str(row.get("schema_json")), bool(row.get("enabled", True)),
            int(row.get("api_limit") or 5000),
            _dt(row.get("created_at")) or datetime.now(),
            _dt(row.get("updated_at")) or datetime.now(),
        )
        count += 1
    log.info(f"sync_task_configs: migrated {count} rows")
    return count


async def migrate_etl_task_configs(pg: asyncpg.Connection) -> int:
    from store.dolphindb_client import db_client
    df = db_client.query("SELECT * FROM etl_task_config")
    if df.is_empty():
        log.info("etl_task_config: no data")
        return 0
    count = 0
    for row in df.to_dicts():
        await pg.execute("""
            INSERT INTO etl_task_configs
              (task_id, description, script, sync_type, date_field,
               primary_keys_json, table_name, enabled, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (task_id) DO NOTHING
        """,
            _str(row.get("task_id")), _str(row.get("description")),
            _str(row.get("script")), _str(row.get("sync_type")),
            _str(row.get("date_field")), _str(row.get("primary_keys_json")),
            _str(row.get("table_name")), bool(row.get("enabled", True)),
            _dt(row.get("created_at")) or datetime.now(),
            _dt(row.get("updated_at")) or datetime.now(),
        )
        count += 1
    log.info(f"etl_task_configs: migrated {count} rows")
    return count


async def migrate_factor_configs(pg: asyncpg.Connection) -> int:
    from store.dolphindb_client import db_client
    df = db_client.query("SELECT * FROM factor_metadata")
    if df.is_empty():
        log.info("factor_metadata: no data")
        return 0
    count = 0
    for row in df.to_dicts():
        await pg.execute("""
            INSERT INTO factor_configs
              (factor_id, description, category, compute_mode, storage_target,
               depends_on, params, code, enabled, align_calendar, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (factor_id) DO NOTHING
        """,
            _str(row.get("factor_id")), _str(row.get("description")),
            _str(row.get("category") or "custom"),
            _str(row.get("compute_mode") or "incremental"),
            _str(row.get("storage_target") or "factor_values"),
            _str(row.get("depends_on") or "[]"),
            _str(row.get("params") or "{}"),
            _str(row.get("code")), bool(row.get("enabled", True)),
            bool(row.get("align_calendar", False)),
            _dt(row.get("created_at")) or datetime.now(),
            _dt(row.get("updated_at")) or datetime.now(),
        )
        count += 1
    log.info(f"factor_configs: migrated {count} rows")
    return count


async def migrate_factor_field_mappings(pg: asyncpg.Connection) -> int:
    from store.dolphindb_client import db_client
    df = db_client.query("SELECT * FROM factor_data_config")
    if df.is_empty():
        log.info("factor_data_config: no data")
        return 0
    count = 0
    for row in df.to_dicts():
        await pg.execute("""
            INSERT INTO factor_field_mappings
              (field_key, description, table_name, column_name, extra_config, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (field_key) DO NOTHING
        """,
            _str(row.get("field_key")), _str(row.get("description")),
            _str(row.get("table_name")), _str(row.get("column_name")),
            _str(row.get("extra_config") or "{}"),
            _dt(row.get("updated_at")) or datetime.now(),
        )
        count += 1
    log.info(f"factor_field_mappings: migrated {count} rows")
    return count


async def migrate_stocks(pg: asyncpg.Connection) -> int:
    from store.dolphindb_client import db_client
    df = db_client.query("SELECT * FROM stock_basic")
    if df.is_empty():
        log.info("stock_basic: no data")
        return 0
    count = 0
    for row in df.to_dicts():
        list_date = row.get("list_date")
        if list_date and not isinstance(list_date, type(None)):
            try:
                from datetime import date
                if hasattr(list_date, 'date'):
                    list_date = list_date.date()
            except Exception:
                list_date = None
        await pg.execute("""
            INSERT INTO stocks
              (ts_code, symbol, name, area, industry, market, list_date, list_status)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (ts_code) DO NOTHING
        """,
            _str(row.get("ts_code")), _str(row.get("symbol")),
            _str(row.get("name")), _str(row.get("area")),
            _str(row.get("industry")), _str(row.get("market")),
            list_date, _str(row.get("list_status")),
        )
        count += 1
    log.info(f"stocks: migrated {count} rows")
    return count


async def migrate_trading_calendar(pg: asyncpg.Connection) -> int:
    from store.dolphindb_client import db_client
    df = db_client.query("SELECT * FROM trade_cal")
    if df.is_empty():
        log.info("trade_cal: no data")
        return 0
    count = 0
    for row in df.to_dicts():
        cal_date = row.get("cal_date")
        pretrade_date = row.get("pretrade_date")
        if hasattr(cal_date, 'date'):
            cal_date = cal_date.date()
        if hasattr(pretrade_date, 'date'):
            pretrade_date = pretrade_date.date()
        await pg.execute("""
            INSERT INTO trading_calendar (exchange, cal_date, is_open, pretrade_date)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (exchange, cal_date) DO NOTHING
        """,
            _str(row.get("exchange")), cal_date,
            int(row.get("is_open") or 0), pretrade_date,
        )
        count += 1
    log.info(f"trading_calendar: migrated {count} rows")
    return count


async def migrate_index_configs(pg: asyncpg.Connection) -> int:
    from store.dolphindb_client import db_client
    df = db_client.query("SELECT * FROM index_metadata")
    if df.is_empty():
        log.info("index_metadata: no data")
        return 0
    count = 0
    for row in df.to_dicts():
        latest_date = row.get("latest_date")
        if hasattr(latest_date, 'date'):
            latest_date = latest_date.date()
        await pg.execute("""
            INSERT INTO index_configs
              (index_code, index_name, description, stock_count, latest_date,
               created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (index_code) DO NOTHING
        """,
            _str(row.get("index_code")), _str(row.get("index_name")),
            _str(row.get("description")), int(row.get("stock_count") or 0),
            latest_date,
            _dt(row.get("created_at")) or datetime.now(),
            _dt(row.get("updated_at")) or datetime.now(),
        )
        count += 1
    log.info(f"index_configs: migrated {count} rows")
    return count


async def migrate_user_preferences(pg: asyncpg.Connection) -> int:
    from store.dolphindb_client import db_client
    df = db_client.query("SELECT * FROM user_sync_preference")
    if df.is_empty():
        log.info("user_sync_preference: no data")
        return 0
    count = 0
    for row in df.to_dicts():
        await pg.execute("""
            INSERT INTO user_preferences
              (user_id, index_table, filter_config, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (user_id) DO NOTHING
        """,
            _str(row.get("user_id")), _str(row.get("index_table")),
            _str(row.get("filter_config") or "{}"),
            _dt(row.get("created_at")) or datetime.now(),
            _dt(row.get("updated_at")) or datetime.now(),
        )
        count += 1
    log.info(f"user_preferences: migrated {count} rows")
    return count


async def migrate_task_runs(pg: asyncpg.Connection) -> int:
    """Migrate DolphinDB task_runs into PG task_runs (run_id column)."""
    from store.dolphindb_client import db_client
    df = db_client.query("SELECT * FROM task_runs")
    if df.is_empty():
        log.info("task_runs (DolphinDB): no data")
        return 0
    count = 0
    for row in df.to_dicts():
        await pg.execute("""
            INSERT INTO task_runs
              (run_id, task_type, task_id, task_name, status,
               started_at, finished_at, elapsed_sec, rows, error, params, extra)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (run_id) WHERE run_id IS NOT NULL DO NOTHING
        """,
            _str(row.get("run_id")), _str(row.get("task_type")),
            _str(row.get("task_id")), _str(row.get("task_name")),
            _str(row.get("status") or "success"),
            _dt(row.get("started_at")), _dt(row.get("finished_at")),
            float(row.get("elapsed_sec") or 0),
            int(row.get("rows") or 0),
            _str(row.get("error")), _str(row.get("params")), _str(row.get("extra")),
        )
        count += 1
    log.info(f"task_runs: migrated {count} rows from DolphinDB")
    return count


async def migrate_backtest_results(pg: asyncpg.Connection) -> int:
    from store.dolphindb_client import db_client
    df = db_client.query("SELECT * FROM backtest_results")
    if df.is_empty():
        log.info("backtest_results: no data")
        return 0
    count = 0
    for row in df.to_dicts():
        await pg.execute("""
            INSERT INTO backtest_results
              (run_id, task_id, task_name, metrics_json,
               equity_curve_json, trades_json, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (run_id) DO NOTHING
        """,
            _str(row.get("run_id")), _str(row.get("task_id")),
            _str(row.get("task_name")),
            _str(row.get("metrics_json") or "{}"),
            _str(row.get("equity_curve_json") or "[]"),
            _str(row.get("trades_json") or "[]"),
            _dt(row.get("created_at")) or datetime.now(),
        )
        count += 1
    log.info(f"backtest_results: migrated {count} rows")
    return count


async def migrate_factor_analysis_results(pg: asyncpg.Connection) -> int:
    """Merge factor_analysis + factor_analysis_extended → factor_analysis_results."""
    from store.dolphindb_client import db_client
    count = 0

    # Base table: factor_analysis
    df = db_client.query("SELECT * FROM factor_analysis")
    if not df.is_empty():
        for row in df.to_dicts():
            await pg.execute("""
                INSERT INTO factor_analysis_results
                  (factor_id, analysis_date, start_date, end_date, periods,
                   ic_mean, ic_std, rank_ic_mean, rank_ic_std, ic_ir,
                   turnover_mean, created_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (factor_id, analysis_date) DO NOTHING
            """,
                _str(row.get("factor_id")),
                _dt(row.get("analysis_date")) or datetime.now(),
                _str(row.get("start_date")), _str(row.get("end_date")),
                _str(row.get("periods") or "[]"),
                row.get("ic_mean"), row.get("ic_std"),
                row.get("rank_ic_mean"), row.get("rank_ic_std"),
                row.get("ic_ir"), row.get("turnover_mean"),
                _dt(row.get("created_at")) or datetime.now(),
            )
            count += 1

    # Extended table: factor_analysis_extended (UPDATE existing rows with extra fields)
    try:
        df_ext = db_client.query("SELECT * FROM factor_analysis_extended")
        if not df_ext.is_empty():
            for row in df_ext.to_dicts():
                await pg.execute("""
                    INSERT INTO factor_analysis_results
                      (factor_id, analysis_date, start_date, end_date, config,
                       ic_summary, ic_by_period, report_path,
                       task_status, task_id, error_message, created_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                    ON CONFLICT (factor_id, analysis_date) DO UPDATE SET
                      config        = EXCLUDED.config,
                      ic_summary    = EXCLUDED.ic_summary,
                      ic_by_period  = EXCLUDED.ic_by_period,
                      report_path   = EXCLUDED.report_path,
                      task_status   = EXCLUDED.task_status,
                      task_id       = EXCLUDED.task_id,
                      error_message = EXCLUDED.error_message
                """,
                    _str(row.get("factor_id")),
                    _dt(row.get("analysis_date")) or datetime.now(),
                    _str(row.get("start_date")), _str(row.get("end_date")),
                    _str(row.get("config") or "{}"),
                    _str(row.get("ic_summary") or "{}"),
                    _str(row.get("ic_by_period") or "{}"),
                    _str(row.get("report_path")),
                    _str(row.get("task_status")), _str(row.get("task_id")),
                    _str(row.get("error_message")),
                    _dt(row.get("created_at")) or datetime.now(),
                )
                count += 1
    except Exception as e:
        log.warning(f"factor_analysis_extended not found or error: {e}")

    log.info(f"factor_analysis_results: migrated {count} rows")
    return count


async def main() -> None:
    import sys
    sys.path.insert(0, ".")

    dsn = _get_pg_dsn()
    log.info(f"Connecting to PostgreSQL: {dsn.split('@')[1]}")
    pg = await asyncpg.connect(dsn)

    try:
        total = 0
        total += await migrate_sync_task_configs(pg)
        total += await migrate_etl_task_configs(pg)
        total += await migrate_factor_configs(pg)
        total += await migrate_factor_field_mappings(pg)
        total += await migrate_stocks(pg)
        total += await migrate_trading_calendar(pg)
        total += await migrate_index_configs(pg)
        total += await migrate_user_preferences(pg)
        total += await migrate_task_runs(pg)
        total += await migrate_backtest_results(pg)
        total += await migrate_factor_analysis_results(pg)
        log.info(f"Migration complete. Total rows migrated: {total}")
    finally:
        await pg.close()


if __name__ == "__main__":
    asyncio.run(main())
