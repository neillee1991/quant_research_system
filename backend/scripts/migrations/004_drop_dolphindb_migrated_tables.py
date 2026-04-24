"""
Step 4: 删除已迁移到 PostgreSQL 的 DolphinDB 表

前置条件：
  1. 003_migrate_dolphindb_tables.sql 已在 PostgreSQL 执行
  2. migrate_dolphindb_to_pg.py 已运行（数据已迁移）
  3. 新代码已部署并通过 smoke test

运行方式：
  cd backend
  python scripts/migrations/004_drop_dolphindb_migrated_tables.py

  加 --dry-run 只打印不执行：
  python scripts/migrations/004_drop_dolphindb_migrated_tables.py --dry-run
"""
import sys
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# 已迁移到 PostgreSQL 的 DolphinDB 表（按迁移目标分组）
MIGRATED_TABLES = {
    # 调度编排 → flow_configs / flow_runs / task_runs
    "flow_config": "flow_configs (PostgreSQL)",
    "flow_run": "flow_runs (PostgreSQL)",
    "task_run": "task_runs (PostgreSQL)",
    # 任务配置 → sync_task_configs / etl_task_configs
    "sync_task_config": "sync_task_configs (PostgreSQL)",
    "etl_task_config": "etl_task_configs (PostgreSQL)",
    # 因子配置 → factor_configs / factor_field_mappings
    "factor_metadata": "factor_configs (PostgreSQL)",
    "factor_data_config": "factor_field_mappings (PostgreSQL)",
    # 参考数据 → stocks / trading_calendar / index_configs / user_preferences
    "stock_basic": "stocks (PostgreSQL)",
    "trade_cal": "trading_calendar (PostgreSQL)",
    "index_metadata": "index_configs (PostgreSQL)",
    "user_sync_preference": "user_preferences (PostgreSQL)",
    # 结果表 → factor_analysis_results
    "factor_analysis": "factor_analysis_results (PostgreSQL)",
    "factor_analysis_extended": "factor_analysis_results (PostgreSQL)",
}

# DolphinDB 保留的时序表（不删除）
RETAINED_TABLES = [
    "factor_values",
    "index_constituents",
    "sync_daily_data",
    "sync_adj_factor",
    # 以及所有其他 sync_task_config 动态创建的行情表
]


def verify_pg_tables(pg_tables: list[str]) -> bool:
    """验证 PostgreSQL 目标表存在且有数据"""
    import psycopg2
    import psycopg2.extras

    sys.path.insert(0, ".")
    from app.core.config import settings

    conn = psycopg2.connect(
        host=settings.postgresql.postgres_host,
        port=settings.postgresql.postgres_port,
        dbname=settings.postgresql.postgres_db,
        user=settings.postgresql.postgres_user,
        password=settings.postgresql.postgres_password,
    )
    ok = True
    with conn:
        with conn.cursor() as cur:
            for tbl in pg_tables:
                cur.execute(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = %s AND table_schema = 'public')",
                    (tbl,),
                )
                exists = cur.fetchone()[0]
                if not exists:
                    logger.error(f"  PostgreSQL 表 {tbl} 不存在！中止删除。")
                    ok = False
                else:
                    cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                    cnt = cur.fetchone()[0]
                    logger.info(f"  PostgreSQL {tbl}: {cnt} 行 ✓")
    conn.close()
    return ok


def drop_dolphindb_table(db_client, table_name: str, dry_run: bool) -> bool:
    """删除单张 DolphinDB 表"""
    try:
        # 先检查表是否存在
        exists_result = db_client.query(
            f"existsTable('dfs://quant', '{table_name}')"
        )
        if exists_result.is_empty() or not exists_result.row(0)[0]:
            logger.info(f"  {table_name}: 不存在，跳过")
            return True

        if dry_run:
            logger.info(f"  [DRY-RUN] 将删除 DolphinDB 表: {table_name}")
            return True

        db_client.execute(f"dropTable(database('dfs://quant'), '{table_name}')")
        logger.info(f"  {table_name}: 已删除 ✓")
        return True
    except Exception as e:
        logger.error(f"  {table_name}: 删除失败 — {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="删除已迁移到 PostgreSQL 的 DolphinDB 表")
    parser.add_argument("--dry-run", action="store_true", help="只打印，不执行删除")
    parser.add_argument("--skip-verify", action="store_true", help="跳过 PostgreSQL 验证步骤")
    parser.add_argument("--tables", nargs="+", help="只删除指定表（默认删除全部 13 张）")
    args = parser.parse_args()

    sys.path.insert(0, ".")

    tables_to_drop = args.tables if args.tables else list(MIGRATED_TABLES.keys())

    # 验证指定表名合法
    for t in tables_to_drop:
        if t not in MIGRATED_TABLES:
            logger.error(f"未知表名: {t}。合法表名: {list(MIGRATED_TABLES.keys())}")
            sys.exit(1)

    logger.info("=" * 60)
    logger.info("Step 4: 删除已迁移的 DolphinDB 表")
    logger.info(f"模式: {'DRY-RUN' if args.dry_run else '实际执行'}")
    logger.info(f"待删除: {tables_to_drop}")
    logger.info("=" * 60)

    # Step 1: 验证 PostgreSQL 目标表
    if not args.skip_verify:
        logger.info("\n[1/3] 验证 PostgreSQL 目标表...")
        pg_targets = list({v.split(" ")[0] for v in MIGRATED_TABLES.values()})
        if not verify_pg_tables(pg_targets):
            logger.error("PostgreSQL 验证失败，中止。使用 --skip-verify 跳过此步骤。")
            sys.exit(1)
        logger.info("PostgreSQL 验证通过 ✓")
    else:
        logger.info("\n[1/3] 跳过 PostgreSQL 验证")

    # Step 2: 连接 DolphinDB
    logger.info("\n[2/3] 连接 DolphinDB...")
    from infrastructure.database.dolphindb_client import db_client
    logger.info("DolphinDB 连接成功 ✓")

    # Step 3: 删除表
    logger.info(f"\n[3/3] 删除 {len(tables_to_drop)} 张 DolphinDB 表...")
    success, failed = 0, []
    for table in tables_to_drop:
        pg_target = MIGRATED_TABLES[table]
        logger.info(f"  {table} → {pg_target}")
        if drop_dolphindb_table(db_client, table, args.dry_run):
            success += 1
        else:
            failed.append(table)

    logger.info("\n" + "=" * 60)
    if args.dry_run:
        logger.info(f"DRY-RUN 完成: {success} 张表将被删除")
    else:
        logger.info(f"完成: {success} 张表已删除, {len(failed)} 张失败")
        if failed:
            logger.error(f"失败的表: {failed}")
            sys.exit(1)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
