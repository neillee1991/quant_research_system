"""
Seed data for flow_config table
"""
from typing import Any
from app.core.logger import logger
from store.dolphindb_client import db_client

DEFAULT_FLOWS = [
    {
        "name": "daily_data_sync",
        "description": "每日数据同步流水线: 同步行情 → 计算因子",
        "cron": "0 18 * * 1-5",
        "tags": ["data-sync", "daily"],
        "enabled": True,
        "date_offset_days": -1,
        "tasks": [
            {"id": "sync_daily", "type": "sync", "depends_on": []},
            {"id": "sync_daily_basic", "type": "sync", "depends_on": []},
            {"id": "sync_adj_factor", "type": "sync", "depends_on": []},
            {"id": "factor_ma_20", "type": "factor", "depends_on": ["sync_daily"]},
            {"id": "factor_pe_rank", "type": "factor", "depends_on": ["sync_daily_basic"]},
        ],
    },
    {
        "name": "weekly_analysis",
        "description": "每周分析流水线: 同步基础数据 → 计算技术因子",
        "cron": "0 3 * * 6",
        "tags": ["analysis", "weekly"],
        "enabled": True,
        "date_offset_days": -1,
        "tasks": [
            {"id": "sync_stock_basic", "type": "sync", "depends_on": []},
            {"id": "sync_daily", "type": "sync", "depends_on": []},
            {"id": "factor_rsi_14", "type": "factor", "depends_on": ["sync_daily"]},
        ],
    },
]


def seed_flow_config(force: bool = False) -> None:
    """
    Seed flow_config table with default flows

    Args:
        force: If True, delete existing data first
    """
    try:
        if force:
            logger.info("Clearing existing flow_config data")
            db_client.query("DELETE FROM flow_config")

        # Check if data already exists
        check_df = db_client.query("SELECT count(*) as cnt FROM flow_config")
        if not check_df.is_empty() and check_df["cnt"][0] > 0 and not force:
            logger.info("flow_config already has data, skipping seed")
            return

        from datetime import datetime
        import json

        rows = []
        for flow in DEFAULT_FLOWS:
            rows.append({
                "name": flow["name"],
                "description": flow["description"],
                "cron": flow["cron"],
                "tags": json.dumps(flow["tags"]),
                "enabled": flow["enabled"],
                "date_offset_days": flow["date_offset_days"],
                "tasks": json.dumps(flow["tasks"]),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "version": 1,
            })

        if rows:
            import polars as pl
            df = pl.DataFrame(rows)
            db_client.upsert("flow_config", df, ["name"])
            logger.info(f"Seeded {len(rows)} flows into flow_config")

    except Exception as e:
        logger.error(f"Failed to seed flow_config: {e}", exc_info=True)
        raise
