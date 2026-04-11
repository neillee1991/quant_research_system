"""
Seed data for flow_configs table
"""

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
