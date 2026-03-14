"""
种子因子初始化脚本
直接向数据库写入预定义的因子
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from datetime import datetime
import polars as pl
from store.dolphindb_client import db_client
from app.core.logger import logger


# 预定义的种子因子
SEED_FACTORS = [
    {
        "factor_id": "factor_ma_5",
        "description": "5日移动平均线",
        "category": "technical",
        "compute_mode": "incremental",
        "storage_target": "factor_values",
        "depends_on": ["sync_daily_data"],
        "params": {"window": 5, "lookback_days": 60},
        "code": """import polars as pl

def compute(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    \"\"\"计算5日移动平均线\"\"\"
    window = params.get("window", 5)
    result = df.with_columns([
        pl.col("close").rolling_mean(window).alias("factor_value")
    ])
    return result.select(["ts_code", "trade_date", "factor_value"])
"""
    },
    {
        "factor_id": "factor_ma_10",
        "description": "10日移动平均线",
        "category": "technical",
        "compute_mode": "incremental",
        "storage_target": "factor_values",
        "depends_on": ["sync_daily_data"],
        "params": {"window": 10, "lookback_days": 60},
        "code": """import polars as pl

def compute(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    \"\"\"计算10日移动平均线\"\"\"
    window = params.get("window", 10)
    result = df.with_columns([
        pl.col("close").rolling_mean(window).alias("factor_value")
    ])
    return result.select(["ts_code", "trade_date", "factor_value"])
"""
    },
    {
        "factor_id": "factor_ma_20",
        "description": "20日移动平均线",
        "category": "technical",
        "compute_mode": "incremental",
        "storage_target": "factor_values",
        "depends_on": ["sync_daily_data"],
        "params": {"window": 20, "lookback_days": 60},
        "code": """import polars as pl

def compute(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    \"\"\"计算20日移动平均线\"\"\"
    window = params.get("window", 20)
    result = df.with_columns([
        pl.col("close").rolling_mean(window).alias("factor_value")
    ])
    return result.select(["ts_code", "trade_date", "factor_value"])
"""
    },
    {
        "factor_id": "factor_rsi_14",
        "description": "14日相对强弱指标",
        "category": "technical",
        "compute_mode": "incremental",
        "storage_target": "factor_values",
        "depends_on": ["sync_daily_data"],
        "params": {"window": 14, "lookback_days": 60},
        "code": """import polars as pl

def compute(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    \"\"\"计算RSI指标\"\"\"
    window = params.get("window", 14)

    # 计算价格变化
    df = df.with_columns([
        (pl.col("close") - pl.col("close").shift(1)).alias("price_change")
    ])

    # 分离涨跌
    df = df.with_columns([
        pl.when(pl.col("price_change") > 0).then(pl.col("price_change")).otherwise(0).alias("gain"),
        pl.when(pl.col("price_change") < 0).then(-pl.col("price_change")).otherwise(0).alias("loss")
    ])

    # 计算平均涨跌
    df = df.with_columns([
        pl.col("gain").rolling_mean(window).alias("avg_gain"),
        pl.col("loss").rolling_mean(window).alias("avg_loss")
    ])

    # 计算RSI
    df = df.with_columns([
        (100 - (100 / (1 + pl.col("avg_gain") / pl.col("avg_loss")))).alias("factor_value")
    ])

    return df.select(["ts_code", "trade_date", "factor_value"])
"""
    },
    {
        "factor_id": "factor_volatility_20",
        "description": "20日波动率",
        "category": "risk",
        "compute_mode": "incremental",
        "storage_target": "factor_values",
        "depends_on": ["sync_daily_data"],
        "params": {"window": 20, "lookback_days": 60},
        "code": """import polars as pl

def compute(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    \"\"\"计算收益率波动率\"\"\"
    window = params.get("window", 20)

    # 计算收益率
    df = df.with_columns([
        (pl.col("close") / pl.col("close").shift(1) - 1).alias("returns")
    ])

    # 计算波动率（标准差）
    df = df.with_columns([
        pl.col("returns").rolling_std(window).alias("factor_value")
    ])

    return df.select(["ts_code", "trade_date", "factor_value"])
"""
    },
]


def seed_factors(overwrite: bool = False):
    """
    向数据库写入种子因子

    Args:
        overwrite: 是否覆盖已存在的因子
    """
    import json

    logger.info(f"开始种子因子初始化，共 {len(SEED_FACTORS)} 个因子")

    for factor in SEED_FACTORS:
        factor_id = factor["factor_id"]

        # 检查因子是否已存在
        existing = db_client.query(
            "SELECT factor_id FROM factor_metadata WHERE factor_id = %s",
            (factor_id,)
        )

        if not existing.is_empty() and not overwrite:
            logger.info(f"因子 {factor_id} 已存在，跳过")
            continue

        # 准备数据
        now = datetime.now()
        factor_df = pl.DataFrame({
            "factor_id": [factor_id],
            "description": [factor["description"]],
            "category": [factor["category"]],
            "compute_mode": [factor["compute_mode"]],
            "storage_target": [factor["storage_target"]],
            "depends_on": [json.dumps(factor["depends_on"])],
            "params": [json.dumps(factor["params"])],
            "code": [factor["code"]],
            "enabled": [True],
            "created_at": [now],
            "updated_at": [now],
        })

        # 写入数据库
        try:
            db_client.upsert("factor_metadata", factor_df, ["factor_id"])
            logger.info(f"✓ 种子因子 {factor_id} 写入成功")
        except Exception as e:
            logger.error(f"✗ 种子因子 {factor_id} 写入失败: {e}")

    logger.info("种子因子初始化完成")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="种子因子初始化")
    parser.add_argument("--overwrite", action="store_true", help="覆盖已存在的因子")
    args = parser.parse_args()

    seed_factors(overwrite=args.overwrite)
