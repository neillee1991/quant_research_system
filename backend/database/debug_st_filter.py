#!/usr/bin/env python3
"""
诊断脚本：追查 000004.SZ 在 20260227 的 factor_ma_3 因子为何未被 ST 过滤
按数据流顺序检查每一层
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from store.dolphindb_client import db_client

TS_CODE   = "000004.SZ"
DATE      = "20260227"
FACTOR_ID = "factor_ma_3"

SEP = "-" * 60

def check(title, df):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(SEP)
    if df is None or df.is_empty():
        print("  (空结果)")
    else:
        print(df)
    return df

# ----------------------------------------------------------------
# 1. 确认因子值确实存在
# ----------------------------------------------------------------
check(
    "1. factor_values 中该股该日的因子值",
    db_client.query(
        "SELECT ts_code, trade_date, factor_id, factor_value "
        "FROM factor_values "
        "WHERE ts_code = %s AND trade_date = %s AND factor_id = %s",
        (TS_CODE, DATE, FACTOR_ID)
    )
)

# ----------------------------------------------------------------
# 2. stock_daily_status 中该股该日的状态
# ----------------------------------------------------------------
check(
    "2. stock_daily_status 中该股该日的 is_st",
    db_client.query(
        "SELECT ts_code, trade_date, is_st, is_suspend, is_limit "
        "FROM stock_daily_status "
        "WHERE ts_code = %s AND trade_date = %s",
        (TS_CODE, DATE)
    )
)

# ----------------------------------------------------------------
# 3. stock_daily_status 中该股最近 5 条记录（看 is_st 变化趋势）
# ----------------------------------------------------------------
check(
    "3. stock_daily_status 该股最近 5 条（含前后日期）",
    db_client.query(
        "SELECT ts_code, trade_date, is_st, is_suspend, is_limit "
        "FROM stock_daily_status "
        "WHERE ts_code = %s AND trade_date >= %s AND trade_date <= %s "
        "ORDER BY trade_date",
        (TS_CODE, "20260220", "20260305")
    )
)

# ----------------------------------------------------------------
# 4. 查最近一次 factor_ma_3 运行记录，看用的是哪套 preprocess
# ----------------------------------------------------------------
check(
    "4. factor_ma_3 最近一次运行记录（preprocess 配置）",
    db_client.query(
        "SELECT factor_id, mode, status, start_date, end_date, "
        "rows_affected, preprocess, created_at "
        "FROM factor_task_run "
        "WHERE factor_id = %s "
        "ORDER BY created_at DESC "
        "LIMIT 3",
        (FACTOR_ID,)
    )
)

# ----------------------------------------------------------------
# 5. factor_metadata 中 factor_ma_3 的 params（含 preprocess 配置）
# ----------------------------------------------------------------
check(
    "5. factor_metadata 中 factor_ma_3 的参数",
    db_client.query(
        "SELECT factor_id, compute_mode, params, last_computed_date "
        "FROM factor_metadata "
        "WHERE factor_id = %s",
        (FACTOR_ID,)
    )
)

# ----------------------------------------------------------------
# 6. 检查 stock_daily_status 中 20260227 这天 is_st=1 的总行数
#    （验证这天的状态数据是否整体缺失/未同步）
# ----------------------------------------------------------------
check(
    "6. stock_daily_status 中 20260227 这天 is_st 统计",
    db_client.query(
        "SELECT is_st, count(*) AS cnt "
        "FROM stock_daily_status "
        "WHERE trade_date = %s "
        "GROUP BY is_st",
        (DATE,)
    )
)

# ----------------------------------------------------------------
# 7. 检查 stock_daily_status 表最新日期（看数据是否同步到位）
# ----------------------------------------------------------------
check(
    "7. stock_daily_status 最新日期",
    db_client.query(
        "SELECT max(trade_date) AS latest_date FROM stock_daily_status"
    )
)
