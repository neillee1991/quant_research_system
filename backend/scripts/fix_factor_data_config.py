"""
初始化 factor_data_config 表的基础数据源配置

用途：
- 为新用户初始化常用的数据源配置（sync_daily_data, sync_daily_basic, sync_adj_factor）
- 系统不再提供内置默认配置，所有数据源必须在 factor_data_config 表中配置

何时使用此脚本：
- 首次部署系统时，初始化基础数据源配置
- 添加新的数据源配置时
"""
import sys
sys.path.insert(0, '/Users/lisheng/Code/quantsystem/quant_research_system/backend')

from infrastructure.database.dolphindb_client import db_client
import polars as pl
from datetime import datetime

def main():
    # 获取 DB 客户端
    db = db_client

    # 基础数据源配置（因子计算的核心依赖）
    data_sources = [
        {
            "field_key": "sync_daily_data",
            "description": "日线行情数据（开高低收、成交量等）",
            "table_name": "sync_daily_data",
            "column_name": "open,high,low,close,vol,amount,pct_chg",
            "extra_config": "{}",
        },
        {
            "field_key": "sync_daily_basic",
            "description": "每日指标（换手率、市盈率、市值等）",
            "table_name": "sync_daily_basic",
            "column_name": "close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv",
            "extra_config": "{}",
        },
        {
            "field_key": "sync_adj_factor",
            "description": "复权因子",
            "table_name": "sync_adj_factor",
            "column_name": "adj_factor",
            "extra_config": "{}",
        },
    ]

    now = datetime.now()

    # 创建 DataFrame
    df = pl.DataFrame({
        "field_key": [d["field_key"] for d in data_sources],
        "description": [d["description"] for d in data_sources],
        "table_name": [d["table_name"] for d in data_sources],
        "column_name": [d["column_name"] for d in data_sources],
        "extra_config": [d["extra_config"] for d in data_sources],
        "updated_at": [now] * len(data_sources),
    })

    # 写入数据库（upsert）
    print(f"准备写入 {len(df)} 条基础数据源配置...")

    for row in df.to_dicts():
        field_key = row["field_key"]

        # 检查是否已存在
        existing = db.query(
            f"SELECT * FROM factor_data_config WHERE field_key = '{field_key}'"
        )

        if not existing.is_empty():
            print(f"  - {field_key}: 已存在，跳过")
            continue

        # 插入新记录
        insert_df = pl.DataFrame([row])
        pdf = insert_df.to_pandas()

        db.session.upload({"tmp_data": pdf})
        db.session.run(
            "handle = loadTable('dfs://quant', 'factor_data_config');"
            "tableInsert(handle, tmp_data);"
            "undef('tmp_data')"
        )
        print(f"  - {field_key}: 已添加")

    print("\n✅ 基础数据源配置已添加完成！")
    print("系统现在使用 factor_data_config 表作为唯一的数据源配置")


if __name__ == "__main__":
    main()
