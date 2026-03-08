"""
DolphinDB 数据库客户端（向后兼容层）
此文件保持向后兼容，重新导出新的模块化实现

新的实现位于: infrastructure/database/
- connection.py: 连接管理
- sql_adapter.py: SQL 适配器
- type_converter.py: 类型转换
- table_manager.py: 表管理
- data_operations.py: 数据操作
- metadata_manager.py: 元数据管理
- dolphindb_client.py: 门面模式客户端
"""
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl

from app.core.logger import logger

# 重新导出新的实现
from infrastructure.database.dolphindb_client import (
    DolphinDBClient as _NewDolphinDBClient,
    db_client as _new_db_client,
)


class DolphinDBClient(_NewDolphinDBClient):
    """
    DolphinDB 数据库客户端（向后兼容）

    继承新的模块化实现，并添加种子数据方法
    """

    def __init__(self):
        """初始化客户端"""
        super().__init__()

    # ------------------------------------------------------------------
    # 种子数据方法（包含大量配置数据）
    # ------------------------------------------------------------------

    def seed_sync_task_config(self) -> None:
        """
        如果 sync_task_config 表为空，则写入默认同步任务定义。
        仅在首次启动时生效，后续可通过 API 增删改。
        """
        try:
            count = self.query("SELECT count(*) as cnt FROM sync_task_config")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("sync_task_config 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.debug(f"检查 sync_task_config 数据失败（表可能刚创建）: {e}")
            # 表可能刚创建，继续 seed

        now = datetime.now()
        # 默认同步任务定义（基于 Tushare Pro API）
        tasks = [
            # ==================== 全量同步 ====================
            {
                "task_id": "sync_stock_basic",
                "api_name": "stock_basic",
                "description": "股票基础信息（代码、名称、行业、上市日期等）",
                "sync_type": "full",
                "date_field": "",
                "table_name": "sync_stock_basic",
                "params": {
                    "exchange": "",
                    "list_status": "",
                    "fields": "ts_code,symbol,name,area,industry,market,list_date",
                },
                "primary_keys": ["ts_code"],
                "api_limit": 5000,
                "schema": {
                    "ts_code": {"type": "SYMBOL"},
                    "symbol": {"type": "STRING"},
                    "name": {"type": "STRING"},
                    "area": {"type": "STRING"},
                    "industry": {"type": "STRING"},
                    "market": {"type": "STRING"},
                    "list_date": {"type": "STRING"},
                },
            },
            {
                "task_id": "sync_trade_cal",
                "api_name": "trade_cal",
                "description": "交易日历（A股交易日、休市日）",
                "sync_type": "full",
                "date_field": "",
                "table_name": "sync_trade_cal",
                "params": {
                    "exchange": "SSE",
                    "start_date": "20100101",
                    "end_date": "20301231",
                    "fields": "exchange,cal_date,is_open,pretrade_date",
                },
                "primary_keys": ["exchange", "cal_date"],
                "api_limit": 10000,
                "schema": {
                    "exchange": {"type": "SYMBOL"},
                    "cal_date": {"type": "DATE"},
                    "is_open": {"type": "INT"},
                    "pretrade_date": {"type": "DATE"},
                },
            },
            # ==================== 增量同步 ====================
            {
                "task_id": "sync_daily_data",
                "api_name": "daily",
                "description": "日线行情（开高低收、成交量、成交额）",
                "sync_type": "incremental",
                "date_field": "trade_date",
                "table_name": "sync_daily_data",
                "params": {
                    "trade_date": "{date}",
                    "fields": "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
                },
                "primary_keys": ["ts_code", "trade_date"],
                "api_limit": 5000,
                "schema": {
                    "ts_code": {"type": "SYMBOL"},
                    "trade_date": {"type": "DATE"},
                    "open": {"type": "DOUBLE"},
                    "high": {"type": "DOUBLE"},
                    "low": {"type": "DOUBLE"},
                    "close": {"type": "DOUBLE"},
                    "pre_close": {"type": "DOUBLE"},
                    "change": {"type": "DOUBLE"},
                    "pct_chg": {"type": "DOUBLE"},
                    "vol": {"type": "DOUBLE"},
                    "amount": {"type": "DOUBLE"},
                },
            },
            {
                "task_id": "sync_adj_factor",
                "api_name": "adj_factor",
                "description": "复权因子（用于前复权、后复权计算）",
                "sync_type": "incremental",
                "date_field": "trade_date",
                "table_name": "sync_adj_factor",
                "params": {
                    "trade_date": "{date}",
                    "fields": "ts_code,trade_date,adj_factor",
                },
                "primary_keys": ["ts_code", "trade_date"],
                "api_limit": 5000,
                "schema": {
                    "ts_code": {"type": "SYMBOL"},
                    "trade_date": {"type": "DATE"},
                    "adj_factor": {"type": "DOUBLE"},
                },
            },
            {
                "task_id": "sync_daily_basic",
                "api_name": "daily_basic",
                "description": "每日指标（换手率、量比、市盈率、市净率、市值等）",
                "sync_type": "incremental",
                "date_field": "trade_date",
                "table_name": "sync_daily_basic",
                "params": {
                    "trade_date": "{date}",
                    "fields": "ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv",
                },
                "primary_keys": ["ts_code", "trade_date"],
                "api_limit": 6000,
                "schema": {
                    "ts_code": {"type": "SYMBOL"},
                    "trade_date": {"type": "DATE"},
                    "close": {"type": "DOUBLE"},
                    "turnover_rate": {"type": "DOUBLE"},
                    "turnover_rate_f": {"type": "DOUBLE"},
                    "volume_ratio": {"type": "DOUBLE"},
                    "pe": {"type": "DOUBLE"},
                    "pe_ttm": {"type": "DOUBLE"},
                    "pb": {"type": "DOUBLE"},
                    "ps": {"type": "DOUBLE"},
                    "ps_ttm": {"type": "DOUBLE"},
                    "dv_ratio": {"type": "DOUBLE"},
                    "dv_ttm": {"type": "DOUBLE"},
                    "total_share": {"type": "DOUBLE"},
                    "float_share": {"type": "DOUBLE"},
                    "free_share": {"type": "DOUBLE"},
                    "total_mv": {"type": "DOUBLE"},
                    "circ_mv": {"type": "DOUBLE"},
                },
            },
            {
                "task_id": "sync_sw_index_member_Y",
                "api_name": "index_member_all",
                "description": "申万行业成分构成（三级分类，含纳入/剔除日期）",
                "sync_type": "full",
                "date_field": "",
                "table_name": "sync_sw_index_member_Y",
                "params": {
                    "is_new": "Y",
                    "fields": "l1_code,l1_name,l2_code,l2_name,l3_code,l3_name,ts_code,name,in_date,out_date,is_new",
                },
                "primary_keys": [],
                "api_limit": 2000,
                "schema": {
                    "l1_code": {"type": "SYMBOL"},
                    "l1_name": {"type": "STRING"},
                    "l2_code": {"type": "SYMBOL"},
                    "l2_name": {"type": "STRING"},
                    "l3_code": {"type": "SYMBOL"},
                    "l3_name": {"type": "STRING"},
                    "ts_code": {"type": "SYMBOL"},
                    "name": {"type": "STRING"},
                    "in_date": {"type": "STRING"},
                    "out_date": {"type": "STRING"},
                    "is_new": {"type": "STRING"},
                },
            },
            {
                "task_id": "sync_sw_index_member_N",
                "api_name": "index_member_all",
                "description": "申万行业成分构成（三级分类，含纳入/剔除日期）",
                "sync_type": "full",
                "date_field": "",
                "table_name": "sync_sw_index_member_N",
                "params": {
                    "is_new": "N",
                    "fields": "l1_code,l1_name,l2_code,l2_name,l3_code,l3_name,ts_code,name,in_date,out_date,is_new",
                },
                "primary_keys": [],
                "api_limit": 2000,
                "schema": {
                    "l1_code": {"type": "SYMBOL"},
                    "l1_name": {"type": "STRING"},
                    "l2_code": {"type": "SYMBOL"},
                    "l2_name": {"type": "STRING"},
                    "l3_code": {"type": "SYMBOL"},
                    "l3_name": {"type": "STRING"},
                    "ts_code": {"type": "SYMBOL"},
                    "name": {"type": "STRING"},
                    "in_date": {"type": "STRING"},
                    "out_date": {"type": "STRING"},
                    "is_new": {"type": "STRING"},
                },
            },
            {
                "task_id": "sync_ci_index_member_Y",
                "api_name": "ci_index_member",
                "description": "中信行业成分构成（三级分类，含纳入/剔除日期）",
                "sync_type": "full",
                "date_field": "",
                "table_name": "sync_ci_index_member_Y",
                "params": {
                    "is_new": "Y",
                    "fields": "l1_code,l1_name,l2_code,l2_name,l3_code,l3_name,ts_code,name,in_date,out_date,is_new",
                },
                "primary_keys": [],
                "api_limit": 2000,
                "schema": {
                    "l1_code": {"type": "SYMBOL"},
                    "l1_name": {"type": "STRING"},
                    "l2_code": {"type": "SYMBOL"},
                    "l2_name": {"type": "STRING"},
                    "l3_code": {"type": "SYMBOL"},
                    "l3_name": {"type": "STRING"},
                    "ts_code": {"type": "SYMBOL"},
                    "name": {"type": "STRING"},
                    "in_date": {"type": "STRING"},
                    "out_date": {"type": "STRING"},
                    "is_new": {"type": "STRING"},
                },
            },
            {
                "task_id": "sync_ci_index_member_N",
                "api_name": "ci_index_member",
                "description": "中信行业成分构成（三级分类，含纳入/剔除日期）",
                "sync_type": "full",
                "date_field": "",
                "table_name": "sync_ci_index_member_N",
                "params": {
                    "is_new": "N",
                    "fields": "l1_code,l1_name,l2_code,l2_name,l3_code,l3_name,ts_code,name,in_date,out_date,is_new",
                },
                "primary_keys": [],
                "api_limit": 2000,
                "schema": {
                    "l1_code": {"type": "SYMBOL"},
                    "l1_name": {"type": "STRING"},
                    "l2_code": {"type": "SYMBOL"},
                    "l2_name": {"type": "STRING"},
                    "l3_code": {"type": "SYMBOL"},
                    "l3_name": {"type": "STRING"},
                    "ts_code": {"type": "SYMBOL"},
                    "name": {"type": "STRING"},
                    "in_date": {"type": "STRING"},
                    "out_date": {"type": "STRING"},
                    "is_new": {"type": "STRING"},
                },
            },
            {
                "task_id": "sync_stk_limit",
                "api_name": "stk_limit",
                "description": "每日涨跌停价格（涨停价、跌停价、昨日收盘价）",
                "sync_type": "incremental",
                "date_field": "trade_date",
                "table_name": "sync_stk_limit",
                "params": {
                    "trade_date": "{date}",
                    "fields": "ts_code,trade_date,pre_close,up_limit,down_limit",
                },
                "primary_keys": ["ts_code", "trade_date"],
                "api_limit": 5000,
                "schema": {
                    "ts_code": {"type": "SYMBOL"},
                    "trade_date": {"type": "DATE"},
                    "pre_close": {"type": "DOUBLE"},
                    "up_limit": {"type": "DOUBLE"},
                    "down_limit": {"type": "DOUBLE"},
                },
            },
            {
                "task_id": "sync_suspend_d",
                "api_name": "suspend_d",
                "description": "每日停复牌信息（停牌/复牌类型、日内停牌时间段）",
                "sync_type": "incremental",
                "date_field": "trade_date",
                "table_name": "sync_suspend_d",
                "params": {
                    "trade_date": "{date}",
                    "fields": "ts_code,trade_date,suspend_timing,suspend_type",
                },
                "primary_keys": ["ts_code", "trade_date", "suspend_type"],
                "api_limit": 5000,
                "schema": {
                    "ts_code": {"type": "SYMBOL"},
                    "trade_date": {"type": "DATE"},
                    "suspend_timing": {"type": "STRING"},
                    "suspend_type": {"type": "STRING"},
                },
            },
            {
                "task_id": "sync_stock_st",
                "api_name": "stock_st",
                "description": "ST股票列表（每日ST/退市风险警示股票）",
                "sync_type": "incremental",
                "date_field": "trade_date",
                "table_name": "sync_stock_st",
                "params": {
                    "trade_date": "{date}",
                    "fields": "ts_code,name,trade_date,type,type_name",
                },
                "primary_keys": ["ts_code", "trade_date"],
                "api_limit": 1000,
                "schema": {
                    "ts_code": {"type": "SYMBOL"},
                    "name": {"type": "STRING"},
                    "trade_date": {"type": "DATE"},
                    "type": {"type": "STRING"},
                    "type_name": {"type": "STRING"},
                },
            },
        ]

        seed_df = pl.DataFrame({
            "task_id": [t["task_id"] for t in tasks],
            "api_name": [t["api_name"] for t in tasks],
            "description": [t["description"] for t in tasks],
            "sync_type": [t["sync_type"] for t in tasks],
            "params_json": [json.dumps(t["params"]) for t in tasks],
            "date_field": [t["date_field"] for t in tasks],
            "primary_keys_json": [json.dumps(t["primary_keys"]) for t in tasks],
            "table_name": [t["table_name"] for t in tasks],
            "schema_json": [json.dumps(t["schema"]) for t in tasks],
            "enabled": [True] * len(tasks),
            "api_limit": [t["api_limit"] for t in tasks],
            "created_at": [now] * len(tasks),
            "updated_at": [now] * len(tasks),
        })
        self.upsert("sync_task_config", seed_df, ["task_id"])
        logger.info(f"已写入 {len(tasks)} 条默认同步任务配置")

    def seed_etl_task_config(self) -> None:
        """
        如果 etl_task_config 表为空，则写入默认 ETL 任务定义。
        仅在首次启动时生效，后续可通过 API 增删改。
        """
        try:
            count = self.query("SELECT count(*) as cnt FROM etl_task_config")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("etl_task_config 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.warning(f"检查 etl_task_config 表时出错（可能表不存在，将继续 seed）: {e}")

        now = datetime.now()
        db_meta = self._db_path

        # ETL 任务脚本定义
        script_index_member = (
            f't_sw = select l1_code, l1_name, l2_code, l2_name, l3_code, l3_name, '
            f'ts_code, in_date, out_date, "sw" as source '
            f'from loadTable("{db_meta}", "sync_sw_index_member_Y"); '
            f't_ci = select l1_code, l1_name, l2_code, l2_name, l3_code, l3_name, '
            f'ts_code, in_date, out_date, "ci" as source '
            f'from loadTable("{db_meta}", "sync_ci_index_member_Y"); '
            f'unionAll([t_sw, t_ci], false)'
        )

        script_index_member_daily = (
            f'daily = select trade_date, ts_code '
            f'from loadTable("dfs://quant_ts", "sync_daily_data"); '
            f'etl = select ts_code, source, l1_code, l1_name, l2_code, l2_name, l3_code, l3_name, '
            f'in_date, out_date '
            f'from loadTable("{db_meta}", "etl_index_member"); '
            f'joined = lj(daily, etl, `ts_code); '
            f'select trade_date, ts_code, source, l1_code, l1_name, l2_code, l2_name, l3_code, l3_name '
            f'from joined '
            f'where isNull(in_date) '
            f'or (trade_date >= temporalParse(in_date, "yyyyMMdd") '
            f'and (isNull(out_date) or out_date = "" '
            f'or trade_date < temporalParse(out_date, "yyyyMMdd")))'
        )

        script_stock_daily_info = (
            f'daily = select trade_date, ts_code, open, high, low, close, pre_close, '
            f'pct_chg, vol, amount '
            f'from loadTable("dfs://quant_ts", "sync_daily_data"); '
            f'basic = select trade_date, ts_code, pe_ttm, pb, total_mv, circ_mv, turnover_rate '
            f'from loadTable("dfs://quant_ts", "sync_daily_basic"); '
            f'ind = select trade_date, ts_code, source, l1_code, l1_name, l2_code, l2_name, l3_code, l3_name '
            f'from loadTable("{db_meta}", "etl_index_member_daily"); '
            f't1 = lj(daily, basic, `trade_date`ts_code); '
            f'lj(t1, ind, `trade_date`ts_code)'
        )

        tasks = [
            {
                "task_id": "etl_index_member",
                "description": "合并申万+中信行业成员表（当前有效分类）",
                "script": script_index_member,
                "sync_type": "full",
                "date_field": "",
                "primary_keys": ["ts_code", "source", "l3_code"],
                "table_name": "etl_index_member",
            },
            {
                "task_id": "etl_index_member_daily",
                "description": "每只股票每个交易日所属行业（行情左关联行业成员）",
                "script": script_index_member_daily,
                "sync_type": "full",
                "date_field": "trade_date",
                "primary_keys": ["trade_date", "ts_code", "source", "l3_code"],
                "table_name": "etl_index_member_daily",
            },
            {
                "task_id": "etl_stock_daily_info",
                "description": "行情+基本面+行业宽表（每日全量刷新）",
                "script": script_stock_daily_info,
                "sync_type": "full",
                "date_field": "trade_date",
                "primary_keys": ["trade_date", "ts_code", "source"],
                "table_name": "etl_stock_daily_info",
            },
        ]

        seed_df = pl.DataFrame({
            "task_id": [t["task_id"] for t in tasks],
            "description": [t["description"] for t in tasks],
            "script": [t["script"] for t in tasks],
            "sync_type": [t["sync_type"] for t in tasks],
            "date_field": [t["date_field"] for t in tasks],
            "primary_keys_json": [json.dumps(t["primary_keys"]) for t in tasks],
            "table_name": [t["table_name"] for t in tasks],
            "enabled": [True] * len(tasks),
            "created_at": [now] * len(tasks),
            "updated_at": [now] * len(tasks),
        })
        self.upsert("etl_task_config", seed_df, ["task_id"])
        logger.info(f"已写入 {len(tasks)} 条默认 ETL 任务配置")

    def seed_factor_data_config(self) -> None:
        """
        如果 factor_data_config 表为空，则写入默认字段映射。
        仅在首次启动时生效，后续可通过 API 修改。
        """
        try:
            count = self.query("SELECT count(*) as cnt FROM factor_data_config")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("factor_data_config 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.debug(f"检查 factor_data_config 数据失败（表可能刚创建）: {e}")

        now = datetime.now()
        mappings = [
            {"field_key": "adj_factor", "description": "复权因子", "table_name": "sync_adj_factor", "column_name": "adj_factor", "extra_config": "{}"},
            {"field_key": "list_date", "description": "股票上市日期", "table_name": "sync_stock_basic", "column_name": "list_date", "extra_config": "{}"},
            {
                "field_key": "is_st",
                "description": "是否ST（0=正常, 1=ST/*ST/退市风险警示）。来源: sync_stock_st 表，当日有记录则为1",
                "table_name": "sync_stock_st",
                "column_name": "ts_code",
                "extra_config": '{"mode":"exists_in_table","values":{"0":"正常","1":"ST/*ST/退市风险警示"}}',
            },
            {
                "field_key": "is_suspend",
                "description": "是否停牌（0=正常交易, 1=全天停牌, 2=盘中临时停牌）。来源: sync_suspend_d 表",
                "table_name": "sync_suspend_d",
                "column_name": "suspend_type",
                "extra_config": '{"mode":"exists_in_table","filter":{"suspend_type":"S"},"values":{"0":"正常交易","1":"全天停牌(suspend_timing为空)","2":"盘中临时停牌(suspend_timing非空)"}}',
            },
            {
                "field_key": "is_limit",
                "description": "涨跌停状态（0=未涨跌停, 1=涨停, -1=跌停）。来源: sync_stk_limit + sync_daily_data，收盘价触及涨跌停价",
                "table_name": "sync_stk_limit",
                "column_name": "up_limit,down_limit",
                "extra_config": '{"mode":"compare_with_price","price_table":"sync_daily_data","price_column":"close","values":{"0":"未涨跌停","1":"涨停(close>=up_limit)","-1":"跌停(close<=down_limit)"}}',
            },
            {"field_key": "industry_l1", "description": "股票一级行业", "table_name": "sync_stock_basic", "column_name": "industry", "extra_config": "{}"},
            {"field_key": "industry_l2", "description": "股票二级行业", "table_name": "", "column_name": "", "extra_config": "{}"},
            {"field_key": "market_cap", "description": "股票总市值（万元）", "table_name": "sync_daily_basic", "column_name": "total_mv", "extra_config": "{}"},
        ]
        seed_df = pl.DataFrame({
            "field_key": [m["field_key"] for m in mappings],
            "description": [m["description"] for m in mappings],
            "table_name": [m["table_name"] for m in mappings],
            "column_name": [m["column_name"] for m in mappings],
            "extra_config": [m["extra_config"] for m in mappings],
            "updated_at": [now] * len(mappings),
        })
        self.upsert("factor_data_config", seed_df, ["field_key"])
        logger.info(f"已写入 {len(mappings)} 条默认因子数据配置")

    def seed_factor_metadata(self) -> None:
        """
        如果 factor_metadata 表为空，则写入默认种子因子定义。
        仅在首次启动时生效，后续可通过 API 增删改。
        """
        try:
            count = self.query("SELECT count(*) as cnt FROM factor_metadata")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("factor_metadata 已有数据，跳过 seed")
                return
        except Exception:
            pass

        now = datetime.now()
        factors = [
            # ==================== 技术指标因子（依赖 sync_daily_data）====================
            {
                "factor_id": "factor_ma_5",
                "description": "5日移动平均线",
                "category": "technical",
                "compute_mode": "incremental",
                "storage_target": "factor_values",
                "depends_on": json.dumps(["sync_daily_data"]),
                "params": json.dumps({"window": 5}),
                "code": (
                    "import polars as pl\n"
                    "def compute(df, params):\n"
                    "    window = params.get('window', 5)\n"
                    "    return (\n"
                    "        df.sort(['ts_code', 'trade_date'])\n"
                    "        .with_columns(\n"
                    "            pl.col('close').rolling_mean(window_size=window)\n"
                    "            .over('ts_code').alias('factor_value')\n"
                    "        )\n"
                    "        .select(['ts_code', 'trade_date', 'factor_value'])\n"
                    "    )\n"
                ),
            },
            {
                "factor_id": "factor_ma_20",
                "description": "20日移动平均线",
                "category": "technical",
                "compute_mode": "incremental",
                "storage_target": "factor_values",
                "depends_on": json.dumps(["sync_daily_data"]),
                "params": json.dumps({"window": 20}),
                "code": (
                    "import polars as pl\n"
                    "def compute(df, params):\n"
                    "    window = params.get('window', 20)\n"
                    "    return (\n"
                    "        df.sort(['ts_code', 'trade_date'])\n"
                    "        .with_columns(\n"
                    "            pl.col('close').rolling_mean(window_size=window)\n"
                    "            .over('ts_code').alias('factor_value')\n"
                    "        )\n"
                    "        .select(['ts_code', 'trade_date', 'factor_value'])\n"
                    "    )\n"
                ),
            },
            {
                "factor_id": "factor_momentum_20",
                "description": "20日价格动量（当日收盘价 / 20日前收盘价 - 1）",
                "category": "momentum",
                "compute_mode": "incremental",
                "storage_target": "factor_values",
                "depends_on": json.dumps(["sync_daily_data"]),
                "params": json.dumps({"window": 20}),
                "code": (
                    "import polars as pl\n"
                    "def compute(df, params):\n"
                    "    window = params.get('window', 20)\n"
                    "    return (\n"
                    "        df.sort(['ts_code', 'trade_date'])\n"
                    "        .with_columns(\n"
                    "            (pl.col('close') / pl.col('close').shift(window).over('ts_code') - 1)\n"
                    "            .alias('factor_value')\n"
                    "        )\n"
                    "        .select(['ts_code', 'trade_date', 'factor_value'])\n"
                    "    )\n"
                ),
            },
            {
                "factor_id": "factor_volatility_20",
                "description": "20日收益率波动率（年化）",
                "category": "technical",
                "compute_mode": "incremental",
                "storage_target": "factor_values",
                "depends_on": json.dumps(["sync_daily_data"]),
                "params": json.dumps({"window": 20}),
                "code": (
                    "import polars as pl\n"
                    "def compute(df, params):\n"
                    "    window = params.get('window', 20)\n"
                    "    return (\n"
                    "        df.sort(['ts_code', 'trade_date'])\n"
                    "        .with_columns(\n"
                    "            pl.col('close').pct_change().over('ts_code').alias('ret')\n"
                    "        )\n"
                    "        .with_columns(\n"
                    "            (pl.col('ret').rolling_std(window_size=window).over('ts_code') * (252 ** 0.5))\n"
                    "            .alias('factor_value')\n"
                    "        )\n"
                    "        .select(['ts_code', 'trade_date', 'factor_value'])\n"
                    "    )\n"
                ),
            },
            {
                "factor_id": "factor_volatility_10",
                "description": "10日收益率波动率（年化）",
                "category": "technical",
                "compute_mode": "incremental",
                "storage_target": "factor_values",
                "depends_on": json.dumps(["sync_daily_data"]),
                "params": json.dumps({"window": 10}),
                "code": (
                    "import polars as pl\n"
                    "def compute(df, params):\n"
                    "    window = params.get('window', 10)\n"
                    "    return (\n"
                    "        df.sort(['ts_code', 'trade_date'])\n"
                    "        .with_columns(\n"
                    "            pl.col('close').pct_change().over('ts_code').alias('ret')\n"
                    "        )\n"
                    "        .with_columns(\n"
                    "            (pl.col('ret').rolling_std(window_size=window).over('ts_code') * (252 ** 0.5))\n"
                    "            .alias('factor_value')\n"
                    "        )\n"
                    "        .select(['ts_code', 'trade_date', 'factor_value'])\n"
                    "    )\n"
                ),
            },
            {
                "factor_id": "factor_rsi_14",
                "description": "14日相对强弱指数（RSI，Wilder EWM法）",
                "category": "technical",
                "compute_mode": "incremental",
                "storage_target": "factor_values",
                "depends_on": json.dumps(["sync_daily_data"]),
                "params": json.dumps({"window": 14}),
                "code": (
                    "import polars as pl\n"
                    "def compute(df, params):\n"
                    "    window = params.get('window', 14)\n"
                    "    alpha = 1.0 / window\n"
                    "    return (\n"
                    "        df.sort(['ts_code', 'trade_date'])\n"
                    "        .with_columns(\n"
                    "            pl.col('close').diff().over('ts_code').alias('delta')\n"
                    "        )\n"
                    "        .with_columns(\n"
                    "            pl.when(pl.col('delta') > 0).then(pl.col('delta')).otherwise(0.0).alias('gain'),\n"
                    "            pl.when(pl.col('delta') < 0).then(-pl.col('delta')).otherwise(0.0).alias('loss'),\n"
                    "        )\n"
                    "        .with_columns(\n"
                    "            pl.col('gain').ewm_mean(alpha=alpha, adjust=False).over('ts_code').alias('avg_gain'),\n"
                    "            pl.col('loss').ewm_mean(alpha=alpha, adjust=False).over('ts_code').alias('avg_loss'),\n"
                    "        )\n"
                    "        .with_columns(\n"
                    "            (100.0 - 100.0 / (1.0 + pl.col('avg_gain') / (pl.col('avg_loss') + 1e-10)))\n"
                    "            .alias('factor_value')\n"
                    "        )\n"
                    "        .select(['ts_code', 'trade_date', 'factor_value'])\n"
                    "    )\n"
                ),
            },
            # ==================== 估值因子（依赖 sync_daily_basic）====================
            {
                "factor_id": "factor_pe_rank",
                "description": "PE百分位排名（截面，0~1）",
                "category": "value",
                "compute_mode": "incremental",
                "storage_target": "factor_values",
                "depends_on": json.dumps(["sync_daily_basic"]),
                "params": json.dumps({}),
                "code": (
                    "import polars as pl\n"
                    "def compute(df, params):\n"
                    "    return (\n"
                    "        df.sort(['trade_date', 'ts_code'])\n"
                    "        .with_columns(\n"
                    "            pl.col('pe_ttm').rank(method='average').over('trade_date').alias('_rank'),\n"
                    "            pl.col('pe_ttm').count().over('trade_date').alias('_total'),\n"
                    "        )\n"
                    "        .with_columns(\n"
                    "            (pl.col('_rank') / pl.col('_total')).alias('factor_value')\n"
                    "        )\n"
                    "        .select(['ts_code', 'trade_date', 'factor_value'])\n"
                    "    )\n"
                ),
            },
            {
                "factor_id": "factor_pb_rank",
                "description": "PB百分位排名（截面，0~1）",
                "category": "value",
                "compute_mode": "incremental",
                "storage_target": "factor_values",
                "depends_on": json.dumps(["sync_daily_basic"]),
                "params": json.dumps({}),
                "code": (
                    "import polars as pl\n"
                    "def compute(df, params):\n"
                    "    return (\n"
                    "        df.sort(['trade_date', 'ts_code'])\n"
                    "        .with_columns(\n"
                    "            pl.col('pb').rank(method='average').over('trade_date').alias('_rank'),\n"
                    "            pl.col('pb').count().over('trade_date').alias('_total'),\n"
                    "        )\n"
                    "        .with_columns(\n"
                    "            (pl.col('_rank') / pl.col('_total')).alias('factor_value')\n"
                    "        )\n"
                    "        .select(['ts_code', 'trade_date', 'factor_value'])\n"
                    "    )\n"
                ),
            },
        ]

        seed_df = pl.DataFrame({
            "factor_id": [f["factor_id"] for f in factors],
            "description": [f["description"] for f in factors],
            "category": [f["category"] for f in factors],
            "compute_mode": [f["compute_mode"] for f in factors],
            "storage_target": [f["storage_target"] for f in factors],
            "depends_on": [f["depends_on"] for f in factors],
            "params": [f["params"] for f in factors],
            "code": [f["code"] for f in factors],
            "enabled": [True] * len(factors),
            "created_at": [now] * len(factors),
            "updated_at": [now] * len(factors),
        })
        self.upsert("factor_metadata", seed_df, ["factor_id"])
        logger.info(f"已写入 {len(factors)} 条默认因子元数据")


# 单例实例（延迟初始化）
_db_client_instance: Optional["DolphinDBClient"] = None


def _get_db_client() -> "DolphinDBClient":
    """获取单例客户端实例"""
    global _db_client_instance
    if _db_client_instance is None:
        _db_client_instance = DolphinDBClient()
    return _db_client_instance


class _DBClientProxy:
    """Lazy proxy so existing `db_client.xxx` call sites continue to work."""
    def __getattr__(self, name):
        return getattr(_get_db_client(), name)


db_client = _DBClientProxy()


__all__ = ["DolphinDBClient", "db_client"]
