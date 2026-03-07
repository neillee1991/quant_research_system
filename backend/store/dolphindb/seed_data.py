"""
DolphinDB 数据初始化模块
负责默认配置数据的种子化（seed）
包含：同步任务配置、ETL任务配置、因子数据配置、因子元数据
"""
import json
import threading
from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd
import polars as pl

from app.core.logger import logger

if TYPE_CHECKING:
    from .connection import DolphinDBConnection
    from .query_builder import QueryBuilder


class SeedDataManager:
    """数据初始化管理器"""

    # factor_values 等非 sync 任务表的日期列配置
    _EXTRA_DATE_COLUMNS = {
        "factor_values": ["trade_date"],
    }

    def __init__(
        self,
        connection: "DolphinDBConnection",
        query_builder: "QueryBuilder",
    ) -> None:
        """
        初始化数据种子管理器

        Args:
            connection: DolphinDB 连接管理器
            query_builder: SQL 查询构建器
        """
        self.conn = connection
        self.query = query_builder

    def seed_sync_task_config(self) -> None:
        """
        如果 sync_task_config 表为空，则写入默认同步任务定义
        仅在首次启动时生效，后续可通过 API 增删改
        """
        try:
            count = self.query.query("SELECT count(*) as cnt FROM sync_task_config")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("sync_task_config 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.debug(f"检查 sync_task_config 数据失败（表可能刚创建）: {e}")
            # 表可能刚创建，继续 seed

        now = datetime.now()
        # 默认同步任务定义（基于 Tushare Pro API）
        tasks = self._get_default_sync_tasks()

        seed_df = pl.DataFrame({
            "task_id": [t["task_id"] for t in tasks],
            "api_name": [t["api_name"] for t in tasks],
            "description": [t["description"] for t in tasks],
            "params": [json.dumps(t["params"], ensure_ascii=False) for t in tasks],
            "sync_type": [t["sync_type"] for t in tasks],
            "date_field": [t.get("date_field", "") for t in tasks],
            "primary_keys": [json.dumps(t["primary_keys"]) for t in tasks],
            "table_name": [t["table_name"] for t in tasks],
            "schema": [json.dumps(t["schema"], ensure_ascii=False) for t in tasks],
            "api_limit": [t.get("api_limit", 5000) for t in tasks],
            "enabled": [True] * len(tasks),
            "created_at": [now] * len(tasks),
            "updated_at": [now] * len(tasks),
        })

        self._upsert_data("sync_task_config", seed_df, ["task_id"])
        logger.info(f"已写入 {len(tasks)} 条默认同步任务配置")

    def seed_etl_task_config(self) -> None:
        """
        如果 etl_task_config 表为空，则写入默认 ETL 任务定义
        仅在首次启动时生效，后续可通过 API 增删改
        """
        try:
            count = self.query.query("SELECT count(*) as cnt FROM etl_task_config")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("etl_task_config 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.warning(
                f"检查 etl_task_config 表时出错（可能表不存在，将继续 seed）: {e}"
            )

        now = datetime.now()
        tasks = self._get_default_etl_tasks()

        seed_df = pl.DataFrame({
            "task_id": [t["task_id"] for t in tasks],
            "task_name": [t["task_id"] for t in tasks],
            "description": [t["description"] for t in tasks],
            "source_tables": [json.dumps(t.get("source_tables", [])) for t in tasks],
            "target_table": [t["table_name"] for t in tasks],
            "transform_logic": [t["script"] for t in tasks],
            "schedule": [""] * len(tasks),
            "enabled": [True] * len(tasks),
            "created_at": [now] * len(tasks),
            "updated_at": [now] * len(tasks),
        })

        self._upsert_data("etl_task_config", seed_df, ["task_id"])
        logger.info(f"已写入 {len(tasks)} 条默认 ETL 任务配置")

    def seed_factor_data_config(self) -> None:
        """
        如果 factor_data_config 表为空，则写入默认字段映射
        仅在首次启动时生效，后续可通过 API 修改
        """
        try:
            count = self.query.query("SELECT count(*) as cnt FROM factor_data_config")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("factor_data_config 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.debug(f"检查 factor_data_config 数据失败（表可能刚创建）: {e}")

        now = datetime.now()
        mappings = [
            {
                "field_key": "adj_factor",
                "description": "复权因子",
                "table_name": "sync_adj_factor",
                "column_name": "adj_factor",
                "extra_config": "{}",
            },
            {
                "field_key": "list_date",
                "description": "股票上市日期",
                "table_name": "sync_stock_basic",
                "column_name": "list_date",
                "extra_config": "{}",
            },
            {
                "field_key": "is_st",
                "description": "是否ST（0=正常, 1=ST/*ST/退市风险警示）",
                "table_name": "sync_stock_st",
                "column_name": "ts_code",
                "extra_config": '{"mode":"exists_in_table","values":{"0":"正常","1":"ST/*ST/退市风险警示"}}',
            },
            {
                "field_key": "is_suspend",
                "description": "是否停牌（0=正常交易, 1=全天停牌, 2=盘中临时停牌）",
                "table_name": "sync_suspend_d",
                "column_name": "suspend_type",
                "extra_config": '{"mode":"exists_in_table","filter":{"suspend_type":"S"},"values":{"0":"正常交易","1":"全天停牌","2":"盘中临时停牌"}}',
            },
            {
                "field_key": "is_limit",
                "description": "涨跌停状态（0=未涨跌停, 1=涨停, -1=跌停）",
                "table_name": "sync_stk_limit",
                "column_name": "up_limit,down_limit",
                "extra_config": '{"mode":"compare_with_price","price_table":"sync_daily_data","price_column":"close","values":{"0":"未涨跌停","1":"涨停","-1":"跌停"}}',
            },
            {
                "field_key": "industry_l1",
                "description": "股票一级行业",
                "table_name": "sync_stock_basic",
                "column_name": "industry",
                "extra_config": "{}",
            },
            {
                "field_key": "industry_l2",
                "description": "股票二级行业",
                "table_name": "",
                "column_name": "",
                "extra_config": "{}",
            },
            {
                "field_key": "market_cap",
                "description": "股票总市值（万元）",
                "table_name": "sync_daily_basic",
                "column_name": "total_mv",
                "extra_config": "{}",
            },
        ]

        seed_df = pl.DataFrame({
            "field_key": [m["field_key"] for m in mappings],
            "description": [m["description"] for m in mappings],
            "table_name": [m["table_name"] for m in mappings],
            "column_name": [m["column_name"] for m in mappings],
            "extra_config": [m["extra_config"] for m in mappings],
            "updated_at": [now] * len(mappings),
        })

        self._upsert_data("factor_data_config", seed_df, ["field_key"])
        logger.info(f"已写入 {len(mappings)} 条默认因子数据配置")

    def seed_factor_metadata(self) -> None:
        """
        如果 factor_metadata 表为空，则写入默认种子因子定义
        仅在首次启动时生效，后续可通过 API 增删改
        """
        try:
            count = self.query.query("SELECT count(*) as cnt FROM factor_metadata")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("factor_metadata 已有数据，跳过 seed")
                return
        except Exception:
            pass

        now = datetime.now()
        factors = self._get_default_factors()

        seed_df = pl.DataFrame({
            "factor_id": [f["factor_id"] for f in factors],
            "version_number": [1] * len(factors),
            "is_current": [True] * len(factors),
            "description": [f["description"] for f in factors],
            "category": [f["category"] for f in factors],
            "compute_mode": [f["compute_mode"] for f in factors],
            "storage_target": [f["storage_target"] for f in factors],
            "depends_on": [f["depends_on"] for f in factors],
            "params": [f["params"] for f in factors],
            "code": [f["code"] for f in factors],
            "changed_by": ["system"] * len(factors),
            "change_reason": ["Initial seed"] * len(factors),
            "created_at": [now] * len(factors),
            "updated_at": [now] * len(factors),
        })

        self._upsert_data("factor_metadata", seed_df, ["factor_id", "version_number"])
        logger.info(f"已写入 {len(factors)} 条默认因子定义")

    def _upsert_data(
        self,
        table_name: str,
        df: pl.DataFrame,
        key_columns: list[str],
    ) -> None:
        """
        插入或更新数据（内部辅助方法）

        Args:
            table_name: 表名
            df: Polars DataFrame
            key_columns: 主键列
        """
        if df.is_empty():
            logger.warning(f"空 DataFrame，跳过写入: {table_name}")
            return

        db_path = self.conn.db_path

        try:
            # 转换日期列
            date_cols = self._EXTRA_DATE_COLUMNS.get(table_name, [])
            for col in date_cols:
                if col in df.columns and df[col].dtype == pl.Utf8:
                    df = df.with_columns(
                        pl.col(col).str.to_date("%Y%m%d", strict=False).alias(col)
                    )

            pdf = df.to_pandas()
            for col in date_cols:
                if col in pdf.columns and pd.api.types.is_datetime64_any_dtype(pdf[col]):
                    pdf[col] = pdf[col].dt.date

            with self.conn.lock:
                self.conn._ensure_connected()

                # 上传临时变量
                tmp_var = f"tmp_{table_name}_{threading.current_thread().ident}"
                self.conn.session.upload({tmp_var: pdf})

                # 先删除旧记录，再插入（模拟 upsert）
                if key_columns:
                    handle = f"{table_name}_handle"
                    delete_conds = [f'{kc} in {tmp_var}.{kc}' for kc in key_columns]
                    cond_str = " and ".join(delete_conds)
                    self.conn.session.run(
                        f"{handle} = loadTable('{db_path}', '{table_name}')"
                    )
                    self.conn.session.run(f"delete from {handle} where {cond_str}")
                    self.conn.session.run(
                        f"tableInsert({handle}, {tmp_var});"
                        f"undef('{tmp_var}')"
                    )
                else:
                    # 无主键，直接插入
                    self.conn.session.run(
                        f"{table_name}_handle = loadTable('{db_path}', '{table_name}');"
                        f"tableInsert({table_name}_handle, {tmp_var});"
                        f"undef('{tmp_var}')"
                    )

            logger.info(f"写入 {len(df)} 行到 {table_name}")
        except Exception as e:
            logger.error(f"写入失败 [{table_name}]: {e}")
            raise

    def _get_default_sync_tasks(self) -> list[dict]:
        """获取默认同步任务配置"""
        return [
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
                "description": "交易日历（SSE/SZSE 开市日期、前一交易日）",
                "sync_type": "full",
                "date_field": "cal_date",
                "table_name": "sync_trade_cal",
                "params": {
                    "exchange": "",
                    "start_date": "",
                    "end_date": "",
                    "is_open": "",
                },
                "primary_keys": ["exchange", "cal_date"],
                "api_limit": 5000,
                "schema": {
                    "exchange": {"type": "SYMBOL"},
                    "cal_date": {"type": "STRING"},
                    "is_open": {"type": "INT"},
                    "pretrade_date": {"type": "STRING"},
                },
            },
            # ==================== 增量同步 ====================
            {
                "task_id": "sync_daily",
                "api_name": "daily",
                "description": "A股日线行情（OHLCV、涨跌幅、成交量成交额）",
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
                    "trade_date": {"type": "DATE"},
                    "ts_code": {"type": "SYMBOL"},
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
                "description": "复权因子（用于计算前/后复权价格）",
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
        ]

    def _get_default_etl_tasks(self) -> list[dict]:
        """获取默认 ETL 任务配置"""
        db_meta = self.conn.db_path

        script_index_member = (
            f't_sw = select l1_code, l1_name, l2_code, l2_name, l3_code, l3_name, '
            f'ts_code, in_date, out_date, "sw" as source '
            f'from loadTable("{db_meta}", "sync_sw_index_member_Y"); '
            f't_ci = select l1_code, l1_name, l2_code, l2_name, l3_code, l3_name, '
            f'ts_code, in_date, out_date, "ci" as source '
            f'from loadTable("{db_meta}", "sync_ci_index_member_Y"); '
            f'unionAll([t_sw, t_ci], false)'
        )

        return [
            {
                "task_id": "etl_index_member",
                "description": "合并申万+中信行业成员表（当前有效分类）",
                "script": script_index_member,
                "sync_type": "full",
                "date_field": "",
                "primary_keys": ["ts_code", "source", "l3_code"],
                "table_name": "etl_index_member",
                "source_tables": ["sync_sw_index_member_Y", "sync_ci_index_member_Y"],
            },
        ]

    def _get_default_factors(self) -> list[dict]:
        """获取默认因子定义"""
        return [
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
                    "            .over('ts_code')\n"
                    "            .alias('factor_value')\n"
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
                    "            .over('ts_code')\n"
                    "            .alias('factor_value')\n"
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
        ]
