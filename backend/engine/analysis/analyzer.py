"""
因子分析引擎
支持传统 Polars 实现和 Alphalens 框架两种分析方式
"""
import math
import polars as pl
import json
import numpy as np
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from app.core.logger import logger
from engine.analysis.alphalens_adapter import AlphalensAdapter
from engine.production.data_config import DataConfigLoader


def _sanitize_for_json(obj):
    """递归清理 NaN/Inf 值，确保 JSON 序列化安全"""
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]
    if isinstance(obj, (float, np.floating)):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return float(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    return obj


class FactorAnalyzer:
    """因子分析器 - 支持传统分析和 Alphalens 框架"""

    def __init__(self, db_client):
        self.db = db_client
        self.alphalens_adapter = AlphalensAdapter(db_client)
        self.data_config_loader = DataConfigLoader(db_client)

    def analyze(
        self,
        factor_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        periods: List[int] = None,
        quantiles: int = 5,
        index_pool: Optional[str] = None,
        groupby_field: Optional[str] = None,
        next_day_entry: bool = True,
        entry_price: str = "open",
        neutralize: bool = False,
        neutralize_controls: Optional[List[str]] = None,
        industry_level: str = "industry_l1",
    ) -> Optional[Dict[str, Any]]:
        """执行因子分析（Alphalens）"""
        if periods is None:
            periods = [1, 5, 10, 20]
        logger.info(f"Analyzing factor: {factor_id}, index_pool={index_pool}, groupby_field={groupby_field}")
        return self._analyze_with_alphalens(
            factor_id, start_date, end_date, periods, quantiles, index_pool, groupby_field,
            next_day_entry=next_day_entry, entry_price=entry_price,
            neutralize=neutralize, neutralize_controls=neutralize_controls,
            industry_level=industry_level,
        )

    # ==================== 数据加载 ====================

    def _load_factor_data(self, factor_id: str, start_date: Optional[str],
                          end_date: Optional[str]) -> Optional[pl.DataFrame]:
        """加载因子数据（支持分批加载大数据集）"""
        conditions = ["factor_id = %s"]
        params = [factor_id]
        if start_date:
            conditions.append(f"trade_date >= temporalParse('{start_date}','yyyyMMdd')")
        if end_date:
            conditions.append(f"trade_date <= temporalParse('{end_date}','yyyyMMdd')")

        where = " AND ".join(conditions)
        sql = f"SELECT ts_code, trade_date, factor_value FROM factor_values WHERE {where} ORDER BY trade_date, ts_code"

        try:
            df = self.db.query(sql, tuple(params))
            return df if not df.is_empty() else None
        except Exception as e:
            logger.error(f"Failed to load factor data: {e}")
            return None

    def _load_price_data(self, factor_df: pl.DataFrame, start_date: Optional[str],
                         end_date: Optional[str], max_period: int) -> Optional[pl.DataFrame]:
        """加载价格数据（支持分批加载），需要额外加载 max_period 天用于计算远期收益"""
        min_date = factor_df["trade_date"].min()
        max_date = factor_df["trade_date"].max()

        # trade_date 可能是 datetime 对象，统一转为 YYYYMMDD 字符串
        if hasattr(min_date, 'strftime'):
            min_date = min_date.strftime("%Y%m%d")
        if hasattr(max_date, 'strftime'):
            max_date = max_date.strftime("%Y%m%d")

        extra_days = max_period * 2 + 1
        from datetime import timedelta
        end_dt = datetime.strptime(max_date, "%Y%m%d") + timedelta(days=extra_days)
        load_end = end_dt.strftime("%Y%m%d")

        sql = f"""
            SELECT ts_code, trade_date, open, high, low, close
            FROM sync_daily_data
            WHERE trade_date >= temporalParse('{min_date}','yyyyMMdd')
              AND trade_date <= temporalParse('{load_end}','yyyyMMdd')
            ORDER BY ts_code, trade_date
        """
        try:
            df = self.db.query(sql, ())
            return df if not df.is_empty() else None
        except Exception as e:
            logger.error(f"Failed to load price data: {e}")
            return None

    # ==================== Alphalens 分析方法 ====================

    def _analyze_with_alphalens(
        self,
        factor_id: str,
        start_date: Optional[str],
        end_date: Optional[str],
        periods: List[int],
        quantiles: int,
        index_pool: Optional[str],
        groupby_field: Optional[str],
        next_day_entry: bool = True,
        entry_price: str = "open",
        neutralize: bool = False,
        neutralize_controls: Optional[List[str]] = None,
        industry_level: str = "industry_l1",
    ) -> Optional[Dict[str, Any]]:
        """使用 Alphalens 框架进行因子分析"""
        started_at = datetime.now()
        logger.info(f"Starting Alphalens analysis for {factor_id}")

        pipeline_stats = []

        def _record_step(step: str, count: int, prev_count: int) -> int:
            """记录一个流水线步骤的数据量变化"""
            dropped = prev_count - count
            pipeline_stats.append({
                "step": step,
                "total_rows": count,
                "dropped": dropped,
                "drop_pct": round(dropped / prev_count * 100, 2) if prev_count > 0 else 0.0,
            })
            return count

        try:
            # 0. 尝试从缓存读取数据
            from engine.analysis.data_cache import AnalysisDataCache
            cache = AnalysisDataCache.get_instance()
            cache_key = AnalysisDataCache.make_key(factor_id, start_date, end_date, index_pool)
            cached = cache.get(cache_key)

            # 1. 加载因子数据
            if cached:
                factor_df = cached["factor_df"]
                logger.info(f"Using cached factor_df for {cache_key}")
            else:
                factor_df = self._load_factor_data(factor_id, start_date, end_date)
            if factor_df is None or factor_df.is_empty():
                logger.warning(f"No factor data for {factor_id}")
                return None

            count = len(factor_df)
            _record_step("raw_factor", count, count)  # 第一步 dropped=0

            # 2. 应用股票池过滤（如果指定）
            prev = count
            if index_pool:
                constituents = self._get_index_constituents(index_pool, start_date, end_date)
                if constituents is not None and not constituents.is_empty():
                    logger.info(f"Filtering by index pool {index_pool}: {len(constituents)} records")
                    factor_df = factor_df.join(
                        constituents.select(["ts_code", "trade_date"]),
                        on=["ts_code", "trade_date"],
                        how="inner"
                    )
                    logger.info(f"After filtering: {len(factor_df)} rows")
                else:
                    logger.warning(f"Index pool {index_pool} has no data, using all stocks")
            count = len(factor_df)
            _record_step("index_pool_filter", count, prev)

            # 2b. 因子中性化（可选）
            if neutralize:
                controls = neutralize_controls or ["market", "industry", "size"]
                try:
                    from engine.analysis.neutralizer import Neutralizer
                    industry_df = None
                    size_df = None
                    ts_codes = factor_df["ts_code"].unique().to_list()
                    date_min = factor_df["trade_date"].min()
                    date_max = factor_df["trade_date"].max()

                    if "industry" in controls:
                        try:
                            # 使用用户选择的行业级别（industry_l1 或 industry_l2）
                            industry_df = self.data_config_loader.load_field_data(
                                industry_level, ts_codes, date_min, date_max
                            )
                            if industry_df is not None and not industry_df.is_empty():
                                value_col = [c for c in industry_df.columns if c not in ["ts_code", "trade_date"]]
                                if value_col:
                                    industry_df = industry_df.rename({value_col[0]: "industry"})
                        except Exception as e:
                            logger.warning(f"Failed to load industry data for neutralization: {e}")

                    if "size" in controls:
                        try:
                            # market_cap 是 seed_data 中配置的 field_key
                            size_df = self.data_config_loader.load_field_data(
                                "market_cap", ts_codes, date_min, date_max
                            )
                            if size_df is not None and not size_df.is_empty():
                                value_col = [c for c in size_df.columns if c not in ["ts_code", "trade_date"]]
                                if value_col:
                                    size_df = size_df.rename({value_col[0]: "size_value"})
                        except Exception as e:
                            logger.warning(f"Failed to load size data for neutralization: {e}")

                    factor_df = Neutralizer.neutralize(
                        factor_df=factor_df,
                        controls=controls,
                        industry_df=industry_df,
                        size_df=size_df,
                    )
                    logger.info(f"Factor neutralized with controls={controls}")
                except Exception as e:
                    logger.warning(f"Neutralization failed (non-fatal): {e}")

            # 3. 加载价格数据
            if cached:
                price_df = cached["price_df"]
                logger.info(f"Using cached price_df for {cache_key}")
            else:
                price_df = self._load_price_data(factor_df, start_date, end_date, max(periods))
                if price_df is not None and not price_df.is_empty():
                    cache.set(cache_key, {"factor_df": factor_df, "price_df": price_df})
            if price_df is None or price_df.is_empty():
                logger.warning(f"No price data for analysis")
                return None

            # 4. 加载分组数据（如果指定）
            groupby_df = None
            if groupby_field:
                groupby_df = self._load_groupby_data(
                    groupby_field,
                    factor_df["ts_code"].unique().to_list(),
                    factor_df["trade_date"].min(),
                    factor_df["trade_date"].max()
                )
                if groupby_df is None:
                    logger.warning(f"Groupby field {groupby_field} not configured or has no data")

            # 5. 准备 Alphalens 数据格式
            factor_data = self.alphalens_adapter.prepare_factor_data(
                factor_df=factor_df,
                price_df=price_df,
                periods=periods,
                quantiles=quantiles,
                groupby_df=groupby_df,
                next_day_entry=next_day_entry,
                entry_price=entry_price,
            )

            prev = count
            count = len(factor_data)
            _record_step("forward_return_and_clean", count, prev)

            # 6. 运行完整分析
            results = self.alphalens_adapter.run_full_analysis(
                factor_data=factor_data,
                periods=periods,
                quantiles=quantiles
            )

            # 6b. 行业分析（始终计算，使用 industry_level 字段）
            try:
                _ts_codes = factor_df["ts_code"].unique().to_list()
                _date_min = factor_df["trade_date"].min()
                _date_max = factor_df["trade_date"].max()
                industry_df_for_analysis = self.data_config_loader.load_field_data(
                    industry_level, _ts_codes, _date_min, _date_max
                )
                if industry_df_for_analysis is not None and not industry_df_for_analysis.is_empty():
                    industry_results = self.alphalens_adapter.compute_industry_analysis(
                        factor_data=factor_data,
                        industry_groupby_df=industry_df_for_analysis,
                        periods=periods,
                    )
                    if industry_results:
                        results['ic_by_industry'] = industry_results.get('ic_by_industry', {})
                        results['returns_by_industry'] = industry_results.get('returns_by_industry', {})
                        # 同时写入 charts_data 以便持久化
                        charts_data = results.get('charts_data', {})
                        charts_data['ic_by_industry'] = results['ic_by_industry']
                        charts_data['returns_by_industry'] = results['returns_by_industry']
                        results['charts_data'] = charts_data
                        logger.info(f"Industry analysis done: {len(results['ic_by_industry'])} industries")
            except Exception as e:
                logger.warning(f"Industry analysis failed (non-fatal): {e}")

            results["diagnostics"] = {
                "pipeline_stats": pipeline_stats,
                "final_rows": len(factor_data),
                "final_dates": factor_data.index.get_level_values("date").nunique(),
                "avg_daily_coverage": round(
                    len(factor_data) / max(factor_data.index.get_level_values("date").nunique(), 1), 1
                ),
            }

            # 运行因子诊断
            try:
                from engine.analysis.diagnostics import FactorDiagnostics
                diag = FactorDiagnostics.diagnose(
                    factor_df=factor_df,
                    factor_data=factor_data,
                    ic_series=None,  # TODO: P1 完成后传入 ic_series
                )
                results["diagnostics"]["warnings"] = diag["warnings"]
                results["diagnostics"]["distribution"] = diag["distribution"]
                results["diagnostics"]["extreme_values"] = diag["extreme_values"]
            except Exception as e:
                logger.warning(f"Diagnostics failed (non-fatal): {e}")

            # 7. 保存到 factor_analysis_extended 表
            actual_start = factor_df["trade_date"].min()
            actual_end = factor_df["trade_date"].max()
            self._save_alphalens_analysis(
                factor_id=factor_id,
                results=results,
                start_date=actual_start,
                end_date=actual_end,
                config={
                    "periods": periods,
                    "quantiles": quantiles,
                    "index_pool": index_pool,
                    "groupby_field": groupby_field,
                }
            )

            elapsed = (datetime.now() - started_at).total_seconds()
            logger.info(f"Alphalens analysis for {factor_id} completed in {elapsed:.1f}s")
            return _sanitize_for_json(results)

        except Exception as e:
            logger.error(f"Alphalens analysis failed for {factor_id}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _get_index_constituents(
        self,
        index_code: str,
        start_date: Optional[str],
        end_date: Optional[str]
    ) -> Optional[pl.DataFrame]:
        """获取指数成分股数据

        Args:
            index_code: 指数代码（如 '000300.SH'）
            start_date: 起始日期
            end_date: 结束日期

        Returns:
            DataFrame with columns: ts_code, trade_date, index_code, weight
        """
        try:
            conditions = ["index_code = %s"]
            params = [index_code]

            if start_date:
                conditions.append("trade_date >= %s")
                params.append(start_date)
            if end_date:
                conditions.append("trade_date <= %s")
                params.append(end_date)

            where = " AND ".join(conditions)
            sql = f"""
                SELECT ts_code, trade_date, index_code, weight
                FROM loadTable("dfs://quant", "index_constituents")
                WHERE {where}
                ORDER BY trade_date, ts_code
            """

            df = self.db.query(sql, tuple(params))
            if df.is_empty():
                logger.warning(f"No constituents found for index {index_code}")
                return None

            logger.info(f"Loaded {len(df)} constituent records for {index_code}")
            return df

        except Exception as e:
            logger.error(f"Failed to load index constituents: {e}")
            return None

    def _load_groupby_data(
        self,
        field_key: str,
        ts_codes: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pl.DataFrame]:
        """加载分组字段数据（行业、市值等）

        Args:
            field_key: 字段键（如 'industry', 'market_cap'）
            ts_codes: 股票代码列表
            start_date: 起始日期
            end_date: 结束日期

        Returns:
            DataFrame with columns: ts_code, trade_date, {field_key}_value
        """
        return self.data_config_loader.load_field_data(
            field_key=field_key,
            ts_codes=ts_codes,
            start_date=start_date,
            end_date=end_date
        )

    def _save_alphalens_analysis(
        self,
        factor_id: str,
        results: Dict[str, Any],
        start_date: str,
        end_date: str,
        config: Dict[str, Any]
    ):
        """保存 Alphalens 分析结果到 factor_analysis_extended 表"""
        try:
            import polars as pl
            from datetime import datetime as dt
            import math
            import numpy as np

            # 清理 NaN/Inf 值
            def clean_value(v):
                if isinstance(v, (float, np.floating)):
                    if math.isnan(v) or math.isinf(v):
                        return None
                elif isinstance(v, (np.integer, np.int64, np.int32)):
                    return int(v)
                return v

            def clean_dict(d):
                if isinstance(d, dict):
                    return {k: clean_dict(v) for k, v in d.items()}
                elif isinstance(d, (list, tuple)):
                    return [clean_dict(item) for item in d]
                else:
                    return clean_value(d)

            # 清理结果
            results = clean_dict(results)

            # 生成唯一 ID（使用时间戳）
            analysis_id = int(datetime.now().timestamp() * 1000)

            # 转换日期格式为 DolphinDB 兼容格式
            # analysis_date: TIMESTAMP (datetime)
            # start_date, end_date: DATE (YYYYMMDD -> date object)
            analysis_datetime = datetime.now()

            # 将 YYYYMMDD 字符串或 datetime 转换为 date 对象
            def _to_date(d):
                if d is None:
                    return None
                if hasattr(d, 'date'):
                    return d.date()
                return dt.strptime(str(d), "%Y%m%d").date()
            start_date_obj = _to_date(start_date)
            end_date_obj = _to_date(end_date)

            # 构建记录
            record = {
                "id": analysis_id,
                "factor_id": factor_id,
                "analysis_date": analysis_datetime,
                "start_date": start_date_obj,
                "end_date": end_date_obj,
                "config": json.dumps(config),
                "ic_summary": json.dumps(results.get("ic_summary", {})),
                "ic_by_period": json.dumps(results.get("ic_by_period", [])),
                "ic_ts": json.dumps(results.get("ic_ts", [])),
                "quantile_returns": json.dumps(results.get("quantile_returns", [])),
                "cumulative_returns": json.dumps(results.get("cumulative_returns", [])),
                "ic_by_group": json.dumps(results.get("ic_by_group", {})),
                "returns_by_group": json.dumps(results.get("returns_by_group", {})),
                "turnover": json.dumps(results.get("turnover", {})),
                "decay_analysis": json.dumps(results.get("decay_analysis", {})),
                "charts_data": json.dumps(results.get("charts_data", {})),
                "task_status": "completed",
                "task_id": None,
                "error_message": None
            }

            # 转换为 DataFrame
            df = pl.DataFrame([record])

            # 使用 upsert 保存
            self.db.upsert("factor_analysis_extended", df, key_columns=["id"])

            logger.info(f"Saved Alphalens analysis result for {factor_id} (id={analysis_id})")

        except Exception as e:
            logger.error(f"Failed to save Alphalens analysis: {e}")
            import traceback
            traceback.print_exc()

    # ==================== 查询接口 ====================

    def get_task_status(self, task_id: int) -> Optional[Dict]:
        """查询分析任务状态"""
        try:
            df = self.db.query("""
                SELECT id, factor_id, task_status, error_message, analysis_date
                FROM factor_analysis_extended
                WHERE id = %s
            """, (task_id,))
            if df.is_empty():
                return None
            return df.to_dicts()[0]
        except Exception as e:
            logger.error(f"Failed to get task status: {e}")
            return None


def run_prefect_flow(factor_id: str, params: dict) -> None:
    """
    Prefect flow hook stub.
    用户可在此配置 Prefect flow 来调度因子分析任务。

    示例：
        from prefect import flow
        @flow
        def factor_analysis_flow(factor_id: str, params: dict):
            analyzer = FactorAnalyzer()
            return analyzer.analyze(factor_id, **params)
    """
    raise NotImplementedError(
        "Prefect flow not configured. "
        "Implement this function to integrate with Prefect scheduling."
    )
