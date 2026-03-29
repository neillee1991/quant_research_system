"""
因子分析引擎
支持传统 Polars 实现和 Alphalens 框架两种分析方式
"""
import math
import polars as pl
import json
import gzip
import base64
import numpy as np
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from app.core.logger import logger
from engine.analysis.alphalens_adapter import AlphalensAdapter
from engine.factor.data_config import DataConfigLoader


def _compress_json(data: Any) -> str:
    """压缩 JSON 数据（gzip + base64）以突破 DolphinDB 256KB 字符串限制"""
    json_str = json.dumps(data)
    compressed = gzip.compress(json_str.encode('utf-8'))
    return base64.b64encode(compressed).decode('ascii')


def _decompress_json(compressed_str: str) -> Any:
    """解压缩 JSON 数据"""
    try:
        compressed = base64.b64decode(compressed_str.encode('ascii'))
        json_str = gzip.decompress(compressed).decode('utf-8')
        return json.loads(json_str)
    except Exception:
        # 兼容未压缩的旧数据
        return json.loads(compressed_str)


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
        winsorize: bool = False,
        winsorize_lower: float = 0.01,
        winsorize_upper: float = 0.99,
        task_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """执行因子分析（Alphalens）"""
        if periods is None:
            periods = [1, 5, 10, 20]
        logger.info(f"Analyzing factor: {factor_id}, index_pool={index_pool}, groupby_field={groupby_field}, task_id={task_id}, winsorize={winsorize}")
        return self._analyze_with_alphalens(
            factor_id, start_date, end_date, periods, quantiles, index_pool, groupby_field,
            next_day_entry=next_day_entry, entry_price=entry_price,
            neutralize=neutralize, neutralize_controls=neutralize_controls,
            industry_level=industry_level,
            winsorize=winsorize, winsorize_lower=winsorize_lower, winsorize_upper=winsorize_upper,
            task_id=task_id,
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
        winsorize: bool = False,
        winsorize_lower: float = 0.01,
        winsorize_upper: float = 0.99,
        task_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """使用 Alphalens 框架进行因子分析"""
        started_at = datetime.now()
        logger.info(f"Starting Alphalens analysis for {factor_id}")

        pipeline_stats = []
        analysis_warnings = []  # 收集分析过程中的警告，传递到前端

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

        def _add_warning(level: str, type_: str, message: str, action: Optional[str] = None):
            """添加警告到结果中"""
            analysis_warnings.append({
                "level": level,
                "type": type_,
                "message": message,
                "action": action,
            })

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

            # 1b. 极端值处理（Winsorize，可选）
            if winsorize:
                prev = count
                try:
                    # 计算分位数边界
                    lower_bound = factor_df["factor_value"].quantile(winsorize_lower)
                    upper_bound = factor_df["factor_value"].quantile(winsorize_upper)

                    # 应用 winsorize
                    factor_df = factor_df.with_columns(
                        pl.when(pl.col("factor_value") < lower_bound)
                        .then(lower_bound)
                        .when(pl.col("factor_value") > upper_bound)
                        .then(upper_bound)
                        .otherwise(pl.col("factor_value"))
                        .alias("factor_value")
                    )
                    _add_warning(
                        "INFO",
                        "winsorize_applied",
                        f"已应用Winsorize处理: [{winsorize_lower*100:.0f}%, {winsorize_upper*100:.0f}%], 边界=[{lower_bound:.4f}, {upper_bound:.4f}]",
                    )
                    logger.info(f"Winsorize applied: bounds [{lower_bound:.4f}, {upper_bound:.4f}]")
                except Exception as e:
                    msg = f"Winsorize处理失败: {e}"
                    logger.warning(msg)
                    _add_warning("WARNING", "winsorize_failed", msg)

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
                            msg = f"加载行业数据失败，将跳过行业中性化: {e}"
                            logger.warning(msg)
                            _add_warning("WARNING", "neutralize_industry_failed", msg)

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
                            msg = f"加载市值数据失败，将跳过市值中性化: {e}"
                            logger.warning(msg)
                            _add_warning("WARNING", "neutralize_size_failed", msg)

                    factor_df = Neutralizer.neutralize(
                        factor_df=factor_df,
                        controls=controls,
                        industry_df=industry_df,
                        size_df=size_df,
                    )
                    logger.info(f"Factor neutralized with controls={controls}")
                except Exception as e:
                    msg = f"中性化失败，将使用原始因子: {e}"
                    logger.warning(msg)
                    _add_warning("WARNING", "neutralize_failed", msg)

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
                else:
                    msg = "无行业数据，跳过行业分析"
                    logger.warning(msg)
                    _add_warning("INFO", "industry_no_data", msg)
            except Exception as e:
                msg = f"行业分析失败: {e}"
                logger.warning(msg)
                _add_warning("WARNING", "industry_analysis_failed", msg)

            results["diagnostics"] = {
                "pipeline_stats": pipeline_stats,
                "final_rows": len(factor_data),
                "final_dates": factor_data.index.get_level_values("date").nunique(),
                "avg_daily_coverage": round(
                    len(factor_data) / max(factor_data.index.get_level_values("date").nunique(), 1), 1
                ),
                "warnings": analysis_warnings,  # 添加分析过程中的警告
            }

            # 运行因子诊断
            try:
                from engine.analysis.diagnostics import FactorDiagnostics
                diag = FactorDiagnostics.diagnose(
                    factor_df=factor_df,
                    factor_data=factor_data,
                    ic_series=None,  # TODO: P1 完成后传入 ic_series
                )
                # 合并诊断警告
                results["diagnostics"]["warnings"].extend(diag.get("warnings", []))
                results["diagnostics"]["distribution"] = diag.get("distribution")
                results["diagnostics"]["extreme_values"] = diag.get("extreme_values")
            except Exception as e:
                logger.warning(f"Diagnostics failed (non-fatal): {e}")

            # 7. 返回结果，不在这里保存（由调用者统一保存）
            actual_start = factor_df["trade_date"].min()
            actual_end = factor_df["trade_date"].max()
            results["_actual_start"] = actual_start
            results["_actual_end"] = actual_end
            results["_config"] = {
                "periods": periods,
                "quantiles": quantiles,
                "index_pool": index_pool,
                "groupby_field": groupby_field,
                "entry_price": entry_price,
                "neutralize": neutralize,
                "neutralize_controls": neutralize_controls,
                "industry_level": industry_level,
            }

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
        config: Dict[str, Any],
        task_id: Optional[int] = None,
    ):
        """保存 Alphalens 分析结果

        如果提供了 task_id，则更新该记录；否则创建新记录。

        存储策略：
        - 数据库：任务元数据 + 摘要指标（用于列表展示和跨因子比较）
        - 文件：完整分析报告（仅在查看单次分析详情时加载）
        """
        import polars as pl
        from datetime import datetime as dt
        import math
        import numpy as np
        import json
        from pathlib import Path
        from app.core.config import settings

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

        results = clean_dict(results)

        # 如果提供了 task_id，使用它；否则生成新的
        if task_id is not None:
            analysis_id = task_id
            logger.info(f"Updating existing analysis record: id={analysis_id}")
        else:
            analysis_id = int(datetime.now().timestamp() * 1000)
            logger.info(f"Creating new analysis record: id={analysis_id}")

        analysis_datetime = datetime.now()

        def _to_date(d):
            if d is None:
                return None
            if hasattr(d, 'date'):
                return d.date()
            return dt.strptime(str(d), "%Y%m%d").date()

        # 1. 完整分析报告存文件（详情页使用）
        report_fields = [
            'ic_ts', 'quantile_returns', 'cumulative_returns',
            'returns_by_group', 'turnover', 'charts_data',
            'ic_by_group', 'spread_ts', 'alpha_beta',
            'factor_cumulative_returns', 'ic_by_month', 'event_study',
            'ic_by_industry', 'returns_by_industry', 'diagnostics',
            'decay_analysis',
        ]
        report = {k: results.get(k) for k in report_fields if results.get(k) is not None}

        factor_analysis_dir = Path(settings.analysis_dir) / factor_id
        factor_analysis_dir.mkdir(parents=True, exist_ok=True)
        report_path = factor_analysis_dir / f"{analysis_id}.json"
        report_path.write_text(json.dumps(report), encoding='utf-8')
        logger.info(f"Analysis report saved to {report_path}")

        # 2. 数据库只存摘要指标 + 文件路径（列表页和比较使用）
        record = {
            "id": analysis_id,
            "factor_id": factor_id,
            "analysis_date": analysis_datetime,
            "start_date": _to_date(start_date),
            "end_date": _to_date(end_date),
            "config": json.dumps(config),
            # 摘要指标：用于列表展示和跨因子比较
            "ic_summary": json.dumps(results.get("ic_summary", {})),
            "ic_by_period": json.dumps(results.get("ic_by_period", [])),
            "decay_analysis": None,
            # 详情报告文件路径
            "report_path": str(report_path),
            "task_status": "completed",
            "task_id": str(task_id) if task_id else None,
            "error_message": None
        }

        df = pl.DataFrame([record])
        self.db.upsert("factor_analysis_extended", df, key_columns=["id"])
        logger.info(f"Saved analysis metadata for {factor_id} (id={analysis_id})")

    # ==================== 查询接口 ====================

    def get_task_status(self, task_id: str) -> Optional[Dict]:
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
