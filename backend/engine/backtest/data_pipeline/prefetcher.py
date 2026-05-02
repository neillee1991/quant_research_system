"""
数据预取器 - 负责数据加载和缓存
"""
from typing import Dict, Any, List
import polars as pl
from app.core.logger import logger
from infrastructure.database.dolphindb_client import db_client


class DataPrefetcher:
    """数据预取器 - 负责数据加载和缓存"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        # 本地字典缓存，未来需要迁移到 Redis
        self._cache = {}

    async def prefetch_data(
        self,
        start_date: str,
        end_date: str,
        factors: List[str],
        stocks: List[str] = None
    ) -> pl.DataFrame:
        """预取数据"""
        # 【重要】回测引擎"悲观且快速失败"，直接检查因子是否已计算
        self._check_factors_are_computed(factors, start_date, end_date)

        cache_key = self._get_cache_key(start_date, end_date, factors, stocks)

        if cache_key in self._cache:
            logger.info("使用缓存的宽表数据")
            return self._cache[cache_key]

        logger.info("从 DolphinDB 加载数据")
        from backend.engine.backtest.data_pipeline.query_builder import QueryBuilder

        builder = QueryBuilder(self.config)
        query, params = builder.build_wide_table_query(start_date, end_date, factors, stocks)

        # 执行查询
        df = await self._execute_query((query, params))

        # 数据验证
        self._validate_data(df, factors)

        self._cache[cache_key] = df
        logger.info(f"数据加载并缓存: {len(df)} 行")

        return df

    def _check_factors_are_computed(self, factors: List[str], start_date: str, end_date: str):
        """检查因子是否已计算，未计算则直接抛出异常"""
        from app.services.factor_service import factor_service

        missing_factors = []
        for factor_id in factors:
            if not factor_service.is_factor_computed(factor_id, start_date, end_date):
                missing_factors.append(factor_id)

        if missing_factors:
            raise ValueError(f"回测失败：因子 [{', '.join(missing_factors)}] 未计算，请先前往因子中心计算后再回测")

    async def _execute_query(self, query: Tuple[str, Dict[str, Any]]) -> pl.DataFrame:
        """执行 DolphinDB 查询"""
        sql, params = query
        return db_client.query(sql, **params)

    def _validate_data(self, df: pl.DataFrame, factors: List[str]):
        """验证数据"""
        required_fields = ["ts_code", "trade_date", "open", "high", "low", "close", "volume"]
        for field in required_fields:
            if field not in df.columns:
                raise ValueError(f"缺少必要字段: {field}")

        # 验证因子列
        for factor_id in factors:
            if factor_id not in df.columns:
                raise ValueError(f"缺少因子列: {factor_id}")

        # 检查是否有重复数据
        duplicates = df.groupby(["ts_code", "trade_date"]).count().filter(pl.col("count") > 1)
        if len(duplicates) > 0:
            raise ValueError(f"发现重复数据")

        logger.info("数据验证通过")

    def _get_cache_key(self, start_date: str, end_date: str, factors: List[str], stocks: List[str]) -> str:
        """生成缓存键"""
        stock_str = "_".join(stocks) if stocks else "all"
        factors_str = "_".join(sorted(factors))
        return f"{start_date}_{end_date}_{factors_str}_{stock_str}"

    def clear_cache(self):
        """清除缓存"""
        self._cache.clear()
