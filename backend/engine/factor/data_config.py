"""因子数据配置加载器 - 从 factor_data_config 表读取字段映射

架构说明：
1. depends_on 配置的表会自动加载所有字段，无需在 factor_data_config 中配置
2. factor_data_config 只用于配置需要特殊处理的字段：
   - 需要计算的字段（如 is_st、is_limit 从特定表加载）
   - 需要跨表关联的字段（如 industry 从行业分类表）
   - 需要特殊处理的字段（如 market_cap 的单位转换）
"""
import json
import time
from typing import Optional, Dict, Any, List
import polars as pl

from app.core.logger import logger

_CACHE_TTL_SECONDS = 300  # 5 分钟后自动失效


# 内置默认值（只包含需要特殊处理的字段）
_DEFAULTS: Dict[str, Dict[str, Any]] = {
    # 股票状态字段：需要特殊计算或跨表关联
    "list_date": {"table_name": "sync_stock_basic", "column_name": "list_date", "extra_config": {}},
    "is_st": {"table_name": "", "column_name": "is_st", "extra_config": {}},
    "is_limit": {"table_name": "", "column_name": "is_limit", "extra_config": {}},
    "industry_l1": {"table_name": "", "column_name": "", "extra_config": {}},
    "industry_l2": {"table_name": "", "column_name": "", "extra_config": {}},
}


class DataConfigLoader:
    """从 DolphinDB factor_data_config 表加载字段映射配置，带内存缓存"""

    def __init__(self, db_client):
        self.db = db_client
        self._cache: Optional[Dict[str, Dict[str, Any]]] = None
        self._cache_ts: float = 0.0

    def load(self) -> Dict[str, Dict[str, Any]]:
        """从 DB 加载配置，带 TTL 缓存"""
        if self._cache is not None and (time.time() - self._cache_ts) < _CACHE_TTL_SECONDS:
            return self._cache
        try:
            df = self.db.query("SELECT * FROM factor_data_config")
            config = {}
            if not df.is_empty():
                for row in df.to_dicts():
                    fk = row["field_key"]
                    extra = row.get("extra_config", "{}") or "{}"
                    try:
                        extra_parsed = json.loads(extra)
                    except Exception:
                        extra_parsed = {}
                    config[fk] = {
                        "table_name": row.get("table_name", "") or "",
                        "column_name": row.get("column_name", "") or "",
                        "extra_config": extra_parsed,
                    }
            self._cache = config
            self._cache_ts = time.time()
            return config
        except Exception as e:
            logger.warning(f"加载 factor_data_config 失败 ({e})，使用内置默认值")
            self._cache = dict(_DEFAULTS)
            self._cache_ts = time.time()
            return self._cache

    def get(self, field_key: str) -> Dict[str, Any]:
        """获取单个字段映射"""
        config = self.load()
        return config.get(field_key, _DEFAULTS.get(field_key, {"table_name": "", "column_name": "", "extra_config": {}}))

    def invalidate_cache(self) -> None:
        """清除缓存（配置更新时调用）"""
        self._cache = None

    def is_field_configured(self, field_key: str) -> bool:
        """检查字段是否已配置（有表名和列名）"""
        cfg = self.get(field_key)
        return bool(cfg.get("table_name") and cfg.get("column_name"))

    def load_field_data(
        self,
        field_key: str,
        ts_codes: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pl.DataFrame]:
        """根据配置动态加载字段数据

        Args:
            field_key: 字段键（如 'industry', 'market_cap'）
            ts_codes: 股票代码列表
            start_date: 起始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            DataFrame with columns: ts_code, trade_date, {field_key}_value
            如果字段未配置或加载失败，返回 None
        """
        if not self.is_field_configured(field_key):
            logger.warning(f"字段 {field_key} 未配置，跳过加载")
            return None

        cfg = self.get(field_key)
        table = cfg["table_name"]
        column = cfg["column_name"]

        if not ts_codes:
            logger.warning(f"股票列表为空，跳过加载字段 {field_key}")
            return None

        # 构建查询（假设表结构包含 ts_code, trade_date）
        # 使用 IN 子句过滤股票
        placeholders = ", ".join(["%s"] * len(ts_codes))
        sql = f"""
            SELECT ts_code, trade_date, {column} AS {field_key}_value
            FROM {table}
            WHERE ts_code IN ({placeholders})
              AND trade_date >= %s AND trade_date <= %s
        """
        params = tuple(ts_codes) + (start_date, end_date)

        try:
            df = self.db.query(sql, params)
            if df.is_empty():
                logger.warning(f"字段 {field_key} 查询结果为空")
                return None
            logger.info(f"成功加载字段 {field_key}: {len(df)} 行")
            return df
        except Exception as e:
            logger.error(f"加载字段 {field_key} 失败: {e}")
            return None
