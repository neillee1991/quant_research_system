"""数据配置加载器 - 从 data_field_mappings 表读取字段映射

架构说明：
1. depends_on 配置的表会自动加载所有字段，无需在 factor_field_mappings 中配置
2. factor_field_mappings 只用于配置需要特殊处理的字段：
   - 需要计算的字段（如 is_st、is_limit 从特定表加载）
   - 需要跨表关联的字段（如 industry 从行业分类表）
   - 需要特殊处理的字段（如 market_cap 的单位转换）

缓存策略：
- 启动时通过 refresh() 从 PostgreSQL 预加载到内存
- load() / get() 纯同步读缓存，不阻塞因子引擎
- 配置更新后调用 invalidate_cache() + refresh() 刷新
"""
import json
import time
from typing import Optional, Dict, Any, List
import polars as pl

from app.core.logger import logger

_CACHE_TTL_SECONDS = 300  # 5 分钟后自动失效


# 内置默认值（因子分析 + 回测共用字段）
_DEFAULTS: Dict[str, Dict[str, Any]] = {
    # 行情字段（因子分析 + 回测共用）
    "open":        {"table_name": "sync_daily_data", "column_name": "open",       "extra_config": {}},
    "high":        {"table_name": "sync_daily_data", "column_name": "high",       "extra_config": {}},
    "low":         {"table_name": "sync_daily_data", "column_name": "low",        "extra_config": {}},
    "close":       {"table_name": "sync_daily_data", "column_name": "close",      "extra_config": {}},
    "volume":      {"table_name": "sync_daily_data", "column_name": "vol",        "extra_config": {}},
    # 回测专用字段
    "amount":      {"table_name": "sync_daily_data", "column_name": "amount",     "extra_config": {}},
    "limit_up":    {"table_name": "sync_stk_limit",  "column_name": "up_limit",   "extra_config": {}},
    "limit_down":  {"table_name": "sync_stk_limit",  "column_name": "down_limit", "extra_config": {}},
    # 因子分析专用字段
    "adj_factor":  {"table_name": "", "column_name": "adj_factor",  "extra_config": {}},
    "industry_l1": {"table_name": "", "column_name": "",            "extra_config": {}},
    "industry_l2": {"table_name": "", "column_name": "",            "extra_config": {}},
    "is_limit":    {"table_name": "", "column_name": "is_limit",    "extra_config": {}},
    "is_st":       {"table_name": "", "column_name": "is_st",       "extra_config": {}},
    "list_date":   {"table_name": "sync_stock_basic", "column_name": "list_date", "extra_config": {}},
    "market_cap":  {"table_name": "", "column_name": "market_cap",  "extra_config": {}},
}


# 单例实例和缓存
_instance: Optional['DataConfigLoader'] = None
_shared_cache: Optional[Dict[str, Dict[str, Any]]] = None
_shared_cache_ts: float = 0.0


class DataConfigLoader:
    """从 PostgreSQL factor_field_mappings 表加载字段映射配置，带内存缓存

    使用方式：
    - 启动时 await DataConfigLoader.get_instance(db_client).refresh()
    - 运行时 DataConfigLoader.get_instance(db_client).get(field_key)  # 纯同步，读缓存

    单例模式：所有实例共享同一份缓存
    """

    def __new__(cls, db_client):
        """单例模式：所有实例共享同一份缓存"""
        global _instance
        if _instance is None:
            _instance = super().__new__(cls)
            _instance._init_once(db_client)
        return _instance

    @classmethod
    def get_instance(cls, db_client) -> 'DataConfigLoader':
        """获取单例实例"""
        return cls(db_client)

    def _init_once(self, db_client):
        """只初始化一次的代码"""
        # db_client 仍用于 load_field_data()（查询 DolphinDB 时序表）
        self.db = db_client

    def __init__(self, db_client):
        """注意：__init__ 会在每次调用 DataConfigLoader(db_client) 时都执行，
        但实际初始化只在 _init_once 中执行一次。
        """
        # 实际初始化在 _init_once 中完成
        pass

    @property
    def _cache(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """获取共享缓存"""
        global _shared_cache
        return _shared_cache

    @_cache.setter
    def _cache(self, value: Optional[Dict[str, Dict[str, Any]]]) -> None:
        """设置共享缓存"""
        global _shared_cache
        _shared_cache = value

    @property
    def _cache_ts(self) -> float:
        """获取共享缓存时间戳"""
        global _shared_cache_ts
        return _shared_cache_ts

    @_cache_ts.setter
    def _cache_ts(self, value: float) -> None:
        """设置共享缓存时间戳"""
        global _shared_cache_ts
        _shared_cache_ts = value

    async def refresh(self) -> None:
        """从 PostgreSQL data_field_mappings 预加载配置到内存缓存"""
        try:
            from scheduler.db import DatabasePool
            rows = await DatabasePool.fetch("SELECT * FROM data_field_mappings")
            config: Dict[str, Dict[str, Any]] = {}
            for row in rows:
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
            logger.info(f"DataConfigLoader: 已从 PostgreSQL 加载 {len(config)} 条字段映射配置")
        except Exception as e:
            logger.warning(f"DataConfigLoader.refresh 失败 ({e})，使用内置默认值")
            if self._cache is None:
                self._cache = dict(_DEFAULTS)
                self._cache_ts = time.time()

    def load(self) -> Dict[str, Dict[str, Any]]:
        """返回内存缓存（同步）。若缓存为空则返回内置默认值。"""
        if self._cache is not None:
            return self._cache
        logger.warning("DataConfigLoader: 缓存未初始化，使用内置默认值（请确保启动时调用 refresh()）")
        return dict(_DEFAULTS)

    def get(self, field_key: str) -> Dict[str, Any]:
        """获取单个字段映射（同步）"""
        config = self.load()
        return config.get(field_key, _DEFAULTS.get(field_key, {
            "table_name": "", "column_name": "", "extra_config": {}
        }))

    def invalidate_cache(self) -> None:
        """清除缓存（配置更新时调用，之后需再调用 refresh()）"""
        self._cache = None

    def is_field_configured(self, field_key: str) -> bool:
        """检查字段是否已配置（有表名和列名）"""
        cfg = self.get(field_key)
        return bool(cfg.get("table_name") and cfg.get("column_name"))

    def load_price_fields(self, field_keys: List[str]) -> Dict[str, Dict[str, str]]:
        """批量获取多个字段的 table_name / column_name 配置"""
        return {k: self.get(k) for k in field_keys}

    def load_field_data(
        self,
        field_key: str,
        ts_codes: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pl.DataFrame]:
        """根据配置动态加载字段数据（查询 DolphinDB 时序表，保持同步）

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


class _DefaultsOnlyLoader:
    """无 db_client 时的轻量 loader，仅读 _DEFAULTS（供 QueryBuilder 在 db 初始化前使用）"""

    def get(self, field_key: str) -> Dict[str, Any]:
        return _DEFAULTS.get(field_key, {"table_name": "", "column_name": "", "extra_config": {}})

    def load_price_fields(self, field_keys: List[str]) -> Dict[str, Dict[str, str]]:
        return {k: self.get(k) for k in field_keys}


# 全局默认 loader（QueryBuilder 在没有 db_client 时使用）
# 应用启动后会被 DataConfigLoader 单例替换
data_config_loader: Any = _DefaultsOnlyLoader()
