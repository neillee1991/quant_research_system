"""因子数据配置加载器 - 从 factor_data_config 表读取字段映射"""
import json
from typing import Optional, Dict, Any

from app.core.logger import logger


# 内置默认值（当 DB 中无配置时的 fallback）
# 注意：表名默认为空，要求用户在"因子-数据配置"界面配置实际数据源
_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "adj_factor": {"table_name": "sync_adj_factor", "column_name": "adj_factor", "extra_config": {}},
    "list_date": {"table_name": "sync_stock_basic", "column_name": "list_date", "extra_config": {}},
    # 股票状态字段：默认不配置，要求用户在配置界面设置数据源
    "is_st": {"table_name": "", "column_name": "is_st", "extra_config": {}},
    "is_suspend": {"table_name": "", "column_name": "is_suspend", "extra_config": {}},
    "is_limit": {"table_name": "", "column_name": "is_limit", "extra_config": {}},
    "industry_l1": {"table_name": "", "column_name": "", "extra_config": {}},
    "industry_l2": {"table_name": "", "column_name": "", "extra_config": {}},
    "market_cap": {"table_name": "sync_daily_basic", "column_name": "total_mv", "extra_config": {}},
}


class DataConfigLoader:
    """从 DolphinDB factor_data_config 表加载字段映射配置，带内存缓存"""

    def __init__(self, db_client):
        self.db = db_client
        self._cache: Optional[Dict[str, Dict[str, Any]]] = None

    def load(self) -> Dict[str, Dict[str, Any]]:
        """从 DB 加载配置，带内存缓存"""
        if self._cache is not None:
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
            return config
        except Exception as e:
            logger.warning(f"加载 factor_data_config 失败 ({e})，使用内置默认值")
            self._cache = dict(_DEFAULTS)
            return self._cache

    def get(self, field_key: str) -> Dict[str, Any]:
        """获取单个字段映射"""
        config = self.load()
        return config.get(field_key, _DEFAULTS.get(field_key, {"table_name": "", "column_name": "", "extra_config": {}}))

    def invalidate_cache(self) -> None:
        """清除缓存（配置更新时调用）"""
        self._cache = None
