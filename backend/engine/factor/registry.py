"""
因子注册表
从 PostgreSQL factor_configs 表动态加载因子代码，exec 编译后注册到内存注册表。
"""
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Any, Optional
from datetime import datetime, timedelta


# 因子发现缓存
_discovery_cache = {
    "last_discovery_time": None,
    "cache_ttl_seconds": 300  # 5分钟缓存
}


@dataclass
class StorageConfig:
    """因子存储配置"""
    target: str = "factor_values"  # 默认存到统一因子表，也可指定自定义表名
    columns: Optional[Dict[str, str]] = None  # 自定义表的列定义 {列名: 类型}
    primary_keys: Optional[List[str]] = None  # 自定义表的主键


@dataclass
class FactorDefinition:
    """因子定义"""
    factor_id: str
    description: str
    func: Callable
    depends_on: List[str]
    category: str
    params: Dict[str, Any]
    compute_mode: str  # "incremental" 或 "full"
    storage: StorageConfig
    align_calendar: bool = False  # 是否对齐交易日历（窗口内每个交易日都必须有数据）


# 全局因子注册表
_factor_registry: Dict[str, FactorDefinition] = {}


def get_registry() -> Dict[str, FactorDefinition]:
    """获取因子注册表"""
    return _factor_registry


def get_factor(factor_id: str) -> Optional[FactorDefinition]:
    """获取指定因子定义"""
    return _factor_registry.get(factor_id)


def list_factors() -> List[Dict[str, Any]]:
    """列出所有已注册因子的摘要信息"""
    return [
        {
            "factor_id": f.factor_id,
            "description": f.description,
            "category": f.category,
            "compute_mode": f.compute_mode,
            "depends_on": f.depends_on,
            "storage_target": f.storage.target,
            "params": f.params,
        }
        for f in _factor_registry.values()
    ]


def unregister_factor(factor_id: str):
    """从注册表中移除因子"""
    _factor_registry.pop(factor_id, None)


def discover_factors(factors_dir: str = None, db_client=None, force_refresh: bool = False):
    """从数据库加载因子（纯数据库驱动）

    不再从 Python 文件扫描，所有因子管理通过数据库完成。
    默认使用5分钟缓存，避免热路径重复加载。

    Args:
        factors_dir: 已废弃，保留参数以兼容旧代码
        db_client: DolphinDB 客户端（必需）
        force_refresh: 强制刷新，忽略缓存
    """
    import logging

    logger = logging.getLogger(__name__)

    # 检查缓存
    if not force_refresh:
        last_time = _discovery_cache.get("last_discovery_time")
        if last_time:
            elapsed = (datetime.now() - last_time).total_seconds()
            if elapsed < _discovery_cache["cache_ttl_seconds"]:
                logger.debug(f"Using cached factor discovery (age: {elapsed:.1f}s)")
                return

    # 更新缓存时间
    _discovery_cache["last_discovery_time"] = datetime.now()

    # 从数据库加载所有因子
    if db_client is None:
        logger.warning("No db_client provided, cannot load factors")
        return

    try:
        before_count = len(_factor_registry)
        load_factors_from_db(db_client)
        after_count = len(_factor_registry)
        logger.info(f"Loaded {after_count - before_count} factors from database (total: {after_count})")
    except Exception as e:
        logger.error(f"Failed to load factors from database: {e}", exc_info=True)


def load_factors_from_db(db_client):
    """从 PostgreSQL factor_configs 加载因子定义并注册到内存

    使用 psycopg2（同步）查询 PostgreSQL，避免在同步上下文中调用 asyncpg。
    db_client 参数保留以兼容旧调用方，实际不再使用。
    """
    import json
    import logging
    import psycopg2
    import psycopg2.extras

    logger = logging.getLogger(__name__)

    try:
        from app.core.config import settings
        conn = psycopg2.connect(
            host=settings.postgresql.postgres_host,
            port=settings.postgresql.postgres_port,
            dbname=settings.postgresql.postgres_db,
            user=settings.postgresql.postgres_user,
            password=settings.postgresql.postgres_password,
        )
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT factor_id, description, category, compute_mode,
                           storage_target, depends_on, params, code, align_calendar
                    FROM factor_configs
                    WHERE code IS NOT NULL AND code != ''
                    ORDER BY factor_id
                """)
                rows = cur.fetchall()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to query factor_configs from PostgreSQL: {e}")
        return

    if not rows:
        logger.info("No factors found in factor_configs")
        return

    loaded_count = 0
    for row in rows:
        factor_id = row["factor_id"]
        code = row.get("code", "")

        if not code:
            continue

        try:
            depends_on = json.loads(row.get("depends_on") or "[]")
            params = json.loads(row.get("params") or "{}")

            from app.core.sandbox import code_sandbox
            is_safe, error = code_sandbox.check_security(code)
            if not is_safe:
                logger.warning(f"Factor {factor_id} security check failed: {error}, skipping")
                continue

            import numpy as np
            import pandas as pd
            import polars as pl
            namespace = {
                "np": np, "numpy": np,
                "pd": pd, "pandas": pd,
                "pl": pl, "polars": pl,
            }
            exec(code, namespace)

            compute_func = None
            for name, obj in namespace.items():
                if callable(obj) and (name.startswith("compute") or name == "main"):
                    compute_func = obj
                    break

            if compute_func is None:
                logger.warning(f"Factor {factor_id}: no compute function found in code")
                continue

            storage_config = StorageConfig(target=row.get("storage_target") or "factor_values")
            _factor_registry[factor_id] = FactorDefinition(
                factor_id=factor_id,
                description=row.get("description") or "",
                func=compute_func,
                depends_on=depends_on,
                category=row.get("category") or "custom",
                params=params,
                compute_mode=row.get("compute_mode") or "incremental",
                storage=storage_config,
                align_calendar=bool(row.get("align_calendar", False)),
            )
            loaded_count += 1

        except Exception as e:
            logger.error(f"Failed to load factor {factor_id} from database: {e}")

    logger.info(f"Loaded {loaded_count} factors from factor_configs (PostgreSQL)")
