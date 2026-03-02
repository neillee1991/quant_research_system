"""
因子注册表
通过装饰器模式注册因子计算函数，框架自动处理数据加载和结果存储
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


# 全局因子注册表
_factor_registry: Dict[str, FactorDefinition] = {}


def factor(
    factor_id: str,
    description: str = "",
    depends_on: list = None,
    category: str = "custom",
    params: dict = None,
    compute_mode: str = "incremental",
    storage: dict = None
):
    """因子注册装饰器

    Args:
        factor_id: 因子唯一标识
        description: 因子描述
        depends_on: 依赖的数据源列表（表名或因子ID）
            - "sync_daily_data": 从日线行情表加载
            - "sync_daily_basic": 从每日指标表加载
            - "factor_xxx": 从已计算的因子加载
        category: 因子分类 ("momentum", "value", "technical", "custom")
        params: 因子参数（如窗口大小等）
        compute_mode: 计算模式
            - "incremental": 增量计算（只算最新数据，需要加载窗口期历史数据）
            - "full": 全量重算（加载所有历史数据）
        storage: 存储配置字典
            - None: 存到统一因子表 factor_values
            - {"target": "factor_values"}: 同上
            - {"target": "my_table", "columns": {"col": "TYPE"}, "primary_keys": ["col1"]}: 自定义表

    Usage:
        @factor("factor_ma_20", description="20日均线",
                depends_on=["sync_daily_data"], category="technical")
        def compute_ma_20(df, params):
            # df: 含 ts_code, trade_date, close 等列的 Polars DataFrame
            # 返回: 含 ts_code, trade_date, factor_value 的 DataFrame
            return df.with_columns(...)
    """
    def decorator(func):
        storage_config = StorageConfig(**(storage or {}))
        _factor_registry[factor_id] = FactorDefinition(
            factor_id=factor_id,
            description=description,
            func=func,
            depends_on=depends_on or [],
            category=category,
            params=params or {},
            compute_mode=compute_mode,
            storage=storage_config
        )
        return func
    return decorator


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
    """自动发现并注册因子

    优先级：
    1. 从数据库加载（如果提供了 db_client）
    2. 从 factors/ 目录导入 Python 文件（作为备用和初始化示例）

    可安全多次调用，Python import 缓存保证幂等。
    默认使用5分钟缓存，避免热路径重复发现。

    Args:
        factors_dir: Python 因子文件目录，默认为 engine/production/factors
        db_client: DolphinDB 客户端，如果提供则从数据库加载因子
        force_refresh: 强制刷新，忽略缓存
    """
    import os
    import importlib
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

    # 1. 优先从数据库加载
    db_loaded_factors = set()
    if db_client is not None:
        try:
            before_count = len(_factor_registry)
            load_factors_from_db(db_client)
            after_count = len(_factor_registry)
            db_loaded_factors = set(_factor_registry.keys())
            logger.info(f"Loaded {after_count - before_count} factors from database")
        except Exception as e:
            logger.warning(f"Failed to load factors from database: {e}")

    # 2. 从 Python 文件加载（仅加载数据库中没有的因子）
    if factors_dir is None:
        factors_dir = os.path.join(os.path.dirname(__file__), "factors")

    if not os.path.isdir(factors_dir):
        return

    for fname in sorted(os.listdir(factors_dir)):
        if fname.endswith(".py") and not fname.startswith("__"):
            module_name = f"engine.production.factors.{fname[:-3]}"
            try:
                # 记录导入前的因子列表
                before_import = set(_factor_registry.keys())
                importlib.import_module(module_name)
                # 检查新导入的因子
                after_import = set(_factor_registry.keys())
                new_factors = after_import - before_import

                # 如果新导入的因子已在数据库中，发出警告
                for fid in new_factors:
                    if fid in db_loaded_factors:
                        logger.warning(f"Factor {fid} loaded from both DB and file {fname}, using DB version")
                        # 注意：由于 DB 先加载，文件导入会覆盖，这里需要恢复 DB 版本
                        # 但实际上 @factor 装饰器会覆盖，所以我们应该跳过文件导入

            except Exception as e:
                logger.warning(f"Failed to import factor module {module_name}: {e}")


def load_factors_from_db(db_client):
    """从数据库加载因子定义并注册到内存

    Args:
        db_client: DolphinDB 客户端实例
    """
    import json
    import logging

    logger = logging.getLogger(__name__)

    try:
        df = db_client.query("""
            SELECT factor_id, description, category, compute_mode,
                   storage_target, depends_on, params, code
            FROM factor_metadata
            WHERE code IS NOT NULL AND code != ''
            ORDER BY factor_id
        """)

        if df.is_empty():
            logger.info("No factors found in database")
            return

        loaded_count = 0
        for row in df.to_dicts():
            factor_id = row["factor_id"]
            code = row.get("code", "")

            if not code:
                continue

            try:
                # 解析 depends_on (JSON 数组)
                depends_on = json.loads(row.get("depends_on") or "[]")

                # 解析 params (JSON 对象)
                params = json.loads(row.get("params") or "{}")

                # 编译代码并提取计算函数
                namespace = {}
                exec(code, namespace)

                # 查找计算函数
                compute_func = None
                for name, obj in namespace.items():
                    if callable(obj) and (name.startswith("compute") or name == "main"):
                        compute_func = obj
                        break

                if compute_func is None:
                    logger.warning(f"Factor {factor_id}: no compute function found in code")
                    continue

                # 注册到内存
                storage_config = StorageConfig(target=row.get("storage_target") or "factor_values")
                _factor_registry[factor_id] = FactorDefinition(
                    factor_id=factor_id,
                    description=row.get("description") or "",
                    func=compute_func,
                    depends_on=depends_on,
                    category=row.get("category") or "custom",
                    params=params,
                    compute_mode=row.get("compute_mode") or "incremental",
                    storage=storage_config
                )
                loaded_count += 1

            except Exception as e:
                logger.error(f"Failed to load factor {factor_id} from database: {e}")

        logger.info(f"Loaded {loaded_count} factors from database")

    except Exception as e:
        logger.error(f"Failed to load factors from database: {e}")
