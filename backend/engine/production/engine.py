"""
因子生产引擎 (兼容层)

⚠️ DEPRECATED: 此实现已废弃,请使用 services.factor_compute_service.FactorComputeService

本文件现在作为兼容层存在,将调用转发到新的 FactorComputeService 实现。
保留此文件是为了向后兼容,但建议尽快迁移到新实现。

迁移指南:
    旧代码:
        from engine.production.engine import ProductionEngine
        engine = ProductionEngine(db_client)
        success = engine.run_task(factor_id="ma_5", target_date="20240101")

    新代码:
        from services.factor_compute_service import FactorComputeService
        service = FactorComputeService(db_client)
        result = service.compute_factor(factor_id="ma_5", target_date="20240101")
        success = result.success
"""
import warnings
import polars as pl
from app.core.utils import DateUtils
from typing import Optional, Dict, Any, List
from datetime import datetime

from app.core.logger import logger
from app.core.utils import TradingCalendar
from data_manager.processor import DataProcessor
from engine.production.registry import (
    FactorDefinition, StorageConfig, get_factor, get_registry, list_factors, discover_factors
)
from engine.production.data_config import DataConfigLoader

# 导入新实现
from services.factor_compute_service import FactorComputeService


# 数据表到查询列的映射
TABLE_COLUMNS = {
    "sync_daily_data": ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount", "pct_chg"],
    "sync_daily_basic": ["ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f", "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm", "total_share", "float_share", "free_share", "total_mv", "circ_mv"],
    "sync_adj_factor": ["ts_code", "trade_date", "adj_factor"],
    "sync_index_daily": ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount", "pct_chg"],
}

# 增量计算时，需要额外加载的历史窗口天数（用于滚动计算）
DEFAULT_LOOKBACK_DAYS = 60

# 新股上市后需排除的最少交易日数
IPO_EXCLUDE_DAYS = 60


class ProductionEngine:
    """因子生产引擎 (兼容层)

    ⚠️ DEPRECATED: 此类已废弃,请使用 FactorComputeService

    此类现在作为兼容层存在,将所有调用转发到 FactorComputeService。
    所有方法调用都会触发 DeprecationWarning。
    """

    def __init__(self, db_client):
        warnings.warn(
            "ProductionEngine is deprecated and will be removed in a future version. "
            "Please use services.factor_compute_service.FactorComputeService instead. "
            "See module docstring for migration guide.",
            DeprecationWarning,
            stacklevel=2
        )

        # 保留旧的属性以保持兼容性
        self.db = db_client
        self.trading_cal = TradingCalendar.get_instance(db_client)
        self.data_config = DataConfigLoader(db_client)

        # 创建新实现的实例
        self._service = FactorComputeService(db_client)

        # 确保 data_config 中引用的表都已注册到 _ALL_TABLES（用于 SQL 语法适配）
        self._register_config_tables()

    # 默认预处理选项
    DEFAULT_PREPROCESS = {
        "adjust_price": "forward",
        "filter_st": True,
        "filter_new_stock": True,
        "new_stock_days": 60,
        "mark_limit": True,
    }

    def _register_config_tables(self):
        """将 data_config 中引用的表名注册到 db._ALL_TABLES，确保 SQL 语法适配能识别"""
        try:
            config = self.data_config.load()
            for cfg in config.values():
                table_name = cfg.get("table_name", "")
                if table_name and table_name not in self.db._ALL_TABLES:
                    self.db.register_meta_table(table_name)
                # 也注册 extra_config 中引用的表（如 price_table）
                extra = cfg.get("extra_config", {})
                if isinstance(extra, dict):
                    for key in ("price_table",):
                        ref_table = extra.get(key, "")
                        if ref_table and ref_table not in self.db._ALL_TABLES:
                            self.db.register_meta_table(ref_table)
        except Exception as e:
            logger.debug(f"注册 data_config 表名失败: {e}")

    def run_task(
        self,
        factor_id: str,
        target_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mode: Optional[str] = None,
        preprocess: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """执行因子计算任务 (兼容方法 - 转发到 FactorComputeService)

        ⚠️ DEPRECATED: 请使用 FactorComputeService.compute_factor() 替代

        Args:
            factor_id: 因子ID
            target_date: 目标日期（增量模式下只算这一天）
            start_date: 开始日期（范围计算）
            end_date: 结束日期（范围计算）
            mode: 强制指定计算模式 ("incremental" / "full")，覆盖因子定义
            preprocess: 预处理选项，None 时从因子 params.preprocess 读取，仍无则使用默认值

        Returns:
            bool: 计算是否成功
        """
        logger.debug(
            f"ProductionEngine.run_task() is deprecated. "
            f"Forwarding to FactorComputeService.compute_factor()"
        )

        # 转发到新实现
        result = self._service.compute_factor(
            factor_id=factor_id,
            target_date=target_date,
            start_date=start_date,
            end_date=end_date,
            mode=mode,
            preprocess=preprocess,
            save_results=True  # 旧实现总是保存结果
        )

        # 转换返回值: ComputeResult -> bool
        return result.success
