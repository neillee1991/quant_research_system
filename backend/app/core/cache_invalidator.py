"""
缓存失效工具
提供缓存清理和失效策略
"""
from typing import List, Optional
from store.redis_client import redis_client
from app.core.logger import logger


class CacheInvalidator:
    """缓存失效管理器"""

    @staticmethod
    def invalidate_stock_list():
        """清除股票列表缓存"""
        pattern = "stock_list:*"
        count = redis_client.delete_pattern(pattern)
        logger.info(f"Invalidated {count} stock list cache entries")
        return count

    @staticmethod
    def invalidate_daily_data(ts_code: Optional[str] = None):
        """
        清除日线数据缓存

        Args:
            ts_code: 股票代码，如果为None则清除所有
        """
        if ts_code:
            pattern = f"daily_data:{ts_code}:*"
        else:
            pattern = "daily_data:*"

        count = redis_client.delete_pattern(pattern)
        logger.info(f"Invalidated {count} daily data cache entries for {ts_code or 'all'}")
        return count

    @staticmethod
    def invalidate_factor_metadata():
        """清除因子元数据缓存"""
        pattern = "production:factors:*"
        count = redis_client.delete_pattern(pattern)
        logger.info(f"Invalidated {count} factor metadata cache entries")
        return count

    @staticmethod
    def invalidate_factor_analysis(factor_id: Optional[str] = None):
        """
        清除因子分析缓存

        Args:
            factor_id: 因子ID，如果为None则清除所有
        """
        if factor_id:
            pattern = f"factor_analysis:{factor_id}:*"
        else:
            pattern = "factor_analysis:*"

        count = redis_client.delete_pattern(pattern)
        logger.info(f"Invalidated {count} factor analysis cache entries for {factor_id or 'all'}")
        return count

    @staticmethod
    def invalidate_all():
        """清除所有缓存"""
        patterns = [
            "stock_list:*",
            "daily_data:*",
            "production:factors:*",
            "factor_analysis:*",
        ]

        total = 0
        for pattern in patterns:
            count = redis_client.delete_pattern(pattern)
            total += count

        logger.info(f"Invalidated {total} total cache entries")
        return total

    @staticmethod
    def on_data_sync_complete(data_type: str, ts_codes: Optional[List[str]] = None):
        """
        数据同步完成后的缓存失效处理

        Args:
            data_type: 数据类型 (stock_basic, daily_data, daily_basic等)
            ts_codes: 受影响的股票代码列表
        """
        logger.info(f"Processing cache invalidation for data_type={data_type}")

        if data_type == "stock_basic":
            # 股票基础信息更新，清除股票列表缓存
            CacheInvalidator.invalidate_stock_list()

        elif data_type in ["daily_data", "daily_basic"]:
            # 日线数据更新
            if ts_codes:
                # 清除特定股票的缓存
                for ts_code in ts_codes:
                    CacheInvalidator.invalidate_daily_data(ts_code)
            else:
                # 清除所有日线数据缓存
                CacheInvalidator.invalidate_daily_data()

        elif data_type == "factor_values":
            # 因子数据更新，清除因子元数据缓存
            CacheInvalidator.invalidate_factor_metadata()

        logger.info(f"Cache invalidation completed for {data_type}")


# 导出单例
cache_invalidator = CacheInvalidator()
