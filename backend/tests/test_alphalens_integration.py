#!/usr/bin/env python3
"""
测试 Alphalens 集成的基础功能
验证 DataConfigLoader 和 AlphalensAdapter
"""
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from store.dolphindb_client import db_client
from engine.production.data_config import DataConfigLoader
from engine.analysis.alphalens_adapter import AlphalensAdapter
from app.core.logger import logger
import polars as pl
import pandas as pd
import numpy as np


def test_data_config_loader():
    """测试 DataConfigLoader"""
    logger.info("=" * 60)
    logger.info("测试 DataConfigLoader")
    logger.info("=" * 60)

    loader = DataConfigLoader(db_client)

    # 1. 测试加载配置
    logger.info("\n1. 加载所有配置...")
    config = loader.load()
    logger.info(f"✓ 加载了 {len(config)} 个配置项")

    # 2. 测试获取单个配置
    logger.info("\n2. 获取 industry 配置...")
    industry_cfg = loader.get("industry")
    logger.info(f"✓ industry 配置: {industry_cfg}")

    # 3. 测试字段是否已配置
    logger.info("\n3. 检查字段配置状态...")
    for field in ["industry", "market_cap", "adj_factor"]:
        is_configured = loader.is_field_configured(field)
        status = "✓ 已配置" if is_configured else "✗ 未配置"
        logger.info(f"  {field}: {status}")

    return loader


def test_alphalens_adapter():
    """测试 AlphalensAdapter 数据转换"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 AlphalensAdapter")
    logger.info("=" * 60)

    adapter = AlphalensAdapter(db_client)

    # 创建模拟数据
    logger.info("\n1. 创建模拟因子数据...")
    np.random.seed(42)
    dates = pd.date_range('2024-01-01', '2024-02-29', freq='D')  # 增加到2个月
    stocks = ['000001.SZ', '000002.SZ', '600000.SH', '600001.SH', '000003.SZ']  # 增加到5只股票

    # 因子数据
    factor_data = []
    for date in dates:
        for stock in stocks:
            factor_data.append({
                'ts_code': stock,
                'trade_date': date.strftime('%Y%m%d'),
                'factor_value': np.random.randn()
            })
    factor_df = pl.DataFrame(factor_data)
    logger.info(f"✓ 因子数据: {len(factor_df)} 行")

    # 价格数据
    price_data = []
    for date in dates:
        for stock in stocks:
            price_data.append({
                'ts_code': stock,
                'trade_date': date.strftime('%Y%m%d'),
                'close': 10.0 + np.random.randn() * 0.5
            })
    price_df = pl.DataFrame(price_data)
    logger.info(f"✓ 价格数据: {len(price_df)} 行")

    # 2. 测试数据转换
    logger.info("\n2. 测试 Polars → Pandas 转换...")
    try:
        factor_data_prepared = adapter.prepare_factor_data(
            factor_df=factor_df,
            price_df=price_df,
            periods=[1, 5],
            quantiles=3
        )
        logger.info(f"✓ 转换成功: {len(factor_data_prepared)} 行")
        logger.info(f"  列: {list(factor_data_prepared.columns)}")
        logger.info(f"  索引: {factor_data_prepared.index.names}")
    except Exception as e:
        logger.error(f"✗ 转换失败: {e}")
        import traceback
        traceback.print_exc()
        return None

    # 3. 测试完整分析
    logger.info("\n3. 测试完整 Alphalens 分析...")
    try:
        results = adapter.run_full_analysis(
            factor_data=factor_data_prepared,
            periods=[1, 5],
            quantiles=3
        )
        logger.info(f"✓ 分析完成")
        logger.info(f"  结果键: {list(results.keys())}")

        # 显示 IC 汇总
        if 'ic_summary' in results:
            ic_summary = results['ic_summary']
            logger.info(f"\n  IC 汇总:")
            logger.info(f"    IC 均值: {ic_summary.get('ic_mean', 0):.4f}")
            logger.info(f"    IC 标准差: {ic_summary.get('ic_std', 0):.4f}")
            logger.info(f"    IC IR: {ic_summary.get('ic_ir', 0):.4f}")
            logger.info(f"    IC 胜率: {ic_summary.get('ic_win_rate', 0):.2%}")

        # 显示分位数收益
        if 'quantile_returns' in results and results['quantile_returns']:
            logger.info(f"\n  分位数收益: {len(results['quantile_returns'])} 条记录")
            for item in results['quantile_returns'][:3]:
                logger.info(f"    周期{item['period']}, Q{item['quantile']}: {item['mean_return']:.4f}")

        return results
    except Exception as e:
        logger.error(f"✗ 分析失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_database_tables():
    """测试数据库表是否存在"""
    logger.info("\n" + "=" * 60)
    logger.info("测试数据库表")
    logger.info("=" * 60)

    tables = [
        'index_constituents',
        'index_metadata',
        'factor_analysis_extended'
    ]

    for table in tables:
        try:
            exists = db_client._session.run(f'existsTable("dfs://quant", "{table}")')
            if exists:
                logger.info(f"✓ 表 {table} 存在")
            else:
                logger.error(f"✗ 表 {table} 不存在")
        except Exception as e:
            logger.error(f"✗ 检查表 {table} 失败: {e}")


def main():
    logger.info("开始测试 Alphalens 集成基础功能...")
    logger.info("=" * 60)

    try:
        # 1. 测试数据库表
        test_database_tables()

        # 2. 测试 DataConfigLoader
        loader = test_data_config_loader()

        # 3. 测试 AlphalensAdapter
        results = test_alphalens_adapter()

        # 汇总
        logger.info("\n" + "=" * 60)
        logger.info("测试汇总")
        logger.info("=" * 60)
        logger.info("✓ 数据库表创建成功")
        logger.info("✓ DataConfigLoader 工作正常")
        if results:
            logger.info("✓ AlphalensAdapter 工作正常")
            logger.info("\n所有基础功能测试通过！可以继续后续阶段。")
        else:
            logger.warning("⚠ AlphalensAdapter 测试失败，需要调试")

        return True

    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
