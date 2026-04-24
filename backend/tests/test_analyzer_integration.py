#!/usr/bin/env python3
"""
测试 FactorAnalyzer 与 Alphalens 集成
使用模拟数据进行端到端测试
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from infrastructure.database.dolphindb_client import db_client
from engine.analysis.analyzer import FactorAnalyzer
from app.core.logger import logger
import polars as pl
import pandas as pd
import numpy as np


def test_analyzer_with_mock_data():
    """使用内存模拟数据测试 FactorAnalyzer"""
    logger.info("=" * 60)
    logger.info("测试 FactorAnalyzer 与 Alphalens 集成（模拟数据）")
    logger.info("=" * 60)

    # 创建模拟数据
    np.random.seed(42)
    dates = pd.date_range('2024-01-01', '2024-02-29', freq='D')
    stocks = ['000001.SZ', '000002.SZ', '600000.SH', '600001.SH', '000003.SZ']

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

    # 价格数据
    price_data = []
    base_prices = {stock: 10.0 + np.random.rand() * 5 for stock in stocks}
    for date in pd.date_range('2024-01-01', '2024-03-31', freq='D'):
        for stock in stocks:
            base_prices[stock] *= (1 + np.random.randn() * 0.02)
            price_data.append({
                'ts_code': stock,
                'trade_date': date.strftime('%Y%m%d'),
                'close': base_prices[stock],
                'pct_chg': np.random.randn() * 2.0
            })
    price_df = pl.DataFrame(price_data)

    logger.info(f"✓ 创建模拟数据: {len(factor_df)} 因子记录, {len(price_df)} 价格记录")

    # 测试 AlphalensAdapter 直接调用
    logger.info("\n测试 AlphalensAdapter...")
    analyzer = FactorAnalyzer(db_client)

    try:
        # 准备数据
        factor_data_prepared = analyzer.alphalens_adapter.prepare_factor_data(
            factor_df=factor_df,
            price_df=price_df,
            periods=[1, 5, 10],
            quantiles=3
        )
        logger.info(f"✓ 数据准备成功: {len(factor_data_prepared)} 行")

        # 运行分析
        results = analyzer.alphalens_adapter.run_full_analysis(
            factor_data=factor_data_prepared,
            periods=[1, 5, 10],
            quantiles=3
        )

        logger.info(f"✓ 分析完成")
        logger.info(f"  结果键: {list(results.keys())}")

        # 显示结果
        if 'ic_summary' in results:
            ic = results['ic_summary']
            logger.info(f"\n  IC 汇总:")
            logger.info(f"    IC 均值: {ic.get('ic_mean', 0):.4f}")
            logger.info(f"    IC 标准差: {ic.get('ic_std', 0):.4f}")
            logger.info(f"    IC IR: {ic.get('ic_ir', 0):.4f}")
            logger.info(f"    IC 胜率: {ic.get('ic_win_rate', 0):.2%}")

        if 'quantile_returns' in results and results['quantile_returns']:
            logger.info(f"\n  分位数收益: {len(results['quantile_returns'])} 条记录")
            for item in results['quantile_returns'][:3]:
                logger.info(f"    周期{item['period']}, Q{item['quantile']}: {item['mean_return']:.4f}")

        if 'turnover' in results and results['turnover']:
            logger.info(f"\n  换手率: {len(results['turnover'])} 个分位数")

        if 'decay_analysis' in results and results['decay_analysis']:
            logger.info(f"\n  衰减分析: {len(results['decay_analysis'])} 个周期")

        logger.info("\n✅ FactorAnalyzer 与 Alphalens 集成测试通过！")
        return True

    except Exception as e:
        logger.error(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_data_config_loader():
    """测试 DataConfigLoader"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 DataConfigLoader")
    logger.info("=" * 60)

    analyzer = FactorAnalyzer(db_client)
    loader = analyzer.data_config_loader

    try:
        # 测试加载配置
        config = loader.load()
        logger.info(f"✓ 加载了 {len(config)} 个配置项")

        # 测试字段检查
        for field in ['industry', 'market_cap', 'adj_factor']:
            is_configured = loader.is_field_configured(field)
            status = "✓ 已配置" if is_configured else "✗ 未配置"
            logger.info(f"  {field}: {status}")

        return True

    except Exception as e:
        logger.error(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    logger.info("开始测试 FactorAnalyzer 与 Alphalens 集成...")
    logger.info("=" * 60)

    try:
        test_results = []

        # 测试 1: AlphalensAdapter 集成
        test_results.append(("AlphalensAdapter 集成", test_analyzer_with_mock_data()))

        # 测试 2: DataConfigLoader
        test_results.append(("DataConfigLoader", test_data_config_loader()))

        # 汇总
        logger.info("\n" + "=" * 60)
        logger.info("测试汇总")
        logger.info("=" * 60)

        for test_name, result in test_results:
            status = "✓ 通过" if result else "✗ 失败"
            logger.info(f"{status}: {test_name}")

        all_passed = all(result for _, result in test_results)

        if all_passed:
            logger.info("\n✅ 所有测试通过！Phase 2.5 完成。")
            logger.info("\n下一步:")
            logger.info("  - Phase 3: 实现指数股票池 CRUD API")
            logger.info("  - Phase 4: 实现 Alphalens 分析 API")
        else:
            logger.warning("\n⚠ 部分测试失败")

        return all_passed

    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
