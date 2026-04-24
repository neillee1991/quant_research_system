#!/usr/bin/env python3
"""
测试集成了 Alphalens 的 FactorAnalyzer
验证完整的因子分析流程
"""
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from infrastructure.database.dolphindb_client import db_client
from engine.analysis.analyzer import FactorAnalyzer
from app.core.logger import logger
import polars as pl
import pandas as pd
import numpy as np


def setup_test_data():
    """创建测试数据：因子值和价格数据"""
    logger.info("=" * 60)
    logger.info("设置测试数据")
    logger.info("=" * 60)

    np.random.seed(42)
    dates = pd.date_range('2024-01-01', '2024-03-31', freq='D')  # 3个月数据
    stocks = ['000001.SZ', '000002.SZ', '600000.SH', '600001.SH', '000003.SZ', '600016.SH']

    # 1. 创建测试因子数据
    logger.info("\n1. 创建测试因子数据...")
    factor_data = []
    for date in dates:
        for stock in stocks:
            factor_data.append({
                'ts_code': stock,
                'trade_date': date.strftime('%Y%m%d'),
                'factor_value': np.random.randn(),
                'factor_id': 'test_factor_001'
            })

    factor_df = pl.DataFrame(factor_data)

    # 清理旧数据
    try:
        db_client.execute(
            "DELETE FROM factor_values WHERE factor_id = %s",
            ('test_factor_001',)
        )
    except:
        pass

    # 插入测试因子数据
    db_client.upsert('factor_values', factor_df, key_columns=['ts_code', 'trade_date', 'factor_id'])
    logger.info(f"✓ 插入了 {len(factor_df)} 条因子数据")

    # 2. 检查价格数据（假设已存在，不创建模拟数据）
    logger.info("\n2. 检查价格数据...")
    try:
        price_check = db_client.query("""
            SELECT COUNT(*) as cnt FROM sync_daily_data
            WHERE trade_date >= %s AND trade_date <= %s
        """, ('20240101', '20240430'))
        logger.info(f"✓ 价格数据充足 ({price_check['cnt'][0]} 条)")
    except Exception as e:
        logger.warning(f"⚠ 无法检查价格数据: {e}")
        logger.info("假设价格数据已存在，继续测试...")

    # 3. 创建测试指数成分股数据
    logger.info("\n3. 创建测试指数成分股数据...")
    constituents_data = []
    # 只选择部分股票作为指数成分股
    index_stocks = ['000001.SZ', '000002.SZ', '600000.SH']

    for date in dates:
        for stock in index_stocks:
            constituents_data.append({
                'ts_code': stock,
                'trade_date': date.strftime('%Y%m%d'),
                'index_code': 'TEST_INDEX',
                'weight': 1.0 / len(index_stocks)
            })

    constituents_df = pl.DataFrame(constituents_data)

    # 清理旧数据
    try:
        db_client.execute(
            "DELETE FROM index_constituents WHERE index_code = %s",
            ('TEST_INDEX',)
        )
    except:
        pass

    db_client.upsert('index_constituents', constituents_df, key_columns=['trade_date', 'ts_code', 'index_code'])
    logger.info(f"✓ 插入了 {len(constituents_df)} 条指数成分股数据")

    return True


def test_alphalens_analysis_basic():
    """测试基础 Alphalens 分析（无股票池、无分组）"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 1: 基础 Alphalens 分析")
    logger.info("=" * 60)

    analyzer = FactorAnalyzer(db_client)

    try:
        results = analyzer.analyze(
            factor_id='test_factor_001',
            start_date='20240101',
            end_date='20240331',
            periods=[1, 5, 10],
            quantiles=3,
            use_alphalens=True,
            index_pool=None,
            groupby_field=None
        )

        if results:
            logger.info("✓ 分析完成")
            logger.info(f"  结果键: {list(results.keys())}")

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

            return True
        else:
            logger.error("✗ 分析失败，返回 None")
            return False

    except Exception as e:
        logger.error(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_alphalens_analysis_with_index_pool():
    """测试带股票池过滤的 Alphalens 分析"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 2: 带股票池过滤的 Alphalens 分析")
    logger.info("=" * 60)

    analyzer = FactorAnalyzer(db_client)

    try:
        results = analyzer.analyze(
            factor_id='test_factor_001',
            start_date='20240101',
            end_date='20240331',
            periods=[1, 5],
            quantiles=3,
            use_alphalens=True,
            index_pool='TEST_INDEX',  # 使用测试指数
            groupby_field=None
        )

        if results:
            logger.info("✓ 分析完成（带股票池过滤）")
            logger.info(f"  结果键: {list(results.keys())}")

            if 'ic_summary' in results:
                ic = results['ic_summary']
                logger.info(f"\n  IC 汇总:")
                logger.info(f"    IC 均值: {ic.get('ic_mean', 0):.4f}")
                logger.info(f"    IC IR: {ic.get('ic_ir', 0):.4f}")

            return True
        else:
            logger.error("✗ 分析失败，返回 None")
            return False

    except Exception as e:
        logger.error(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_legacy_analysis():
    """测试传统分析方法（确保向后兼容）"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 3: 传统分析方法（向后兼容）")
    logger.info("=" * 60)

    analyzer = FactorAnalyzer(db_client)

    try:
        results = analyzer.analyze(
            factor_id='test_factor_001',
            start_date='20240101',
            end_date='20240331',
            periods=[1, 5],
            quantiles=3,
            use_alphalens=False  # 使用传统方法
        )

        if results:
            logger.info("✓ 传统分析完成")
            logger.info(f"  结果键: {list(results.keys())}")

            if 'ic_mean' in results:
                logger.info(f"\n  IC 均值: {results.get('ic_mean', 0):.4f}")
                logger.info(f"  IC IR: {results.get('ic_ir', 0):.4f}")

            return True
        else:
            logger.error("✗ 传统分析失败，返回 None")
            return False

    except Exception as e:
        logger.error(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_query_latest_analysis():
    """测试查询最新分析结果"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 4: 查询最新分析结果")
    logger.info("=" * 60)

    analyzer = FactorAnalyzer(db_client)

    try:
        # 查询 factor_analysis_extended 表
        df = db_client.query("""
            SELECT id, factor_id, analysis_date, start_date, end_date, task_status
            FROM factor_analysis_extended
            WHERE factor_id = %s
            ORDER BY analysis_date DESC
            LIMIT 5
        """, ('test_factor_001',))

        if not df.is_empty():
            logger.info(f"✓ 找到 {len(df)} 条分析记录")
            for row in df.to_dicts():
                logger.info(f"  ID: {row['id']}, 日期: {row['analysis_date']}, 状态: {row['task_status']}")
            return True
        else:
            logger.warning("⚠ 未找到分析记录")
            return False

    except Exception as e:
        logger.error(f"✗ 查询失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    logger.info("开始测试集成了 Alphalens 的 FactorAnalyzer...")
    logger.info("=" * 60)

    try:
        # 设置测试数据
        if not setup_test_data():
            logger.error("测试数据设置失败")
            return False

        # 运行测试
        test_results = []

        test_results.append(("基础 Alphalens 分析", test_alphalens_analysis_basic()))
        test_results.append(("带股票池过滤", test_alphalens_analysis_with_index_pool()))
        test_results.append(("传统分析方法", test_legacy_analysis()))
        test_results.append(("查询分析结果", test_query_latest_analysis()))

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
        else:
            logger.warning("\n⚠ 部分测试失败，需要调试")

        return all_passed

    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
