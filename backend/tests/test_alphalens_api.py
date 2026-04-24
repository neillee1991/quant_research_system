#!/usr/bin/env python3
"""
测试 Alphalens 分析 API
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from fastapi.testclient import TestClient
from app.main import app
from app.core.logger import logger
from infrastructure.database.dolphindb_client import db_client
import polars as pl
import pandas as pd
import numpy as np

client = TestClient(app)


def setup_test_data():
    """创建测试数据：因子值和价格数据"""
    logger.info("=" * 60)
    logger.info("设置测试数据")
    logger.info("=" * 60)

    np.random.seed(42)
    dates = pd.date_range('2024-01-01', '2024-02-29', freq='D')
    stocks = ['000001.SZ', '000002.SZ', '600000.SH', '600001.SH', '000003.SZ']

    # 1. 创建测试因子数据
    logger.info("\n1. 创建测试因子数据...")
    factor_data = []
    for date in dates:
        for stock in stocks:
            factor_data.append({
                'ts_code': stock,
                'trade_date': date.strftime('%Y%m%d'),
                'factor_value': np.random.randn(),
                'factor_id': 'test_alphalens_factor'
            })

    factor_df = pl.DataFrame(factor_data)

    # 清理旧数据
    try:
        db_client._session.run("""
            factor_table = loadTable("dfs://quant", "factor_values");
            delete from factor_table where factor_id = "test_alphalens_factor";
        """)
    except:
        pass

    # 插入测试因子数据
    db_client.upsert('factor_values', factor_df, key_columns=['ts_code', 'trade_date', 'factor_id'])
    logger.info(f"✓ 插入了 {len(factor_df)} 条因子数据")

    # 2. 创建测试指数成分股数据
    logger.info("\n2. 创建测试价格数据...")
    price_data = []
    base_prices = {stock: 10.0 + np.random.rand() * 5 for stock in stocks}

    # 扩展日期范围以包含未来价格（用于计算远期收益）
    extended_dates = pd.date_range('2024-01-01', '2024-03-31', freq='D')

    for date in extended_dates:
        for stock in stocks:
            prev_price = base_prices[stock]
            base_prices[stock] *= (1 + np.random.randn() * 0.02)
            price_data.append({
                'ts_code': stock,
                'trade_date': date.strftime('%Y%m%d'),
                'close': base_prices[stock],
                'open': base_prices[stock] * 0.99,
                'high': base_prices[stock] * 1.02,
                'low': base_prices[stock] * 0.98,
                'pre_close': prev_price,
                'change': base_prices[stock] - prev_price,
                'pct_chg': ((base_prices[stock] - prev_price) / prev_price) * 100,
                'vol': np.random.randint(1000000, 10000000),
                'amount': base_prices[stock] * np.random.randint(1000000, 10000000)
            })

    price_df = pl.DataFrame(price_data)

    # 清理旧价格数据
    try:
        db_client._session.run("""
            price_table = loadTable("dfs://quant", "sync_daily_data");
            delete from price_table where ts_code in ["000001.SZ", "000002.SZ", "600000.SH", "600001.SH", "000003.SZ"];
        """)
    except:
        pass

    db_client.upsert('sync_daily_data', price_df, key_columns=['ts_code', 'trade_date'])
    logger.info(f"✓ 插入了 {len(price_df)} 条价格数据")

    # 3. 创建测试指数成分股数据
    logger.info("\n3. 创建测试指数成分股数据...")
    constituents_data = []
    index_stocks = ['000001.SZ', '000002.SZ', '600000.SH']

    for date in dates:
        for stock in index_stocks:
            constituents_data.append({
                'ts_code': stock,
                'trade_date': date.strftime('%Y%m%d'),
                'index_code': 'TEST_ALPHALENS_INDEX',
                'weight': 1.0 / len(index_stocks)
            })

    constituents_df = pl.DataFrame(constituents_data)

    # 清理旧数据
    try:
        db_client._session.run("""
            constituents_table = loadTable("dfs://quant", "index_constituents");
            delete from constituents_table where index_code = "TEST_ALPHALENS_INDEX";
        """)
        db_client._session.run("""
            metadata_table = loadTable("dfs://quant", "index_metadata");
            delete from metadata_table where index_code = "TEST_ALPHALENS_INDEX";
        """)
    except:
        pass

    db_client.upsert('index_constituents', constituents_df, key_columns=['trade_date', 'ts_code', 'index_code'])
    logger.info(f"✓ 插入了 {len(constituents_df)} 条指数成分股数据")

    # 插入元数据
    metadata_df = pl.DataFrame([{
        'index_code': 'TEST_ALPHALENS_INDEX',
        'index_name': '测试Alphalens指数',
        'description': '用于Alphalens API测试',
        'stock_count': len(index_stocks),
        'latest_date': dates[-1].strftime('%Y%m%d')
    }])
    db_client.upsert('index_metadata', metadata_df, key_columns=['index_code'])

    return True


def test_run_alphalens_analysis_basic():
    """测试 1: 基础 Alphalens 分析（无股票池、无分组）"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 1: 基础 Alphalens 分析")
    logger.info("=" * 60)

    payload = {
        "factor_id": "test_alphalens_factor",
        "start_date": "20240101",
        "end_date": "20240229",
        "periods": [1, 5, 10],
        "quantiles": 3
    }

    response = client.post("/api/v1/analysis/alphalens", json=payload)

    if response.status_code == 200:
        result = response.json()
        logger.info(f"✓ 分析完成")
        logger.info(f"  状态: {result['status']}")
        logger.info(f"  消息: {result['message']}")

        data = result['data']
        logger.info(f"  结果键: {list(data.keys())}")

        if 'ic_summary' in data:
            ic = data['ic_summary']
            logger.info(f"\n  IC 汇总:")
            logger.info(f"    IC 均值: {ic.get('ic_mean', 0):.4f}")
            logger.info(f"    IC 标准差: {ic.get('ic_std', 0):.4f}")
            logger.info(f"    IC IR: {ic.get('ic_ir', 0):.4f}")

        if 'quantile_returns' in data and data['quantile_returns']:
            logger.info(f"\n  分位数收益: {len(data['quantile_returns'])} 条记录")

        return True
    else:
        logger.error(f"✗ 分析失败: {response.status_code}")
        logger.error(f"  响应: {response.text}")
        return False


def test_run_alphalens_analysis_with_index_pool():
    """测试 2: 带股票池的 Alphalens 分析"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 2: 带股票池的 Alphalens 分析")
    logger.info("=" * 60)

    payload = {
        "factor_id": "test_alphalens_factor",
        "start_date": "20240101",
        "end_date": "20240229",
        "periods": [1, 5],
        "quantiles": 3,
        "index_pool": "TEST_ALPHALENS_INDEX"
    }

    response = client.post("/api/v1/analysis/alphalens", json=payload)

    if response.status_code == 200:
        result = response.json()
        logger.info(f"✓ 分析完成（带股票池）")
        logger.info(f"  状态: {result['status']}")

        data = result['data']
        if 'ic_summary' in data:
            ic = data['ic_summary']
            logger.info(f"\n  IC 汇总:")
            logger.info(f"    IC 均值: {ic.get('ic_mean', 0):.4f}")

        return True
    else:
        logger.error(f"✗ 分析失败: {response.status_code}")
        logger.error(f"  响应: {response.text}")
        return False


def test_get_latest_analysis():
    """测试 3: 获取最新分析结果"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 3: 获取最新分析结果")
    logger.info("=" * 60)

    response = client.get("/api/v1/analysis/alphalens/test_alphalens_factor/latest")

    if response.status_code == 200:
        result = response.json()
        logger.info(f"✓ 查询成功")
        logger.info(f"  状态: {result['status']}")

        data = result['data']
        logger.info(f"  因子ID: {data.get('factor_id')}")
        logger.info(f"  分析日期: {data.get('analysis_date')}")
        logger.info(f"  日期范围: {data.get('start_date')} ~ {data.get('end_date')}")
        logger.info(f"  任务状态: {data.get('task_status')}")

        if data.get('ic_summary'):
            logger.info(f"  IC 汇总: 已加载")

        return True
    else:
        logger.error(f"✗ 查询失败: {response.status_code}")
        logger.error(f"  响应: {response.text}")
        return False


def test_get_analysis_history():
    """测试 4: 获取分析历史记录"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 4: 获取分析历史记录")
    logger.info("=" * 60)

    response = client.get("/api/v1/analysis/alphalens/test_alphalens_factor/history?limit=10")

    if response.status_code == 200:
        result = response.json()
        logger.info(f"✓ 查询成功")
        logger.info(f"  状态: {result['status']}")

        data = result['data']
        logger.info(f"  记录数: {len(data['records'])}")
        logger.info(f"  总数: {data['total']}")
        logger.info(f"  分页: limit={data['limit']}, offset={data['offset']}")

        if data['records']:
            logger.info(f"\n  最新记录:")
            record = data['records'][0]
            logger.info(f"    ID: {record.get('id')}")
            logger.info(f"    分析日期: {record.get('analysis_date')}")
            logger.info(f"    状态: {record.get('task_status')}")

        return True
    else:
        logger.error(f"✗ 查询失败: {response.status_code}")
        logger.error(f"  响应: {response.text}")
        return False


def test_get_nonexistent_analysis():
    """测试 5: 查询不存在的因子（应返回 404）"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 5: 查询不存在的因子")
    logger.info("=" * 60)

    response = client.get("/api/v1/analysis/alphalens/nonexistent_factor/latest")

    if response.status_code == 404:
        logger.info(f"✓ 正确返回 404")
        return True
    else:
        logger.error(f"✗ 应返回 404，实际返回: {response.status_code}")
        return False


def cleanup():
    """清理测试数据"""
    logger.info("\n" + "=" * 60)
    logger.info("清理测试数据")
    logger.info("=" * 60)

    try:
        # 清理因子数据
        db_client._session.run("""
            factor_table = loadTable("dfs://quant", "factor_values");
            delete from factor_table where factor_id = "test_alphalens_factor";
        """)
        logger.info("✓ 清理因子数据")
    except Exception as e:
        logger.warning(f"清理因子数据失败: {e}")

    try:
        # 清理价格数据
        db_client._session.run("""
            price_table = loadTable("dfs://quant", "sync_daily_data");
            delete from price_table where ts_code in ["000001.SZ", "000002.SZ", "600000.SH", "600001.SH", "000003.SZ"];
        """)
        logger.info("✓ 清理价格数据")
    except Exception as e:
        logger.warning(f"清理价格数据失败: {e}")

    try:
        # 清理指数数据
        db_client._session.run("""
            constituents_table = loadTable("dfs://quant", "index_constituents");
            delete from constituents_table where index_code = "TEST_ALPHALENS_INDEX";
        """)
        db_client._session.run("""
            metadata_table = loadTable("dfs://quant", "index_metadata");
            delete from metadata_table where index_code = "TEST_ALPHALENS_INDEX";
        """)
        logger.info("✓ 清理指数数据")
    except Exception as e:
        logger.warning(f"清理指数数据失败: {e}")

    try:
        # 清理分析结果
        db_client._session.run("""
            analysis_table = loadTable("dfs://quant", "factor_analysis_extended");
            delete from analysis_table where factor_id = "test_alphalens_factor";
        """)
        logger.info("✓ 清理分析结果")
    except Exception as e:
        logger.warning(f"清理分析结果失败: {e}")


def main():
    logger.info("开始测试 Alphalens 分析 API...")
    logger.info("=" * 60)

    try:
        # 设置测试数据
        if not setup_test_data():
            logger.error("测试数据设置失败")
            return False

        # 运行测试
        test_results = []

        test_results.append(("基础 Alphalens 分析", test_run_alphalens_analysis_basic()))
        test_results.append(("带股票池分析", test_run_alphalens_analysis_with_index_pool()))
        test_results.append(("获取最新分析结果", test_get_latest_analysis()))
        test_results.append(("获取分析历史", test_get_analysis_history()))
        test_results.append(("查询不存在的因子", test_get_nonexistent_analysis()))

        # 清理
        cleanup()

        # 汇总
        logger.info("\n" + "=" * 60)
        logger.info("测试汇总")
        logger.info("=" * 60)

        for test_name, result in test_results:
            status = "✓ 通过" if result else "✗ 失败"
            logger.info(f"{status}: {test_name}")

        all_passed = all(result for _, result in test_results)

        if all_passed:
            logger.info("\n✅ 所有测试通过！Phase 4 完成。")
            logger.info("\n下一步:")
            logger.info("  - Phase 5: 实现指数股票池管理 UI")
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
