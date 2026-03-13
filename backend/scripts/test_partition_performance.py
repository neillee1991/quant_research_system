#!/usr/bin/env python3
"""
测试 factor_values 分区性能

测试三种查询模式：
1. 按股票查询（时序）：查某只股票的因子历史值
2. 按日期查询（横截面）：查某天所有股票的因子值
3. 按因子查询：查某个因子的所有数据
"""

import sys
sys.path.insert(0, '/Users/lisheng/Code/quantsystem/quant_research_system/backend')

from store.dolphindb_client import db_client
from app.core.logger import logger
import time
import polars as pl

def test_query_by_stock(ts_code: str = "000001.SZ", factor_id: str = "ma_5"):
    """测试按股票查询（时序查询）"""
    logger.info(f"\n{'='*60}")
    logger.info(f"测试1: 按股票查询（时序） - {ts_code}, {factor_id}")
    logger.info(f"{'='*60}")

    start = time.time()

    sql = """
        SELECT trade_date, factor_value
        FROM factor_values
        WHERE ts_code = %s AND factor_id = %s
        ORDER BY trade_date
    """

    result = db_client.query(sql, (ts_code, factor_id))

    elapsed = time.time() - start
    row_count = len(result) if not result.is_empty() else 0

    logger.info(f"✅ 查询完成: {row_count} 行, 耗时 {elapsed:.3f}s, 速度 {row_count/elapsed:.0f} 行/秒")

    if not result.is_empty():
        logger.info(f"   数据预览: {result.head(5)}")

    return elapsed, row_count

def test_query_by_date(trade_date: str = "2024-01-02", factor_id: str = "ma_5"):
    """测试按日期查询（横截面查询）"""
    logger.info(f"\n{'='*60}")
    logger.info(f"测试2: 按日期查询（横截面） - {trade_date}, {factor_id}")
    logger.info(f"{'='*60}")

    start = time.time()

    sql = """
        SELECT ts_code, factor_value
        FROM factor_values
        WHERE trade_date = %s AND factor_id = %s
        ORDER BY ts_code
    """

    result = db_client.query(sql, (trade_date, factor_id))

    elapsed = time.time() - start
    row_count = len(result) if not result.is_empty() else 0

    logger.info(f"✅ 查询完成: {row_count} 行, 耗时 {elapsed:.3f}s, 速度 {row_count/elapsed:.0f} 行/秒")

    if not result.is_empty():
        logger.info(f"   数据预览: {result.head(5)}")

    return elapsed, row_count

def test_query_by_factor(factor_id: str = "ma_5", limit: int = 10000):
    """测试按因子查询（全量查询）"""
    logger.info(f"\n{'='*60}")
    logger.info(f"测试3: 按因子查询（全量） - {factor_id}, LIMIT {limit}")
    logger.info(f"{'='*60}")

    start = time.time()

    sql = f"""
        SELECT ts_code, trade_date, factor_value
        FROM factor_values
        WHERE factor_id = %s
        ORDER BY trade_date, ts_code
        LIMIT {limit}
    """

    result = db_client.query(sql, (factor_id,))

    elapsed = time.time() - start
    row_count = len(result) if not result.is_empty() else 0

    logger.info(f"✅ 查询完成: {row_count} 行, 耗时 {elapsed:.3f}s, 速度 {row_count/elapsed:.0f} 行/秒")

    if not result.is_empty():
        logger.info(f"   数据预览: {result.head(5)}")

    return elapsed, row_count

def test_query_date_range(ts_code: str = "000001.SZ", factor_id: str = "ma_5",
                          start_date: str = "2023-01-01", end_date: str = "2023-12-31"):
    """测试日期范围查询"""
    logger.info(f"\n{'='*60}")
    logger.info(f"测试4: 日期范围查询 - {ts_code}, {factor_id}, {start_date} ~ {end_date}")
    logger.info(f"{'='*60}")

    start = time.time()

    sql = """
        SELECT trade_date, factor_value
        FROM factor_values
        WHERE ts_code = %s AND factor_id = %s
          AND trade_date >= %s AND trade_date <= %s
        ORDER BY trade_date
    """

    result = db_client.query(sql, (ts_code, factor_id, start_date, end_date))

    elapsed = time.time() - start
    row_count = len(result) if not result.is_empty() else 0

    logger.info(f"✅ 查询完成: {row_count} 行, 耗时 {elapsed:.3f}s, 速度 {row_count/elapsed:.0f} 行/秒")

    if not result.is_empty():
        logger.info(f"   数据预览: {result.head(5)}")

    return elapsed, row_count

def check_partition_info():
    """检查分区信息"""
    logger.info(f"\n{'='*60}")
    logger.info("检查分区信息")
    logger.info(f"{'='*60}")

    try:
        # 查询表结构
        schema_info = db_client.execute("""
            db = database("dfs://quant");
            schema(loadTable(db, "factor_values"));
        """)

        logger.info("✅ 表结构信息:")
        logger.info(f"   {schema_info}")

        # 查询数据量
        count = db_client.query("SELECT count(*) as cnt FROM factor_values")
        if not count.is_empty():
            logger.info(f"\n✅ 总数据量: {count['cnt'][0]:,} 行")

        # 查询因子数量
        factor_count = db_client.query("SELECT count(distinct factor_id) as cnt FROM factor_values")
        if not factor_count.is_empty():
            logger.info(f"✅ 因子数量: {factor_count['cnt'][0]} 个")

        # 查询日期范围
        date_range = db_client.query("""
            SELECT min(trade_date) as min_date, max(trade_date) as max_date
            FROM factor_values
        """)
        if not date_range.is_empty():
            logger.info(f"✅ 日期范围: {date_range['min_date'][0]} ~ {date_range['max_date'][0]}")

    except Exception as e:
        logger.error(f"❌ 检查分区信息失败: {e}")

def main():
    """主测试流程"""
    logger.info("=" * 60)
    logger.info("factor_values 分区性能测试")
    logger.info("=" * 60)

    # 检查分区信息
    check_partition_info()

    # 测试1: 按股票查询（时序）
    try:
        test_query_by_stock("000001.SZ", "ma_5")
    except Exception as e:
        logger.error(f"❌ 测试1失败: {e}")

    # 测试2: 按日期查询（横截面）
    try:
        test_query_by_date("2024-01-02", "ma_5")
    except Exception as e:
        logger.error(f"❌ 测试2失败: {e}")

    # 测试3: 按因子查询（全量）
    try:
        test_query_by_factor("ma_5", limit=10000)
    except Exception as e:
        logger.error(f"❌ 测试3失败: {e}")

    # 测试4: 日期范围查询
    try:
        test_query_date_range("000001.SZ", "ma_5", "2023-01-01", "2023-12-31")
    except Exception as e:
        logger.error(f"❌ 测试4失败: {e}")

    logger.info("\n" + "=" * 60)
    logger.info("✅ 性能测试完成！")
    logger.info("=" * 60)
    logger.info("\n提示：")
    logger.info("  - 如果查询速度 > 10,000 行/秒，说明分区优化生效")
    logger.info("  - 如果查询速度 < 1,000 行/秒，可能需要检查分区策略")
    logger.info("  - 横截面查询应该比时序查询稍慢（需要扫描更多分区）")

if __name__ == "__main__":
    main()
