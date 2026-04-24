"""
使用示例文档 - Infrastructure Layer

本文档展示如何使用 QueryBuilder 和 Repository 模式进行数据访问。
"""

# ============================================================================
# 1. QueryBuilder 使用示例
# ============================================================================

from infrastructure.database.query_builder import QueryBuilder

# 示例 1: 简单查询
query = QueryBuilder("sync_daily_data") \
    .select(["ts_code", "trade_date", "close"]) \
    .where("trade_date", "=", "20240101") \
    .build()

print(query.sql)
# 输出: SELECT ts_code, trade_date, close FROM sync_daily_data WHERE trade_date = %s
print(query.params)
# 输出: ('20240101',)

# 执行查询
result = db_client.execute(query.sql, query.params)


# 示例 2: WHERE IN 查询
query = QueryBuilder("sync_daily_data") \
    .select(["ts_code", "trade_date", "close"]) \
    .where_in("ts_code", ["000001.SZ", "000002.SZ", "000003.SZ"]) \
    .where_between("trade_date", "20240101", "20240131") \
    .order_by(["trade_date DESC", "ts_code ASC"]) \
    .limit(100) \
    .build()

result = db_client.execute(query.sql, query.params)


# 示例 3: 复杂查询
query = QueryBuilder("factor_values") \
    .select(["ts_code", "trade_date", "factor_value"]) \
    .where("factor_id", "=", "momentum_20") \
    .where_in("ts_code", ["000001.SZ", "000002.SZ"]) \
    .where_between("trade_date", "20240101", "20240131") \
    .where_not_null("factor_value") \
    .order_by(["trade_date DESC"]) \
    .build()

result = db_client.execute(query.sql, query.params)


# 示例 4: 重用 QueryBuilder
builder = QueryBuilder("sync_daily_data")

# 第一次查询
query1 = builder.select(["close"]).where("ts_code", "=", "000001.SZ").build()
result1 = db_client.execute(query1.sql, query1.params)

# 重置后第二次查询
query2 = builder.reset().select(["open", "high", "low"]).where("ts_code", "=", "000002.SZ").build()
result2 = db_client.execute(query2.sql, query2.params)


# ============================================================================
# 2. MarketDataRepository 使用示例
# ============================================================================

from infrastructure.repository.market_data_repository import MarketDataRepository

# 创建 Repository
market_repo = MarketDataRepository(db_client)

# 示例 1: 查询日期范围内的所有数据
df = market_repo.find_by_date_range("20240101", "20240131")
print(f"查询到 {len(df)} 条记录")

# 示例 2: 查询指定股票的数据
df = market_repo.find_by_codes(
    ts_codes=["000001.SZ", "000002.SZ"],
    start_date="20240101",
    end_date="20240131",
    columns=["ts_code", "trade_date", "close"]
)

# 示例 3: 查询带前复权的数据
df = market_repo.get_with_adjustment(
    ts_codes=["000001.SZ", "000002.SZ"],
    start_date="20240101",
    end_date="20240131",
    adjust_type="forward"  # forward=前复权, backward=后复权, none=不复权
)

# 示例 4: 查询带股票状态的数据（过滤 ST、新股）
df = market_repo.get_with_status(
    ts_codes=["000001.SZ"],
    start_date="20240101",
    end_date="20240131",
    filter_st=True,           # 过滤 ST 股票
    filter_new_stock=True,    # 过滤新股
    new_stock_days=60,        # 上市 < 60 天视为新股
    mark_limit=True           # 标记涨跌停
)

# 示例 5: 获取最新交易日期
latest_date = market_repo.get_latest_date()
print(f"最新交易日期: {latest_date}")

# 获取指定股票的最新日期
latest_date = market_repo.get_latest_date("000001.SZ")

# 示例 6: 获取指定日期的所有股票代码
codes = market_repo.get_codes_by_date("20240101")
print(f"2024-01-01 共有 {len(codes)} 只股票")

# 示例 7: 保存数据
new_data = pl.DataFrame({
    "ts_code": ["000001.SZ"],
    "trade_date": ["20240201"],
    "open": [10.0],
    "high": [11.0],
    "low": [9.5],
    "close": [10.5],
    "vol": [1000000],
    "amount": [10500000],
    "pct_chg": [5.0]
})

count = market_repo.save(new_data)
print(f"保存了 {count} 条记录")


# ============================================================================
# 3. FactorDataRepository 使用示例
# ============================================================================

from infrastructure.repository.factor_data_repository import FactorDataRepository

# 创建 Repository
factor_repo = FactorDataRepository(db_client)

# 示例 1: 查询因子值
df = factor_repo.get_factor_values(
    factor_id="momentum_20",
    ts_codes=["000001.SZ", "000002.SZ"],
    start_date="20240101",
    end_date="20240131"
)

# 示例 2: 保存因子计算结果
result_df = pl.DataFrame({
    "ts_code": ["000001.SZ", "000002.SZ"],
    "trade_date": ["20240101", "20240101"],
    "factor_value": [0.05, 0.03],
    "quality_flag": ["good", "good"]
})

count = factor_repo.save_factor_results(
    factor_id="momentum_20",
    data=result_df,
    run_id=123
)
print(f"保存了 {count} 条因子值")

# 示例 3: 获取因子最新日期
latest_date = factor_repo.get_latest_date("momentum_20")
print(f"因子 momentum_20 最新日期: {latest_date}")

# 示例 4: 获取因子日期范围
min_date, max_date = factor_repo.get_date_range("momentum_20")
print(f"因子日期范围: {min_date} ~ {max_date}")

# 示例 5: 获取因子质量统计
stats = factor_repo.get_quality_stats(
    factor_id="momentum_20",
    start_date="20240101",
    end_date="20240131"
)
print(f"总记录数: {stats['total_count']}")
print(f"空值率: {stats['null_rate']:.2%}")
print(f"均值: {stats['mean']:.4f}")
print(f"标准差: {stats['std']:.4f}")

# 示例 6: 获取因子覆盖率
coverage = factor_repo.get_factor_coverage(
    factor_id="momentum_20",
    trade_date="20240101"
)
print(f"总股票数: {coverage['total_stocks']}")
print(f"有因子值的股票数: {coverage['factor_stocks']}")
print(f"覆盖率: {coverage['coverage_rate']:.2%}")

# 示例 7: 删除因子值
count = factor_repo.delete_factor_values(
    factor_id="momentum_20",
    start_date="20240101",
    end_date="20240131",
    ts_codes=["000001.SZ"]
)
print(f"删除了 {count} 条记录")

# 示例 8: 获取指定日期的所有因子值（宽表格式）
df = factor_repo.get_factors_by_date(
    trade_date="20240101",
    ts_codes=["000001.SZ", "000002.SZ"]
)
# 返回格式: ts_code | momentum_20 | rsi_14 | macd | ...


# ============================================================================
# 4. 在 ProductionEngine 中使用 Repository
# ============================================================================

from infrastructure.repository.market_data_repository import MarketDataRepository
from infrastructure.repository.factor_data_repository import FactorDataRepository

class ProductionEngine:
    def __init__(self, db_client):
        self.db = db_client
        self.market_repo = MarketDataRepository(db_client)
        self.factor_repo = FactorDataRepository(db_client)

    def run_task(self, factor_id: str, start_date: str, end_date: str):
        # 1. 加载市场数据（带前复权）
        df = self.market_repo.get_with_adjustment(
            start_date=start_date,
            end_date=end_date,
            adjust_type="forward"
        )

        # 2. 应用股票状态过滤
        df = self.market_repo.get_with_status(
            start_date=start_date,
            end_date=end_date,
            filter_st=True,
            filter_new_stock=True
        )

        # 3. 计算因子
        result = self.calculate_factor(df)

        # 4. 保存因子结果
        count = self.factor_repo.save_factor_results(
            factor_id=factor_id,
            data=result
        )

        return count


# ============================================================================
# 5. 自定义 Repository
# ============================================================================

from infrastructure.repository.base import BaseRepository

class CustomRepository(BaseRepository):
    """自定义 Repository 示例"""

    def __init__(self, db_client):
        super().__init__(db_client, "custom_table")

    def find_by_custom_condition(self, param1: str, param2: int):
        """自定义查询方法"""
        query = QueryBuilder(self.table_name) \
            .select_all() \
            .where("field1", "=", param1) \
            .where("field2", ">", param2) \
            .build()

        return self.db.execute(query.sql, query.params)

    def get_aggregated_data(self, start_date: str, end_date: str):
        """聚合查询示例"""
        # 注意: 聚合查询可能需要直接使用 DolphinDB 的 SQL
        sql = """
        SELECT ts_code, AVG(close) as avg_close, MAX(high) as max_high
        FROM custom_table
        WHERE trade_date >= %s AND trade_date <= %s
        GROUP BY ts_code
        """
        return self.db.execute(sql, (start_date, end_date))


# ============================================================================
# 6. 测试示例
# ============================================================================

def test_repository_pattern():
    """测试 Repository 模式"""
    from infrastructure.database.dolphindb_client import DolphinDBClient

    # 初始化客户端
    db_client = DolphinDBClient()

    # 创建 Repository
    market_repo = MarketDataRepository(db_client)
    factor_repo = FactorDataRepository(db_client)

    # 测试查询
    df = market_repo.find_by_date_range("20240101", "20240131")
    print(f"查询到 {len(df)} 条市场数据")

    # 测试因子查询
    factor_df = factor_repo.get_factor_values(
        factor_id="momentum_20",
        start_date="20240101",
        end_date="20240131"
    )
    print(f"查询到 {len(factor_df)} 条因子数据")

    # 测试质量统计
    stats = factor_repo.get_quality_stats("momentum_20", "20240101", "20240131")
    print(f"因子质量统计: {stats}")


if __name__ == "__main__":
    test_repository_pattern()
