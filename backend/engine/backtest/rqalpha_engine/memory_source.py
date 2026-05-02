"""
RQAlpha 内存数据源 - 继承 AbstractDataSource，挂载预载内存数据
"""
from rqalpha.data import AbstractDataSource
import numpy as np


class RQAlphaMemoryDataSource(AbstractDataSource):
    """RQAlpha 内存数据源 - 正确的 Numpy 格式"""

    def __init__(self, wide_table=None):
        self.wide_table = wide_table
        self._data_cache = {}

        if wide_table is not None:
            self._preprocess_data(wide_table)

    def _preprocess_data(self, wide_table):
        """预处理数据，转换为 RQAlpha 兼容格式"""
        import numpy as np
        import polars as pl

        # 按股票代码分组
        unique_stocks = wide_table.select('ts_code').unique()

        for stock in unique_stocks:
            stock_filter = wide_table.filter(pl.col('ts_code') == stock['ts_code'])

            # 显式定义 RQAlpha 需要的 dtype
            dt = np.dtype([
                ('datetime', 'uint64'),
                ('open', 'f8'),
                ('high', 'f8'),
                ('low', 'f8'),
                ('close', 'f8'),
                ('volume', 'f8'),
                ('total_turnover', 'f8'),
                ('limit_up', 'f8'),
                ('limit_down', 'f8'),
            ])

            n = len(stock_filter)
            stock_data = np.zeros(n, dtype=dt)

            dates = stock_filter['trade_date'].cast(pl.Utf8).to_numpy()
            stock_data['datetime'] = dates.astype(np.uint64) * 1000000

            stock_data['open'] = stock_filter['open'].to_numpy().astype(np.float64)
            stock_data['high'] = stock_filter['high'].to_numpy().astype(np.float64)
            stock_data['low'] = stock_filter['low'].to_numpy().astype(np.float64)
            stock_data['close'] = stock_filter['close'].to_numpy().astype(np.float64)
            stock_data['volume'] = stock_filter['volume'].to_numpy().astype(np.float64)
            stock_data['total_turnover'] = stock_filter['amount'].to_numpy().astype(np.float64) if 'amount' in stock_filter.columns else 0

            if 'limit_up' in stock_filter.columns:
                stock_data['limit_up'] = stock_filter['limit_up'].to_numpy().astype(np.float64)
            if 'limit_down' in stock_filter.columns:
                stock_data['limit_down'] = stock_filter['limit_down'].to_numpy().astype(np.float64)

            self._data_cache[stock['ts_code']] = stock_data.view(np.recarray)
            del stock_filter

    def get_bars(self, order_book_id, start_date, end_date, frequency):
        """获取行情数据"""
        if order_book_id not in self._data_cache:
            return []

        stock_data = self._data_cache.get(order_book_id, [])

        mask = (
            (stock_data['trade_date'] >= start_date) &
            (stock_data['trade_date'] <= end_date)
        )

        return stock_data[mask]
