"""
Provider 接口抽象 - 提供回测所需的各种数据源接口
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd


class ISuspensionProvider(ABC):
    """
    停牌信息Provider接口
    提供股票停牌信息查询服务
    """

    @abstractmethod
    def is_suspended(self, symbol: str, date: pd.Timestamp) -> bool:
        """
        检查指定日期某股票是否停牌

        Args:
            symbol: 股票代码
            date: 查询日期

        Returns:
            True表示停牌，False表示正常交易
        """
        pass

    @abstractmethod
    def get_suspended_symbols(self, date: pd.Timestamp) -> Set[str]:
        """
        获取指定日期所有停牌的股票

        Args:
            date: 查询日期

        Returns:
            停牌股票代码集合
        """
        pass

    @abstractmethod
    def get_suspension_periods(
        self,
        symbol: str
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        """
        获取某股票的所有停牌时间段

        Args:
            symbol: 股票代码

        Returns:
            停牌时间段列表，每个元素为(开始日期, 结束日期)
        """
        pass

    @abstractmethod
    def is_trading_day(
        self,
        symbol: str,
        date: pd.Timestamp
    ) -> bool:
        """
        检查指定日期是否为某股票的交易日（非停牌且在交易时间范围内）

        Args:
            symbol: 股票代码
            date: 查询日期

        Returns:
            True表示可交易，False表示不可交易
        """
        pass


class IAdjustmentProvider(ABC):
    """
    复权因子Provider接口
    提供股票复权因子查询服务
    """

    @abstractmethod
    def get_adjustment_factor(
        self,
        symbol: str,
        date: pd.Timestamp,
        adjustment_type: str = "forward"
    ) -> float:
        """
        获取指定日期的复权因子

        Args:
            symbol: 股票代码
            date: 查询日期
            adjustment_type: 复权类型，'forward'表示前复权，'backward'表示后复权

        Returns:
            复权因子
        """
        pass

    @abstractmethod
    def get_adjustment_factors(
        self,
        symbol: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        adjustment_type: str = "forward"
    ) -> pd.Series:
        """
        获取指定时间范围内的复权因子序列

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            adjustment_type: 复权类型

        Returns:
            复权因子序列，索引为日期
        """
        pass

    @abstractmethod
    def get_adjusted_price(
        self,
        symbol: str,
        date: pd.Timestamp,
        price_type: str = "close",
        adjustment_type: str = "forward"
    ) -> float:
        """
        获取复权后的价格

        Args:
            symbol: 股票代码
            date: 查询日期
            price_type: 价格类型，'open'、'high'、'low'、'close'
            adjustment_type: 复权类型

        Returns:
            复权后的价格
        """
        pass

    @abstractmethod
    def get_dividends(
        self,
        symbol: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp
    ) -> pd.DataFrame:
        """
        获取指定时间范围内的分红信息

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            分红信息DataFrame
        """
        pass

    @abstractmethod
    def get_splits(
        self,
        symbol: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp
    ) -> pd.DataFrame:
        """
        获取指定时间范围内的拆股信息

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            拆股信息DataFrame
        """
        pass


class ILimitPriceProvider(ABC):
    """
    涨跌停价格Provider接口
    提供股票涨跌停价格查询服务
    """

    @abstractmethod
    def get_limit_prices(
        self,
        symbol: str,
        date: pd.Timestamp
    ) -> Tuple[float, float]:
        """
        获取指定日期的涨跌停价格

        Args:
            symbol: 股票代码
            date: 查询日期

        Returns:
            (跌停价, 涨停价)
        """
        pass

    @abstractmethod
    def get_limit_up_price(
        self,
        symbol: str,
        date: pd.Timestamp
    ) -> float:
        """
        获取指定日期的涨停价格

        Args:
            symbol: 股票代码
            date: 查询日期

        Returns:
            涨停价
        """
        pass

    @abstractmethod
    def get_limit_down_price(
        self,
        symbol: str,
        date: pd.Timestamp
    ) -> float:
        """
        获取指定日期的跌停价格

        Args:
            symbol: 股票代码
            date: 查询日期

        Returns:
            跌停价
        """
        pass

    @abstractmethod
    def get_limit_prices_batch(
        self,
        symbols: List[str],
        date: pd.Timestamp
    ) -> Dict[str, Tuple[float, float]]:
        """
        批量获取指定日期多个股票的涨跌停价格

        Args:
            symbols: 股票代码列表
            date: 查询日期

        Returns:
            {symbol: (跌停价, 涨停价)}
        """
        pass

    @abstractmethod
    def is_limit_up(
        self,
        symbol: str,
        date: pd.Timestamp,
        price: Optional[float] = None
    ) -> bool:
        """
        检查指定日期的价格是否触及涨停

        Args:
            symbol: 股票代码
            date: 查询日期
            price: 检查的价格，如果为None则使用收盘价

        Returns:
            True表示触及涨停
        """
        pass

    @abstractmethod
    def is_limit_down(
        self,
        symbol: str,
        date: pd.Timestamp,
        price: Optional[float] = None
    ) -> bool:
        """
        检查指定日期的价格是否触及跌停

        Args:
            symbol: 股票代码
            date: 查询日期
            price: 检查的价格，如果为None则使用收盘价

        Returns:
            True表示触及跌停
        """
        pass


class ICalendarProvider(ABC):
    """
    交易日历Provider接口
    提供交易日历查询服务
    """

    @abstractmethod
    def is_trading_calendar_day(
        self,
        date: pd.Timestamp,
        exchange: str = "SSE"
    ) -> bool:
        """
        检查指定日期是否为交易日（不考虑个股停牌）

        Args:
            date: 查询日期
            exchange: 交易所代码

        Returns:
            True表示是交易日
        """
        pass

    @abstractmethod
    def get_trading_days(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        exchange: str = "SSE"
    ) -> List[pd.Timestamp]:
        """
        获取指定时间范围内的所有交易日

        Args:
            start_date: 开始日期
            end_date: 结束日期
            exchange: 交易所代码

        Returns:
            交易日列表
        """
        pass

    @abstractmethod
    def next_trading_day(
        self,
        date: pd.Timestamp,
        n: int = 1,
        exchange: str = "SSE"
    ) -> pd.Timestamp:
        """
        获取指定日期之后的第n个交易日

        Args:
            date: 基准日期
            n: 天数，正数表示未来，负数表示过去
            exchange: 交易所代码

        Returns:
            交易日
        """
        pass

    @abstractmethod
    def prev_trading_day(
        self,
        date: pd.Timestamp,
        n: int = 1,
        exchange: str = "SSE"
    ) -> pd.Timestamp:
        """
        获取指定日期之前的第n个交易日

        Args:
            date: 基准日期
            n: 天数
            exchange: 交易所代码

        Returns:
            交易日
        """
        pass

    @abstractmethod
    def trading_days_between(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        exchange: str = "SSE"
    ) -> int:
        """
        计算两个日期之间的交易日数量

        Args:
            start_date: 开始日期
            end_date: 结束日期
            exchange: 交易所代码

        Returns:
            交易日数量
        """
        pass


class IMarketDataProvider(ABC):
    """
    市场数据Provider接口
    提供回测所需的市场数据查询服务
    """

    @abstractmethod
    def get_bar(
        self,
        symbol: str,
        date: pd.Timestamp,
        fields: Optional[List[str]] = None
    ) -> Optional[pd.Series]:
        """
        获取指定日期的K线数据

        Args:
            symbol: 股票代码
            date: 查询日期
            fields: 需要的字段列表，如果为None则返回所有字段

        Returns:
            K线数据
        """
        pass

    @abstractmethod
    def get_bars(
        self,
        symbol: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        获取指定时间范围内的K线数据

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            fields: 需要的字段列表

        Returns:
            K线数据DataFrame
        """
        pass

    @abstractmethod
    def get_bars_batch(
        self,
        symbols: List[str],
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        fields: Optional[List[str]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        批量获取多个股票的K线数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            fields: 需要的字段列表

        Returns:
            {symbol: K线数据DataFrame}
        """
        pass

    @abstractmethod
    def get_current_price(
        self,
        symbol: str,
        date: pd.Timestamp
    ) -> float:
        """
        获取指定日期的当前价格（用于交易）

        Args:
            symbol: 股票代码
            date: 查询日期

        Returns:
            价格
        """
        pass

    @abstractmethod
    def get_universe(
        self,
        date: pd.Timestamp,
        universe_type: str = "all"
    ) -> List[str]:
        """
        获取指定日期的股票池

        Args:
            date: 查询日期
            universe_type: 股票池类型

        Returns:
            股票代码列表
        """
        pass
