"""技术因子计算的单元测试"""
import polars as pl
import pytest
from engine.factors.technical import TechnicalFactors, CrossSectionalFactors


@pytest.fixture
def sample_close():
    """模拟收盘价序列"""
    return pl.Series("close", [10.0, 11.0, 12.0, 11.5, 13.0, 12.5, 14.0, 13.5, 15.0, 14.5])


@pytest.fixture
def sample_ohlc():
    """模拟 OHLC 数据"""
    return {
        "high": pl.Series("high", [11.0, 12.0, 13.0, 12.5, 14.0, 13.5, 15.0, 14.5, 16.0, 15.5]),
        "low": pl.Series("low", [9.0, 10.0, 11.0, 10.5, 12.0, 11.5, 13.0, 12.5, 14.0, 13.5]),
        "close": pl.Series("close", [10.0, 11.0, 12.0, 11.5, 13.0, 12.5, 14.0, 13.5, 15.0, 14.5]),
    }


class TestSMA:
    def test_output_length(self, sample_close):
        result = TechnicalFactors.sma(sample_close, 3)
        assert len(result) == len(sample_close)

    def test_known_values(self):
        s = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = TechnicalFactors.sma(s, 3)
        # window=3: 第3个值应为 (1+2+3)/3 = 2.0
        assert result[2] == pytest.approx(2.0)
        # 第4个值: (2+3+4)/3 = 3.0
        assert result[3] == pytest.approx(3.0)

    def test_window_1(self, sample_close):
        result = TechnicalFactors.sma(sample_close, 1)
        for i in range(len(sample_close)):
            assert result[i] == pytest.approx(sample_close[i])


class TestEMA:
    def test_output_length(self, sample_close):
        result = TechnicalFactors.ema(sample_close, 3)
        assert len(result) == len(sample_close)

    def test_first_value(self, sample_close):
        result = TechnicalFactors.ema(sample_close, 3)
        # EMA 第一个值等于原始值
        assert result[0] == pytest.approx(sample_close[0])


class TestRSI:
    def test_output_length(self, sample_close):
        result = TechnicalFactors.rsi(sample_close, 5)
        assert len(result) == len(sample_close)

    def test_rsi_range(self, sample_close):
        result = TechnicalFactors.rsi(sample_close, 5)
        # RSI 应在 0-100 之间（跳过首个 null/nan）
        for val in result[1:]:
            assert 0 <= val <= 100

    def test_uptrend_rsi_high(self):
        """持续上涨，RSI 应接近 100"""
        s = pl.Series([float(i) for i in range(1, 21)])
        result = TechnicalFactors.rsi(s, 14)
        assert result[-1] > 90


class TestMACD:
    def test_output_tuple(self, sample_close):
        macd_line, signal_line, hist = TechnicalFactors.macd(sample_close)
        assert len(macd_line) == len(sample_close)
        assert len(signal_line) == len(sample_close)
        assert len(hist) == len(sample_close)

    def test_histogram_equals_diff(self, sample_close):
        macd_line, signal_line, hist = TechnicalFactors.macd(sample_close)
        for i in range(len(hist)):
            assert hist[i] == pytest.approx(macd_line[i] - signal_line[i], abs=1e-10)


class TestKDJ:
    def test_output_tuple(self, sample_ohlc):
        k, d, j = TechnicalFactors.kdj(
            sample_ohlc["high"], sample_ohlc["low"], sample_ohlc["close"]
        )
        assert len(k) == len(sample_ohlc["close"])
        assert len(d) == len(sample_ohlc["close"])
        assert len(j) == len(sample_ohlc["close"])

    def test_j_formula(self, sample_ohlc):
        k, d, j = TechnicalFactors.kdj(
            sample_ohlc["high"], sample_ohlc["low"], sample_ohlc["close"]
        )
        for i in range(len(j)):
            assert j[i] == pytest.approx(3 * k[i] - 2 * d[i], abs=1e-10)


class TestBollingerBands:
    def test_output_tuple(self, sample_close):
        upper, mid, lower = TechnicalFactors.bollinger_bands(sample_close, 5)
        assert len(upper) == len(sample_close)
        assert len(mid) == len(sample_close)
        assert len(lower) == len(sample_close)

    def test_upper_greater_than_lower(self, sample_close):
        upper, mid, lower = TechnicalFactors.bollinger_bands(sample_close, 5)
        # 从 window 大小之后，std > 0，upper > lower
        for i in range(4, len(sample_close)):
            assert upper[i] >= lower[i]


class TestATR:
    def test_output_length(self, sample_ohlc):
        result = TechnicalFactors.atr(
            sample_ohlc["high"], sample_ohlc["low"], sample_ohlc["close"], 5
        )
        assert len(result) == len(sample_ohlc["close"])

    def test_positive_values(self, sample_ohlc):
        result = TechnicalFactors.atr(
            sample_ohlc["high"], sample_ohlc["low"], sample_ohlc["close"], 5
        )
        for val in result:
            assert val >= 0


class TestRSIKnownValues:
    """RSI 计算结果与已知正确值对比（使用 EWM/Wilder 方法）"""

    def test_rsi_known_value_all_gains(self):
        """全部上涨时 RSI 应接近 100"""
        s = pl.Series([float(i) for i in range(1, 16)])
        result = TechnicalFactors.rsi(s, 14)
        assert result[-1] > 95, f"Expected RSI > 95 for all-gain series, got {result[-1]}"

    def test_rsi_known_value_all_losses(self):
        """全部下跌时 RSI 应接近 0"""
        s = pl.Series([float(15 - i) for i in range(15)])
        result = TechnicalFactors.rsi(s, 14)
        assert result[-1] < 5, f"Expected RSI < 5 for all-loss series, got {result[-1]}"

    def test_rsi_uses_ewm_not_sma(self):
        """验证 RSI 使用 EWM（Wilder 方法）而非 SMA。
        EWM 和 SMA 对同一序列会产生不同结果，此测试确认实现使用 EWM。
        """
        # 构造一个有明显涨跌的序列
        prices = pl.Series([10.0, 11.0, 10.5, 12.0, 11.0, 13.0, 12.0, 14.0,
                            13.0, 15.0, 14.0, 16.0, 15.0, 17.0, 16.0])
        delta = prices.diff()
        gain = delta.clip(lower_bound=0)
        loss = (-delta).clip(lower_bound=0)

        # EWM 方式（当前实现）
        avg_gain_ewm = gain.ewm_mean(span=14, adjust=False)
        avg_loss_ewm = loss.ewm_mean(span=14, adjust=False)

        # SMA 方式（错误实现）
        avg_gain_sma = gain.rolling_mean(window_size=14, min_periods=1)
        avg_loss_sma = loss.rolling_mean(window_size=14, min_periods=1)

        # 两种方式结果不同，说明实现方式有区别
        ewm_last = float(avg_gain_ewm[-1])
        sma_last = float(avg_gain_sma[-1])
        assert abs(ewm_last - sma_last) > 1e-6, "EWM and SMA should produce different results"

        # 验证当前实现使用 EWM
        result = TechnicalFactors.rsi(prices, 14)
        safe_loss = avg_loss_ewm.fill_nan(0.0) + 1e-10
        rs = avg_gain_ewm / safe_loss
        expected = 100 - (100 / (1 + rs))
        assert result[-1] == pytest.approx(float(expected[-1]), abs=1e-6)


class TestATRKnownValues:
    """ATR 计算结果验证"""

    def test_atr_first_bar_equals_hl(self):
        """第一根 K 线的 TR = High - Low（无前收盘价）"""
        high = pl.Series([12.0, 13.0, 14.0])
        low = pl.Series([10.0, 11.0, 12.0])
        close = pl.Series([11.0, 12.0, 13.0])
        result = TechnicalFactors.atr(high, low, close, window=3)
        # 第一根 TR = 12 - 10 = 2.0（fill_null 用自身）
        assert result[0] == pytest.approx(2.0, abs=1e-6)

    def test_atr_true_range_uses_prev_close(self):
        """TR 应考虑前收盘价（gap 情形）"""
        # 跳空高开：high=20, low=18, prev_close=10 → TR = max(2, 10, 8) = 10
        high = pl.Series([10.0, 20.0])
        low = pl.Series([9.0, 18.0])
        close = pl.Series([10.0, 19.0])
        result = TechnicalFactors.atr(high, low, close, window=2)
        # 第二根 TR = max(20-18, |20-10|, |18-10|) = max(2, 10, 8) = 10
        # EWM span=2 第二个值 = 2 * (1/(2+1)) + prev * (1 - 1/(2+1))
        # prev ATR (first bar) = 10-9 = 1.0
        # ewm: alpha = 2/(2+1) = 0.667
        # atr[1] = 10 * 0.667 + 1.0 * 0.333 ≈ 7.0
        assert result[1] == pytest.approx(10 * (2/3) + 1.0 * (1/3), abs=1e-4)

    def test_atr_non_negative(self):
        """ATR 所有值应 >= 0"""
        high = pl.Series([11.0, 12.0, 13.0, 12.5, 14.0])
        low = pl.Series([9.0, 10.0, 11.0, 10.5, 12.0])
        close = pl.Series([10.0, 11.0, 12.0, 11.5, 13.0])
        result = TechnicalFactors.atr(high, low, close, window=3)
        for val in result:
            assert val >= 0


class TestBollingerBandsKnownValues:
    """Bollinger Bands 计算验证"""

    def test_mid_equals_sma(self):
        """中轨应等于 SMA"""
        s = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
        upper, mid, lower = TechnicalFactors.bollinger_bands(s, window=5, num_std=2.0)
        sma = TechnicalFactors.sma(s, 5)
        for i in range(len(s)):
            assert mid[i] == pytest.approx(sma[i], abs=1e-10)

    def test_band_width_equals_2std(self):
        """上下轨距中轨应为 2 * std"""
        s = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
        upper, mid, lower = TechnicalFactors.bollinger_bands(s, window=5, num_std=2.0)
        std = TechnicalFactors.rolling_std(s, 5)
        for i in range(4, len(s)):  # skip first 4 where std may be 0
            assert upper[i] == pytest.approx(mid[i] + 2.0 * std[i], abs=1e-10)
            assert lower[i] == pytest.approx(mid[i] - 2.0 * std[i], abs=1e-10)

    def test_price_within_bands_for_normal_dist(self):
        """对于正态分布数据，大多数价格应在 2-sigma 带内"""
        import random
        random.seed(42)
        prices = pl.Series([100.0 + random.gauss(0, 1) for _ in range(100)])
        upper, mid, lower = TechnicalFactors.bollinger_bands(prices, window=20, num_std=2.0)
        # 从第 20 个点开始检查
        within = sum(1 for i in range(20, 100) if lower[i] <= prices[i] <= upper[i])
        assert within / 80 > 0.90  # 至少 90% 在带内


class TestCrossSectionalFactors:
    @pytest.fixture
    def cross_df(self):
        return pl.DataFrame({
            "trade_date": ["20240101"] * 3 + ["20240102"] * 3,
            "ts_code": ["A", "B", "C", "A", "B", "C"],
            "close": [10.0, 20.0, 30.0, 15.0, 25.0, 5.0],
            "industry": ["tech", "tech", "fin", "tech", "tech", "fin"],
        })

    def test_rank(self, cross_df):
        result = CrossSectionalFactors.rank(cross_df, "close")
        assert "close_rank" in result.columns

    def test_zscore(self, cross_df):
        result = CrossSectionalFactors.zscore(cross_df, "close")
        assert "close_zscore" in result.columns

    def test_neutralize(self, cross_df):
        result = CrossSectionalFactors.neutralize(cross_df, "close", "industry")
        assert "close_neutral" in result.columns
