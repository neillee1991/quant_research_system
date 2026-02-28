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
