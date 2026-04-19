"""脚本执行器单测

使用 mock 数据加载器，验证 IR -> signals -> 回测的完整执行链路。
"""
import pytest
import polars as pl
import numpy as np

from engine.script.executor import execute_ir, ExecutionError


def _make_price_df(n: int = 100) -> pl.DataFrame:
    """生成模拟行情数据"""
    np.random.seed(42)
    base = np.datetime64("2024-01-01")
    dates = [str(np.datetime64(base, "D") + np.timedelta64(i, "D"))[:10].replace("-", "") for i in range(n)]
    close = (100.0 + np.cumsum(np.random.randn(n) * 0.5)).tolist()
    high = [c + abs(np.random.randn()) * 0.3 for c in close]
    low = [c - abs(np.random.randn()) * 0.3 for c in close]
    return pl.DataFrame({
        "trade_date": dates,
        "ts_code": ["000001.SZ"] * n,
        "close": close,
        "high": high,
        "low": low,
    })


def _mock_loader(ts_code: str, start: str, end: str) -> pl.DataFrame:
    return _make_price_df()


class TestExecuteIR:
    """执行器核心逻辑"""

    def test_simple_condition_execution(self):
        ir = {
            "data_source": {"ts_code": "000001.SZ", "start_date": "20240101", "end_date": "20240430"},
            "pipeline": [
                {"type": "condition", "expr": "close > 100", "output_col": "signal"},
            ],
            "backtest_config": {"initial_capital": 1_000_000},
        }
        result = execute_ir(ir, _mock_loader)
        assert "metrics" in result
        assert "equity_curve" in result
        assert "trades_sample" in result
        assert isinstance(result["metrics"], dict)

    def test_indicator_then_condition(self):
        ir = {
            "data_source": {"ts_code": "000001.SZ", "start_date": "20240101", "end_date": "20240430"},
            "pipeline": [
                {"type": "indicator", "op": "sma", "params": {"window": 10}, "output_col": "sma10"},
                {"type": "condition", "expr": "close > sma10", "output_col": "signal"},
            ],
            "backtest_config": {},
        }
        result = execute_ir(ir, _mock_loader)
        assert "metrics" in result
        assert len(result["equity_curve"]) > 0

    def test_multiple_indicators(self):
        ir = {
            "data_source": {"ts_code": "000001.SZ", "start_date": "20240101", "end_date": "20240430"},
            "pipeline": [
                {"type": "indicator", "op": "sma", "params": {"window": 10}, "output_col": "sma10"},
                {"type": "indicator", "op": "ema", "params": {"window": 5}, "output_col": "ema5"},
                {"type": "condition", "expr": "sma10 > ema5", "output_col": "signal"},
            ],
            "backtest_config": {},
        }
        result = execute_ir(ir, _mock_loader)
        assert "metrics" in result

    def test_rsi_indicator(self):
        ir = {
            "data_source": {"ts_code": "000001.SZ", "start_date": "20240101", "end_date": "20240430"},
            "pipeline": [
                {"type": "indicator", "op": "rsi", "params": {"window": 14}, "output_col": "rsi14"},
                {"type": "condition", "expr": "rsi14 < 70", "output_col": "signal"},
            ],
            "backtest_config": {},
        }
        result = execute_ir(ir, _mock_loader)
        assert "metrics" in result

    def test_no_condition_defaults_signal_1(self):
        ir = {
            "data_source": {"ts_code": "000001.SZ", "start_date": "20240101", "end_date": "20240430"},
            "pipeline": [
                {"type": "indicator", "op": "sma", "params": {"window": 10}, "output_col": "sma10"},
            ],
            "backtest_config": {},
        }
        result = execute_ir(ir, _mock_loader)
        assert "metrics" in result

    def test_combined_and_condition(self):
        ir = {
            "data_source": {"ts_code": "000001.SZ", "start_date": "20240101", "end_date": "20240430"},
            "pipeline": [
                {"type": "indicator", "op": "sma", "params": {"window": 10}, "output_col": "sma10"},
                {"type": "indicator", "op": "rsi", "params": {"window": 14}, "output_col": "rsi14"},
                {"type": "condition", "expr": "close > sma10 and rsi14 < 70", "output_col": "signal"},
            ],
            "backtest_config": {},
        }
        result = execute_ir(ir, _mock_loader)
        assert "metrics" in result

    def test_no_data_raises(self):
        def empty_loader(*args):
            return pl.DataFrame()

        ir = {
            "data_source": {"ts_code": "999999.SZ", "start_date": "20240101", "end_date": "20240430"},
            "pipeline": [
                {"type": "condition", "expr": "close > 100", "output_col": "signal"},
            ],
            "backtest_config": {},
        }
        with pytest.raises(ExecutionError, match="无数据"):
            execute_ir(ir, empty_loader)

    def test_unknown_operator_raises(self):
        ir = {
            "data_source": {"ts_code": "000001.SZ", "start_date": "20240101", "end_date": "20240430"},
            "pipeline": [
                {"type": "indicator", "op": "nonexistent", "params": {}, "output_col": "x"},
            ],
            "backtest_config": {},
        }
        with pytest.raises(ExecutionError, match="未知算子"):
            execute_ir(ir, _mock_loader)

    def test_custom_backtest_config(self):
        ir = {
            "data_source": {"ts_code": "000001.SZ", "start_date": "20240101", "end_date": "20240430"},
            "pipeline": [
                {"type": "condition", "expr": "close > 100", "output_col": "signal"},
            ],
            "backtest_config": {
                "initial_capital": 500_000,
                "commission_rate": 0.001,
                "slippage_rate": 0.0005,
            },
        }
        result = execute_ir(ir, _mock_loader)
        assert "metrics" in result

    def test_macd_indicator(self):
        ir = {
            "data_source": {"ts_code": "000001.SZ", "start_date": "20240101", "end_date": "20240430"},
            "pipeline": [
                {"type": "indicator", "op": "macd", "params": {"fast": 12, "slow": 26, "signal": 9}, "output_col": "macd"},
                {"type": "condition", "expr": "macd_hist > 0", "output_col": "signal"},
            ],
            "backtest_config": {},
        }
        result = execute_ir(ir, _mock_loader)
        assert "metrics" in result
