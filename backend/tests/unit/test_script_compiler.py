"""脚本编译器单测"""
import pytest

from engine.script.compiler import compile_script


VALID_SCRIPT = """
def build_strategy():
    return {
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "capital": 1000000,
        "signals": [
            {"type": "indicator", "op": "sma", "params": {"window": 20}, "output_col": "sma20"},
            {"type": "condition", "expr": "close > sma20", "output_col": "signal"}
        ]
    }
"""


class TestCompileScript:
    """编译器核心逻辑"""

    def test_valid_script_compiles(self):
        result = compile_script(VALID_SCRIPT)
        assert result.success is True
        assert result.ir["source_type"] == "script"
        assert result.ir["data_source"]["ts_code"] == "000001.SZ"
        assert len(result.ir["pipeline"]) == 2
        assert result.script_hash

    def test_ir_contains_backtest_config(self):
        result = compile_script(VALID_SCRIPT)
        assert result.success
        assert result.ir["backtest_config"]["initial_capital"] == 1_000_000

    def test_ir_version(self):
        result = compile_script(VALID_SCRIPT)
        assert result.success
        assert "version" in result.ir

    def test_custom_entry_point(self):
        script = """
def my_strategy():
    return {
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "signals": [{"type": "condition", "expr": "close > 10", "output_col": "signal"}]
    }
"""
        result = compile_script(script, entry_point="my_strategy")
        assert result.success is True
        assert result.ir["entry_point"] == "my_strategy"

    def test_missing_entry_point_fails(self):
        script = """
def wrong_name():
    return {
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "signals": [{"type": "condition", "expr": "close > 10", "output_col": "signal"}]
    }
"""
        result = compile_script(script)
        assert result.success is False
        assert "build_strategy" in result.errors[0]

    def test_missing_required_fields(self):
        script = """
def build_strategy():
    return {"ts_code": "000001.SZ"}
"""
        result = compile_script(script)
        assert result.success is False
        assert "start_date" in result.errors[0] or "end_date" in result.errors[0]

    def test_invalid_ts_code(self):
        script = """
def build_strategy():
    return {
        "ts_code": "",
        "start_date": "20230101",
        "end_date": "20241231",
        "signals": [{"type": "condition", "expr": "close > 10", "output_col": "signal"}]
    }
"""
        result = compile_script(script)
        assert result.success is False
        assert "ts_code" in result.errors[0]

    def test_invalid_date_format(self):
        script = """
def build_strategy():
    return {
        "ts_code": "000001.SZ",
        "start_date": "2023-01-01",
        "end_date": "20241231",
        "signals": [{"type": "condition", "expr": "close > 10", "output_col": "signal"}]
    }
"""
        result = compile_script(script)
        assert result.success is False
        assert "start_date" in result.errors[0]

    def test_empty_signals(self):
        script = """
def build_strategy():
    return {
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "signals": []
    }
"""
        result = compile_script(script)
        assert result.success is False
        assert "signals" in result.errors[0]

    def test_unknown_operator_fails(self):
        script = """
def build_strategy():
    return {
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "signals": [{"type": "indicator", "op": "hacked_op", "params": {}, "output_col": "x"}]
    }
"""
        result = compile_script(script)
        assert result.success is False
        assert "未知算子" in result.errors[0]

    def test_unknown_signal_type_fails(self):
        script = """
def build_strategy():
    return {
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "signals": [{"type": "hack", "output_col": "x"}]
    }
"""
        result = compile_script(script)
        assert result.success is False
        assert "未知类型" in result.errors[0]

    def test_condition_injection_fails(self):
        script = """
def build_strategy():
    return {
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "signals": [{"type": "condition", "expr": "close > 10; import os", "output_col": "signal"}]
    }
"""
        result = compile_script(script)
        assert result.success is False
        assert "非法字符" in result.errors[0]

    def test_non_dict_return_fails(self):
        script = "def build_strategy():\n    return 'not a dict'"
        result = compile_script(script)
        assert result.success is False
        assert "dict" in result.errors[0]

    def test_all_operators_compile(self):
        ops = ["sma", "ema", "rsi", "macd", "bollinger", "rank", "zscore"]
        signals = ", ".join(
            f'{{"type": "indicator", "op": "{op}", "params": {{"window": 14}}, "output_col": "{op}_out"}}'
            for op in ops
        )
        script = f"""
def build_strategy():
    return {{
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "signals": [{signals}]
    }}
"""
        result = compile_script(script)
        assert result.success is True
        assert len(result.ir["pipeline"]) == len(ops)

    def test_runtime_error_in_entry_point(self):
        script = """
def build_strategy():
    raise ValueError("boom")
"""
        result = compile_script(script)
        assert result.success is False
        assert "boom" in result.errors[0]

    def test_script_hash_deterministic(self):
        r1 = compile_script(VALID_SCRIPT)
        r2 = compile_script(VALID_SCRIPT)
        assert r1.script_hash == r2.script_hash
