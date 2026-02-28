"""FlowParser 拓扑排序和信号解析的单元测试"""
import polars as pl
import pytest
from engine.parser.flow_parser import FlowParser


@pytest.fixture
def dummy_loader():
    """返回模拟数据的 loader"""
    def loader(ts_code, start, end):
        return pl.DataFrame({
            "trade_date": [f"2024010{i}" for i in range(1, 6)],
            "ts_code": [ts_code] * 5,
            "open": [10.0, 11.0, 12.0, 11.0, 13.0],
            "high": [11.0, 12.0, 13.0, 12.0, 14.0],
            "low": [9.0, 10.0, 11.0, 10.0, 12.0],
            "close": [10.5, 11.5, 12.5, 11.0, 13.5],
        })
    return loader


class TestTopoSort:
    def test_linear_chain(self, dummy_loader):
        parser = FlowParser(dummy_loader)
        nodes = {"1": {}, "2": {}, "3": {}}
        edges = [
            {"source": "1", "target": "2"},
            {"source": "2", "target": "3"},
        ]
        order = parser._topo_sort(nodes, edges)
        assert order == ["1", "2", "3"]

    def test_diamond(self, dummy_loader):
        parser = FlowParser(dummy_loader)
        nodes = {"1": {}, "2": {}, "3": {}, "4": {}}
        edges = [
            {"source": "1", "target": "2"},
            {"source": "1", "target": "3"},
            {"source": "2", "target": "4"},
            {"source": "3", "target": "4"},
        ]
        order = parser._topo_sort(nodes, edges)
        assert order[0] == "1"
        assert order[-1] == "4"
        assert set(order) == {"1", "2", "3", "4"}

    def test_single_node(self, dummy_loader):
        parser = FlowParser(dummy_loader)
        nodes = {"1": {}}
        edges = []
        order = parser._topo_sort(nodes, edges)
        assert order == ["1"]


class TestApplySignal:
    def test_simple_greater_than(self, dummy_loader):
        parser = FlowParser(dummy_loader)
        df = pl.DataFrame({
            "close": [10.0, 20.0, 30.0],
            "sma": [15.0, 15.0, 15.0],
        })
        result = parser._apply_signal(df, {"condition": "close > sma"}, "signal")
        signals = result["signal"].to_list()
        assert signals == [0, 1, 1]

    def test_less_than_numeric(self, dummy_loader):
        parser = FlowParser(dummy_loader)
        df = pl.DataFrame({"rsi": [20.0, 50.0, 80.0]})
        result = parser._apply_signal(df, {"condition": "rsi < 30"}, "signal")
        signals = result["signal"].to_list()
        assert signals == [1, 0, 0]

    def test_empty_condition(self, dummy_loader):
        parser = FlowParser(dummy_loader)
        df = pl.DataFrame({"close": [10.0, 20.0]})
        result = parser._apply_signal(df, {"condition": ""}, "signal")
        assert result["signal"].to_list() == [1, 1]


class TestApplyOperator:
    def test_sma_operator(self, dummy_loader):
        parser = FlowParser(dummy_loader)
        df = pl.DataFrame({"close": [10.0, 11.0, 12.0, 13.0, 14.0]})
        result = parser._apply_operator(df, {"op": "sma", "window": 3, "output_col": "sma3"})
        assert "sma3" in result.columns
        assert len(result) == 5

    def test_rsi_operator(self, dummy_loader):
        parser = FlowParser(dummy_loader)
        df = pl.DataFrame({"close": [10.0, 11.0, 12.0, 11.0, 13.0, 12.0, 14.0]})
        result = parser._apply_operator(df, {"op": "rsi", "window": 5, "output_col": "rsi5"})
        assert "rsi5" in result.columns

    def test_macd_operator(self, dummy_loader):
        parser = FlowParser(dummy_loader)
        df = pl.DataFrame({"close": [float(i) for i in range(30)]})
        result = parser._apply_operator(df, {"op": "macd", "output_col": "m"})
        assert "m_macd" in result.columns
        assert "m_signal" in result.columns
        assert "m_hist" in result.columns

    def test_unknown_operator(self, dummy_loader):
        parser = FlowParser(dummy_loader)
        df = pl.DataFrame({"close": [10.0, 11.0]})
        result = parser._apply_operator(df, {"op": "unknown_op"})
        # 未知算子不应修改 DataFrame 列
        assert result.columns == ["close"]
