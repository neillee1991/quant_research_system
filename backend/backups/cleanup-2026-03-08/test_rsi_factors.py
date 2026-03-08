"""
测试RSI因子

验证迁移后的因子计算结果
"""
import pytest
import polars as pl
import numpy as np


class TestRSIFactors:
    """测试RSI因子"""

    def test_rsi_14_basic_calculation(self):
        """测试RSI14基本计算逻辑"""
        # 构造测试数据：价格先涨后跌
        prices = [100, 102, 104, 103, 105, 107, 106, 108, 110, 109,
                  111, 113, 112, 114, 116, 115, 117, 119, 118, 120]

        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 20,
            "trade_date": [f"2024-01-{i:02d}" for i in range(1, 21)],
            "close": prices
        })

        # 执行计算
        from factors_v2.rsi_factors import compute_rsi_14
        result = compute_rsi_14(test_data, {"window": 14})

        # 验证结果
        assert not result.is_empty(), "结果不应为空"
        assert "factor_value" in result.columns, "应包含 factor_value 列"
        assert len(result) == 20, "行数应与输入一致"

        # RSI应该在0-100之间
        rsi_values = result["factor_value"].drop_nulls()
        assert all(rsi_values >= 0), "RSI不应小于0"
        assert all(rsi_values <= 100), "RSI不应大于100"

    def test_rsi_6_basic_calculation(self):
        """测试RSI6基本计算逻辑"""
        # 持续上涨的价格
        prices = list(range(100, 120))

        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 20,
            "trade_date": [f"2024-01-{i:02d}" for i in range(1, 21)],
            "close": prices
        })

        from factors_v2.rsi_factors import compute_rsi_6
        result = compute_rsi_6(test_data, {"window": 6})

        assert not result.is_empty()
        assert len(result) == 20

        # 持续上涨，RSI应该接近100
        rsi_values = result["factor_value"].drop_nulls()
        last_rsi = rsi_values[-1]
        assert last_rsi > 80, f"持续上涨的RSI应该较高: {last_rsi}"

    def test_rsi_downtrend(self):
        """测试下跌趋势的RSI"""
        # 持续下跌的价格
        prices = list(range(120, 100, -1))

        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 20,
            "trade_date": [f"2024-01-{i:02d}" for i in range(1, 21)],
            "close": prices
        })

        from factors_v2.rsi_factors import compute_rsi_14
        result = compute_rsi_14(test_data, {"window": 14})

        # 持续下跌，RSI应该接近0
        rsi_values = result["factor_value"].drop_nulls()
        last_rsi = rsi_values[-1]
        assert last_rsi < 20, f"持续下跌的RSI应该较低: {last_rsi}"

    def test_multiple_stocks(self):
        """测试多只股票"""
        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 20 + ["000002.SZ"] * 20,
            "trade_date": [f"2024-01-{i:02d}" for i in range(1, 21)] * 2,
            "close": list(range(100, 120)) + list(range(200, 180, -1))  # 一涨一跌
        })

        from factors_v2.rsi_factors import compute_rsi_14
        result = compute_rsi_14(test_data, {"window": 14})

        # 验证两只股票都有结果
        assert len(result) == 40
        stock1_result = result.filter(pl.col("ts_code") == "000001.SZ")
        stock2_result = result.filter(pl.col("ts_code") == "000002.SZ")
        assert len(stock1_result) == 20
        assert len(stock2_result) == 20

        # 验证上涨股票RSI较高
        stock1_rsi = stock1_result["factor_value"].drop_nulls()[-1]
        assert stock1_rsi > 70, "上涨股票RSI应该较高"

        # 验证下跌股票RSI较低
        stock2_rsi = stock2_result["factor_value"].drop_nulls()[-1]
        assert stock2_rsi < 30, "下跌股票RSI应该较低"

    def test_output_columns(self):
        """测试输出列"""
        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 20,
            "trade_date": [f"2024-01-{i:02d}" for i in range(1, 21)],
            "close": list(range(100, 120)),
            "open": list(range(99, 119)),  # 额外列
        })

        from factors_v2.rsi_factors import compute_rsi_14
        result = compute_rsi_14(test_data, {"window": 14})

        # 应该只返回必需的列
        assert set(result.columns) == {"ts_code", "trade_date", "factor_value"}


class TestRSIFactorsIntegration:
    """集成测试（需要数据库）"""

    @pytest.fixture
    def db_client(self):
        """数据库客户端"""
        from store.dolphindb_client import DolphinDBClient
        return DolphinDBClient.get_instance()

    @pytest.mark.integration
    def test_rsi_14_with_new_service(self, db_client):
        """测试使用新服务计算RSI14"""
        from services.factor_compute_service import FactorComputeService

        service = FactorComputeService(db_client)

        result = service.compute_factor(
            factor_id="factor_rsi_14",
            mode="incremental",
            save_results=False
        )

        assert result.success, f"计算失败: {result.message}"
        assert result.rows > 0, "应该有计算结果"

    @pytest.mark.integration
    @pytest.mark.slow
    def test_result_consistency_rsi_14(self, db_client):
        """测试RSI14结果一致性（新旧架构对比）"""
        from engine.production.engine import ProductionEngine
        from services.factor_compute_service import FactorComputeService

        old_engine = ProductionEngine(db_client)
        new_service = FactorComputeService(db_client)

        factor_id = "factor_rsi_14"
        test_date = "2024-01-15"

        # 旧架构计算
        old_success = old_engine.run_task(
            factor_id=factor_id,
            target_date=test_date,
            mode="incremental"
        )
        assert old_success, "旧架构计算失败"

        # 新架构计算
        new_result = new_service.compute_factor(
            factor_id=factor_id,
            target_date=test_date,
            mode="incremental",
            save_results=False
        )
        assert new_result.success, f"新架构计算失败: {new_result.message}"

        # 加载并对比结果
        old_data = db_client.query(f"""
            SELECT ts_code, trade_date, factor_value
            FROM factor_values
            WHERE factor_id = '{factor_id}' AND trade_date = '{test_date}'
            ORDER BY ts_code
        """)

        new_data = new_result.context.dataframe.select(["ts_code", "trade_date", "factor_value"])

        assert len(old_data) == len(new_data), f"行数不一致: {len(old_data)} != {len(new_data)}"

        merged = old_data.join(new_data, on=["ts_code", "trade_date"], suffix="_new")
        max_diff = (merged["factor_value"] - merged["factor_value_new"]).abs().max()
        assert max_diff < 1e-10, f"最大误差超过阈值: {max_diff}"
