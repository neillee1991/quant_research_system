"""
测试移动平均线因子

验证迁移后的因子计算结果
"""
import pytest
import polars as pl
import numpy as np
from datetime import datetime, timedelta


class TestMAFactors:
    """测试移动平均线因子"""

    def test_ma_5_basic_calculation(self):
        """测试MA5基本计算逻辑"""
        # 构造测试数据：单只股票，30天数据
        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 30,
            "trade_date": [f"2024-01-{i:02d}" for i in range(1, 31)],
            "close": list(range(100, 130))  # 100, 101, 102, ..., 129
        })

        # 执行计算
        from factors_v2.ma_factors import compute_ma_5
        result = compute_ma_5(test_data, {"window": 5})

        # 验证结果
        assert not result.is_empty(), "结果不应为空"
        assert "factor_value" in result.columns, "应包含 factor_value 列"
        assert len(result) == 30, "行数应与输入一致"

        # 验证第5个值（前5个值的平均）
        # (100 + 101 + 102 + 103 + 104) / 5 = 102
        expected_ma5 = sum(range(100, 105)) / 5
        actual_ma5 = result.filter(pl.col("trade_date") == "2024-01-05")["factor_value"][0]
        assert abs(actual_ma5 - expected_ma5) < 1e-6, f"MA5计算错误: {actual_ma5} != {expected_ma5}"

    def test_ma_20_basic_calculation(self):
        """测试MA20基本计算逻辑"""
        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 30,
            "trade_date": [f"2024-01-{i:02d}" for i in range(1, 31)],
            "close": list(range(100, 130))
        })

        from factors_v2.ma_factors import compute_ma_20
        result = compute_ma_20(test_data, {"window": 20})

        assert not result.is_empty()
        assert len(result) == 30

        # 验证第20个值
        expected_ma20 = sum(range(100, 120)) / 20
        actual_ma20 = result.filter(pl.col("trade_date") == "2024-01-20")["factor_value"][0]
        assert abs(actual_ma20 - expected_ma20) < 1e-6

    def test_multiple_stocks(self):
        """测试多只股票"""
        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 10 + ["000002.SZ"] * 10,
            "trade_date": [f"2024-01-{i:02d}" for i in range(1, 11)] * 2,
            "close": list(range(100, 110)) + list(range(200, 210))
        })

        from factors_v2.ma_factors import compute_ma_5
        result = compute_ma_5(test_data, {"window": 5})

        # 验证两只股票都有结果
        assert len(result) == 20
        stock1_result = result.filter(pl.col("ts_code") == "000001.SZ")
        stock2_result = result.filter(pl.col("ts_code") == "000002.SZ")
        assert len(stock1_result) == 10
        assert len(stock2_result) == 10

        # 验证各自的MA5
        expected_ma5_stock1 = sum(range(100, 105)) / 5
        actual_ma5_stock1 = stock1_result.filter(pl.col("trade_date") == "2024-01-05")["factor_value"][0]
        assert abs(actual_ma5_stock1 - expected_ma5_stock1) < 1e-6

        expected_ma5_stock2 = sum(range(200, 205)) / 5
        actual_ma5_stock2 = stock2_result.filter(pl.col("trade_date") == "2024-01-05")["factor_value"][0]
        assert abs(actual_ma5_stock2 - expected_ma5_stock2) < 1e-6

    def test_null_handling(self):
        """测试空值处理"""
        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 10,
            "trade_date": [f"2024-01-{i:02d}" for i in range(1, 11)],
            "close": [100.0, 101.0, None, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0]
        })

        from factors_v2.ma_factors import compute_ma_5
        result = compute_ma_5(test_data, {"window": 5})

        # 应该能处理空值
        assert not result.is_empty()
        assert len(result) == 10

    def test_output_columns(self):
        """测试输出列"""
        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 10,
            "trade_date": [f"2024-01-{i:02d}" for i in range(1, 11)],
            "close": list(range(100, 110)),
            "open": list(range(99, 109)),  # 额外列
            "high": list(range(101, 111))  # 额外列
        })

        from factors_v2.ma_factors import compute_ma_5
        result = compute_ma_5(test_data, {"window": 5})

        # 应该只返回必需的列
        assert set(result.columns) == {"ts_code", "trade_date", "factor_value"}


class TestMAFactorsIntegration:
    """集成测试（需要数据库）"""

    @pytest.fixture
    def db_client(self):
        """数据库客户端"""
        from store.dolphindb_client import DolphinDBClient
        return DolphinDBClient.get_instance()

    @pytest.mark.integration
    def test_ma_20_with_new_service(self, db_client):
        """测试使用新服务计算MA20"""
        from services.factor_compute_service import FactorComputeService

        service = FactorComputeService(db_client)

        # 计算最近一个交易日的MA20
        result = service.compute_factor(
            factor_id="factor_ma_20",
            mode="incremental",
            save_results=False
        )

        assert result.success, f"计算失败: {result.message}"
        assert result.rows > 0, "应该有计算结果"

    @pytest.mark.integration
    @pytest.mark.slow
    def test_result_consistency_ma_20(self, db_client):
        """测试MA20结果一致性（新旧架构对比）"""
        from engine.production.engine import ProductionEngine
        from services.factor_compute_service import FactorComputeService

        old_engine = ProductionEngine(db_client)
        new_service = FactorComputeService(db_client)

        factor_id = "factor_ma_20"
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

        # 加载旧架构结果
        old_data = db_client.query(f"""
            SELECT ts_code, trade_date, factor_value
            FROM factor_values
            WHERE factor_id = '{factor_id}' AND trade_date = '{test_date}'
            ORDER BY ts_code
        """)

        # 获取新架构结果
        new_data = new_result.context.dataframe.select(["ts_code", "trade_date", "factor_value"])

        # 对比
        assert len(old_data) == len(new_data), f"行数不一致: {len(old_data)} != {len(new_data)}"

        merged = old_data.join(new_data, on=["ts_code", "trade_date"], suffix="_new")
        max_diff = (merged["factor_value"] - merged["factor_value_new"]).abs().max()
        assert max_diff < 1e-10, f"最大误差超过阈值: {max_diff}"

    @pytest.mark.integration
    def test_performance_ma_20(self, db_client):
        """测试MA20性能"""
        import time
        from services.factor_compute_service import FactorComputeService

        service = FactorComputeService(db_client)

        start = time.time()
        result = service.compute_factor(
            factor_id="factor_ma_20",
            mode="incremental",
            save_results=False
        )
        elapsed = time.time() - start

        assert result.success
        assert elapsed < 10.0, f"计算耗时过长: {elapsed:.2f}s"
        print(f"MA20 计算耗时: {elapsed:.2f}s, 行数: {result.rows}")
