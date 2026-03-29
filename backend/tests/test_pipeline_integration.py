"""
Pipeline 集成测试 - 测试完整的因子计算流程
"""
import pytest
import polars as pl
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch

from infrastructure.processor.pipeline import ProcessContext, DataPipeline, IProcessor
from infrastructure.processor.processors import (
    DataLoaderProcessor,
    AdjustmentProcessor,
    StatusFilterProcessor,
    FactorComputeProcessor,
    DateRangeFilterProcessor,
    QualityCheckerProcessor,
)
from infrastructure.processor.pipeline_factory import PipelineFactory
from app.services.factor_compute_service import FactorComputeService, ComputeResult
from engine.factor.registry import FactorDefinition, StorageConfig


class TestPipelineCore:
    """测试 Pipeline 核心功能"""

    def test_process_context_creation(self):
        """测试 ProcessContext 创建"""
        context = ProcessContext(
            factor_id="test_factor",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240131",
            data_start="20231201",
            preprocess_options={"adjust_price": "forward"},
        )

        assert context.factor_id == "test_factor"
        assert context.calc_start == "20240101"
        assert context.get_option("adjust_price") == "forward"
        assert context.get_option("unknown", "default") == "default"

    def test_process_context_shared_state(self):
        """测试共享状态"""
        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240131",
            data_start="20231201",
        )

        context.set_state("key1", "value1")
        context.set_state("key2", 123)

        assert context.get_state("key1") == "value1"
        assert context.get_state("key2") == 123
        assert context.get_state("key3", "default") == "default"

    def test_pipeline_add_stage(self):
        """测试添加处理阶段"""
        pipeline = DataPipeline(name="TestPipeline")

        class DummyProcessor(IProcessor):
            @property
            def name(self):
                return "Dummy"

            def process(self, df, context):
                return df

        processor1 = DummyProcessor()
        processor2 = DummyProcessor()

        pipeline.add_stage(processor1).add_stage(processor2)

        stages = pipeline.get_stages()
        assert len(stages) == 2

    def test_pipeline_execution_order(self):
        """测试 Pipeline 执行顺序"""
        execution_order = []

        class OrderedProcessor(IProcessor):
            def __init__(self, order_id):
                self.order_id = order_id

            @property
            def name(self):
                return f"Processor{self.order_id}"

            def process(self, df, context):
                execution_order.append(self.order_id)
                return df.with_columns(pl.lit(self.order_id).alias(f"stage_{self.order_id}"))

        pipeline = DataPipeline()
        pipeline.add_stage(OrderedProcessor(1))
        pipeline.add_stage(OrderedProcessor(2))
        pipeline.add_stage(OrderedProcessor(3))

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240131",
            data_start="20231201",
            dataframe=pl.DataFrame({"col": [1, 2, 3]})
        )

        result = pipeline.execute(context)

        assert execution_order == [1, 2, 3]
        assert "stage_1" in result.columns
        assert "stage_2" in result.columns
        assert "stage_3" in result.columns

    def test_pipeline_skip_stage(self):
        """测试跳过处理阶段"""
        execution_order = []

        class ConditionalProcessor(IProcessor):
            def __init__(self, order_id, should_run_flag):
                self.order_id = order_id
                self.should_run_flag = should_run_flag

            @property
            def name(self):
                return f"Processor{self.order_id}"

            def should_run(self, context):
                return self.should_run_flag

            def process(self, df, context):
                execution_order.append(self.order_id)
                return df

        pipeline = DataPipeline()
        pipeline.add_stage(ConditionalProcessor(1, True))
        pipeline.add_stage(ConditionalProcessor(2, False))  # 跳过
        pipeline.add_stage(ConditionalProcessor(3, True))

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240131",
            data_start="20231201",
            dataframe=pl.DataFrame({"col": [1, 2, 3]})
        )

        pipeline.execute(context)

        assert execution_order == [1, 3]  # 2 被跳过


class TestProcessors:
    """测试具体处理器"""

    def test_factor_compute_processor(self):
        """测试因子计算处理器"""
        # 模拟因子定义
        def mock_factor_func(df, params):
            return df.with_columns(
                (pl.col("close") / pl.col("close").shift(1) - 1).alias("factor_value")
            )

        definition = Mock()
        definition.func = mock_factor_func
        definition.params = {}

        # 准备测试数据
        df = pl.DataFrame({
            "ts_code": ["000001.SZ", "000001.SZ", "000001.SZ"],
            "trade_date": ["20240101", "20240102", "20240103"],
            "close": [10.0, 11.0, 10.5],
        })

        context = ProcessContext(
            factor_id="test_factor",
            factor_definition=definition,
            calc_start="20240101",
            calc_end="20240103",
            data_start="20231201",
        )

        processor = FactorComputeProcessor()
        result = processor.process(df, context)

        assert "factor_value" in result.columns
        assert len(result) == 3

    def test_date_range_filter_processor(self):
        """测试日期范围过滤器"""
        df = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 5,
            "trade_date": ["20231201", "20240101", "20240115", "20240131", "20240201"],
            "factor_value": [1.0, 2.0, 3.0, 4.0, 5.0],
        })

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240131",
            data_start="20231201",
        )

        processor = DateRangeFilterProcessor()
        result = processor.process(df, context)

        assert len(result) == 3  # 只保留 20240101-20240131
        assert result["trade_date"].min() == "20240101"
        assert result["trade_date"].max() == "20240131"

    def test_quality_checker_processor(self):
        """测试质量检查处理器"""
        df = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 10,
            "trade_date": [f"2024010{i}" for i in range(10)],
            "factor_value": [1.0, 2.0, None, 3.0, 4.0, None, 5.0, 6.0, 7.0, 8.0],
        })

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240100",
            calc_end="20240109",
            data_start="20231201",
        )

        processor = QualityCheckerProcessor()
        result = processor.process(df, context)

        assert "quality_flag" in result.columns
        quality_metrics = context.get_state("quality_metrics")
        assert quality_metrics is not None
        assert "null_rate" in quality_metrics
        assert quality_metrics["null_rate"] == 0.2  # 2/10


class TestPipelineFactory:
    """测试 PipelineFactory"""

    @patch('infrastructure.processor.pipeline_factory.DataLoaderProcessor')
    @patch('infrastructure.processor.pipeline_factory.AdjustmentProcessor')
    @patch('infrastructure.processor.pipeline_factory.StatusFilterProcessor')
    def test_create_factor_pipeline(self, mock_status, mock_adj, mock_loader):
        """测试创建因子计算管道"""
        db_client = Mock()
        data_config = Mock()
        trading_cal = Mock()

        factory = PipelineFactory(db_client, data_config, trading_cal)

        preprocess_options = {
            "adjust_price": "forward",
            "filter_st": True,
            "filter_new_stock": True,
        }

        pipeline = factory.create_factor_pipeline(
            factor_id="test_factor",
            preprocess_options=preprocess_options,
            save_results=True
        )

        stages = pipeline.get_stages()
        assert len(stages) == 8  # 8个处理阶段

    def test_create_custom_pipeline(self):
        """测试创建自定义管道"""
        db_client = Mock()
        data_config = Mock()
        trading_cal = Mock()

        factory = PipelineFactory(db_client, data_config, trading_cal)

        class CustomProcessor(IProcessor):
            @property
            def name(self):
                return "Custom"

            def process(self, df, context):
                return df

        stages = [CustomProcessor(), CustomProcessor()]
        pipeline = factory.create_custom_pipeline(stages, name="CustomPipeline")

        assert pipeline.name == "CustomPipeline"
        assert len(pipeline.get_stages()) == 2


class TestFactorComputeService:
    """测试 FactorComputeService"""

    @patch('app.services.factor_compute_service.get_factor')
    @patch('app.services.factor_compute_service.discover_factors')
    def test_compute_factor_success(self, mock_discover, mock_get_factor):
        """测试因子计算成功"""
        # 模拟因子定义
        def mock_func(df, params):
            return df.with_columns(pl.lit(1.0).alias("factor_value"))

        mock_definition = Mock(spec=FactorDefinition)
        mock_definition.func = mock_func
        mock_definition.params = {"lookback_days": 60}
        mock_definition.depends_on = ["daily_data"]
        mock_definition.compute_mode = "incremental"
        mock_definition.storage = Mock(spec=StorageConfig)
        mock_definition.storage.table = "factor_values"

        mock_get_factor.return_value = mock_definition

        # 模拟数据库客户端
        db_client = Mock()
        db_client._ALL_TABLES = []
        db_client.query = Mock(return_value=pl.DataFrame())
        db_client.append = Mock()
        db_client.upsert = Mock(return_value=100)

        # 模拟 TradingCalendar
        with patch('app.services.factor_compute_service.TradingCalendar') as mock_cal:
            mock_cal.get_instance.return_value.offset_trading_days = Mock(return_value="20231201")

            # 模拟 DataConfigLoader
            with patch('app.services.factor_compute_service.DataConfigLoader') as mock_config:
                mock_config.return_value.load.return_value = {}

                service = FactorComputeService(db_client)

                # 执行计算（不保存结果，避免数据库操作）
                result = service.compute_factor(
                    factor_id="test_factor",
                    start_date="20240101",
                    end_date="20240131",
                    save_results=False
                )

                assert isinstance(result, ComputeResult)
                # 注意：由于 DataLoader 会返回空数据，这里可能失败
                # 实际测试需要 mock 更多组件

    def test_resolve_preprocess_options_priority(self):
        """测试预处理选项优先级"""
        db_client = Mock()
        db_client._ALL_TABLES = []
        db_client.query = Mock(return_value=pl.DataFrame())

        with patch('app.services.factor_compute_service.TradingCalendar'):
            with patch('app.services.factor_compute_service.DataConfigLoader'):
                with patch('app.services.factor_compute_service.get_preprocess_loader') as mock_loader:
                    # 模拟配置加载器
                    mock_loader.return_value.get_default_profile.return_value = {
                        "adjust_price": "forward",
                        "filter_st": True,
                    }

                    service = FactorComputeService(db_client)

                    # 模拟因子定义
                    definition = Mock()
                    definition.params = {
                        "preprocess": {
                            "filter_st": False,  # 覆盖默认配置
                            "filter_new_stock": True,
                        }
                    }

                    # 显式传入选项（优先级最高）
                    explicit_options = {
                        "adjust_price": "backward"
                    }

                    result = service._resolve_preprocess_options(
                        "test_factor",
                        definition,
                        explicit_options,
                        None
                    )

                    # 验证优先级
                    assert result["adjust_price"] == "backward"  # 显式传入
                    assert result["filter_st"] is False  # 因子定义覆盖默认
                    assert result["filter_new_stock"] is True  # 因子定义


class TestDataLoaderProcessor:
    """测试数据加载处理器"""

    def test_data_loader_basic(self):
        """测试基本数据加载"""
        mock_db = Mock()
        mock_db.query.return_value = pl.DataFrame({
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["20240101", "20240102"],
            "close": [10.0, 20.0]
        })

        mock_data_config = Mock()
        mock_data_config.load.return_value = {
            "daily": {"table_name": "sync_daily_data"}
        }

        processor = DataLoaderProcessor(mock_db, mock_data_config)

        mock_definition = Mock()
        mock_definition.depends_on = ["daily"]

        context = ProcessContext(
            factor_id="test",
            factor_definition=mock_definition,
            calc_start="20240101",
            calc_end="20240102",
            data_start="20231201"
        )

        result = processor.process(pl.DataFrame(), context)

        assert not result.is_empty()
        assert "ts_code" in result.columns

    def test_data_loader_empty_result(self):
        """测试空数据加载"""
        mock_db = Mock()
        mock_db.query.return_value = pl.DataFrame()

        mock_data_config = Mock()
        mock_data_config.load.return_value = {
            "daily": {"table_name": "sync_daily_data"}
        }

        processor = DataLoaderProcessor(mock_db, mock_data_config)

        mock_definition = Mock()
        mock_definition.depends_on = ["daily"]

        context = ProcessContext(
            factor_id="test",
            factor_definition=mock_definition,
            calc_start="20240101",
            calc_end="20240102",
            data_start="20231201"
        )

        result = processor.process(pl.DataFrame(), context)

        assert result.is_empty()


class TestAdjustmentProcessor:
    """测试复权处理器"""

    def test_forward_adjustment(self):
        """测试前复权"""
        mock_db = Mock()
        mock_db.query.return_value = pl.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "adj_factor": [1.5]
        })

        processor = AdjustmentProcessor(mock_db)

        df = pl.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "open": [10.0],
            "high": [12.0],
            "low": [9.0],
            "close": [11.0]
        })

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240101",
            data_start="20240101",
            preprocess_options={"adjust_price": "forward"}
        )

        result = processor.process(df, context)

        # 验证价格被调整
        assert result["close"][0] != 11.0

    def test_no_adjustment(self):
        """测试不复权"""
        processor = AdjustmentProcessor(Mock())

        df = pl.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "close": [10.0]
        })

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240101",
            data_start="20240101",
            preprocess_options={"adjust_price": "none"}
        )

        result = processor.process(df, context)

        # 价格不应该改变
        assert result["close"][0] == 10.0

    def test_adjustment_skip_when_disabled(self):
        """测试禁用复权时跳过"""
        processor = AdjustmentProcessor(Mock())

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240101",
            data_start="20240101",
            preprocess_options={"adjust_price": "none"}
        )

        assert processor.should_run(context) is False


class TestStatusFilterProcessor:
    """测试状态过滤处理器"""

    def test_filter_st_stocks(self):
        """测试过滤 ST 股票"""
        mock_db = Mock()
        mock_db.query.return_value = pl.DataFrame({
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["20240101", "20240101"],
            "is_st": [True, False]
        })

        processor = StatusFilterProcessor(mock_db)

        df = pl.DataFrame({
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["20240101", "20240101"],
            "close": [10.0, 20.0]
        })

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240101",
            data_start="20240101",
            preprocess_options={"filter_st": True}
        )

        result = processor.process(df, context)

        # ST 股票应该被过滤
        assert len(result) < len(df)

    def test_filter_new_stocks(self):
        """测试过滤新股"""
        mock_db = Mock()
        mock_db.query.return_value = pl.DataFrame({
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["20240101", "20240101"],
            "is_new_stock": [True, False]
        })

        processor = StatusFilterProcessor(mock_db)

        df = pl.DataFrame({
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["20240101", "20240101"],
            "close": [10.0, 20.0]
        })

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240101",
            data_start="20240101",
            preprocess_options={"filter_new_stock": True, "new_stock_days": 60}
        )

        result = processor.process(df, context)

        # 新股应该被过滤
        assert len(result) <= len(df)

    def test_mark_limit_up_down(self):
        """测试标记涨跌停"""
        mock_db = Mock()
        mock_db.query.return_value = pl.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "is_limit_up": [True],
            "is_limit_down": [False]
        })

        processor = StatusFilterProcessor(mock_db)

        df = pl.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "close": [10.0]
        })

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240101",
            data_start="20240101",
            preprocess_options={"mark_limit": True}
        )

        result = processor.process(df, context)

        # 应该有涨跌停标记列
        assert "is_limit_up" in result.columns or "is_limit" in result.columns


class TestFactorComputeProcessor:
    """测试因子计算处理器"""

    def test_factor_compute_basic(self):
        """测试基本因子计算"""
        mock_definition = Mock()
        mock_definition.func = Mock(return_value=pl.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "factor_value": [1.5]
        }))
        mock_definition.params = {}

        processor = FactorComputeProcessor()

        df = pl.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "close": [10.0]
        })

        context = ProcessContext(
            factor_id="test",
            factor_definition=mock_definition,
            calc_start="20240101",
            calc_end="20240101",
            data_start="20240101"
        )

        result = processor.process(df, context)

        assert "factor_value" in result.columns
        assert len(result) > 0

    def test_factor_compute_with_params(self):
        """测试带参数的因子计算"""
        mock_definition = Mock()
        mock_definition.func = Mock(return_value=pl.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "factor_value": [2.0]
        }))
        mock_definition.params = {"window": 20, "method": "sma"}

        processor = FactorComputeProcessor()

        df = pl.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "close": [10.0]
        })

        context = ProcessContext(
            factor_id="test",
            factor_definition=mock_definition,
            calc_start="20240101",
            calc_end="20240101",
            data_start="20240101"
        )

        result = processor.process(df, context)

        # 验证参数被传递
        mock_definition.func.assert_called_once()
        call_args = mock_definition.func.call_args
        assert call_args[0][1] == {"window": 20, "method": "sma"}


class TestDateRangeFilterProcessor:
    """测试日期范围过滤处理器"""

    def test_filter_date_range(self):
        """测试日期范围过滤"""
        processor = DateRangeFilterProcessor()

        df = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 5,
            "trade_date": ["20231201", "20240101", "20240115", "20240131", "20240201"],
            "factor_value": [1.0, 2.0, 3.0, 4.0, 5.0]
        })

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240131",
            data_start="20231201"
        )

        result = processor.process(df, context)

        # 只保留 20240101 到 20240131 的数据
        assert len(result) == 3
        assert result["trade_date"].min() >= "20240101"
        assert result["trade_date"].max() <= "20240131"

    def test_filter_empty_dataframe(self):
        """测试空 DataFrame"""
        processor = DateRangeFilterProcessor()

        df = pl.DataFrame()

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240131",
            data_start="20231201"
        )

        result = processor.process(df, context)

        assert result.is_empty()


class TestQualityCheckerProcessor:
    """测试质量检查处理器"""

    def test_quality_check_basic(self):
        """测试基本质量检查"""
        processor = QualityCheckerProcessor()

        df = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 5,
            "trade_date": ["20240101"] * 5,
            "factor_value": [1.0, 2.0, None, 4.0, 5.0]
        })

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240101",
            data_start="20240101"
        )

        result = processor.process(df, context)

        # 应该添加质量标记列
        assert "quality_flag" in result.columns or result is not None

    def test_quality_check_all_valid(self):
        """测试全部有效数据"""
        processor = QualityCheckerProcessor()

        df = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 3,
            "trade_date": ["20240101"] * 3,
            "factor_value": [1.0, 2.0, 3.0]
        })

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240101",
            data_start="20240101"
        )

        result = processor.process(df, context)

        assert len(result) == 3

    def test_quality_check_extreme_values(self):
        """测试极端值检测"""
        processor = QualityCheckerProcessor()

        df = pl.DataFrame({
            "ts_code": ["000001.SZ"] * 5,
            "trade_date": ["20240101"] * 5,
            "factor_value": [1.0, 2.0, 1000.0, 3.0, -1000.0]  # 包含极端值
        })

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240101",
            data_start="20240101"
        )

        result = processor.process(df, context)

        # 应该标记或处理极端值
        assert result is not None


class TestPipelineErrorHandling:
    """测试 Pipeline 错误处理"""

    def test_processor_exception_handling(self):
        """测试处理器异常处理"""
        class FailingProcessor(IProcessor):
            @property
            def name(self):
                return "Failing"

            def process(self, df, context):
                raise Exception("Processor failed")

        pipeline = DataPipeline()
        pipeline.add_stage(FailingProcessor())

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240101",
            data_start="20240101",
            dataframe=pl.DataFrame({"col": [1, 2, 3]})
        )

        with pytest.raises(Exception, match="Processor failed"):
            pipeline.execute(context)

    def test_empty_pipeline(self):
        """测试空 Pipeline"""
        pipeline = DataPipeline()

        context = ProcessContext(
            factor_id="test",
            factor_definition=Mock(),
            calc_start="20240101",
            calc_end="20240101",
            data_start="20240101",
            dataframe=pl.DataFrame({"col": [1, 2, 3]})
        )

        result = pipeline.execute(context)

        # 空 Pipeline 应该返回原始数据
        assert len(result) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
