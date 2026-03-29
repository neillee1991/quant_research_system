"""
FactorComputeService 单元测试
测试因子计算服务的完整流程
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
import polars as pl

from app.services.factor_compute_service import FactorComputeService, ComputeResult, DEFAULT_LOOKBACK_DAYS
from engine.factor.registry import FactorDefinition


class TestFactorComputeServiceBasics:
    """因子计算服务基础功能测试"""

    @pytest.fixture
    def mock_db_client(self):
        """创建 mock 数据库客户端"""
        db = Mock()
        db._ALL_TABLES = []
        db.register_meta_table = Mock()
        db.query = Mock(return_value=pl.DataFrame())
        db.upsert = Mock()
        return db

    @pytest.fixture
    def service(self, mock_db_client):
        """创建服务实例"""
        with patch('app.services.factor_compute_service.TradingCalendar'):
            with patch('app.services.factor_compute_service.DataConfigLoader'):
                with patch('app.services.factor_compute_service.get_preprocess_loader'):
                    with patch('app.services.factor_compute_service.PipelineFactory'):
                        service = FactorComputeService(mock_db_client)
                        return service

    def test_initialization(self, service, mock_db_client):
        """测试初始化"""
        assert service.db is mock_db_client
        assert hasattr(service, 'trading_cal')
        assert hasattr(service, 'data_config')
        assert hasattr(service, 'pipeline_factory')

    def test_register_config_tables(self, mock_db_client):
        """测试注册配置表"""
        with patch('app.services.factor_compute_service.TradingCalendar'):
            with patch('app.services.factor_compute_service.DataConfigLoader') as mock_loader:
                mock_loader.return_value.load.return_value = {
                    "daily": {"table_name": "sync_daily_data"}
                }
                with patch('app.services.factor_compute_service.get_preprocess_loader'):
                    with patch('app.services.factor_compute_service.PipelineFactory'):
                        service = FactorComputeService(mock_db_client)

                        # 验证注册了表
                        assert mock_db_client.register_meta_table.called


class TestComputeFactorFlow:
    """因子计算完整流程测试"""

    @pytest.fixture
    def mock_db_client(self):
        db = Mock()
        db._ALL_TABLES = []
        db.register_meta_table = Mock()
        db.query = Mock(return_value=pl.DataFrame())
        db.upsert = Mock()
        return db

    @pytest.fixture
    def mock_factor_definition(self):
        """创建 mock 因子定义"""
        definition = Mock(spec=FactorDefinition)
        definition.factor_id = "test_factor"
        definition.compute_mode = "incremental"
        definition.depends_on = ["daily"]
        definition.lookback_days = 20
        definition.params = {}
        definition.func = Mock(return_value=pl.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "factor_value": [1.5]
        }))
        return definition

    @pytest.fixture
    def service(self, mock_db_client):
        with patch('app.services.factor_compute_service.TradingCalendar'):
            with patch('app.services.factor_compute_service.DataConfigLoader'):
                with patch('app.services.factor_compute_service.get_preprocess_loader'):
                    with patch('app.services.factor_compute_service.PipelineFactory'):
                        return FactorComputeService(mock_db_client)

    def test_compute_factor_not_found(self, service):
        """测试因子不存在"""
        with patch('app.services.factor_compute_service.discover_factors'):
            with patch('app.services.factor_compute_service.get_factor', return_value=None):
                result = service.compute_factor("nonexistent_factor")

                assert result.success is False
                assert "not found" in result.message.lower()

    def test_compute_factor_success(self, service, mock_db_client, mock_factor_definition):
        """测试因子计算成功"""
        with patch('app.services.factor_compute_service.discover_factors'):
            with patch('app.services.factor_compute_service.get_factor', return_value=mock_factor_definition):
                with patch.object(service, '_resolve_dates', return_value=("20240101", "20240101", "20231201")):
                    with patch.object(service, '_resolve_preprocess_options', return_value={}):
                        with patch.object(service.pipeline_factory, 'create_pipeline') as mock_pipeline:
                            mock_pipeline.return_value.process.return_value = pl.DataFrame({
                                "ts_code": ["000001.SZ"],
                                "trade_date": ["20240101"],
                                "factor_value": [1.5]
                            })

                            result = service.compute_factor("test_factor", save_results=False)

                            assert result.success is True
                            assert result.factor_id == "test_factor"
                            assert result.rows > 0

    def test_compute_factor_with_target_date(self, service, mock_factor_definition):
        """测试指定目标日期计算"""
        with patch('app.services.factor_compute_service.discover_factors'):
            with patch('app.services.factor_compute_service.get_factor', return_value=mock_factor_definition):
                with patch.object(service, '_resolve_dates', return_value=("20240101", "20240101", "20231201")):
                    with patch.object(service, '_resolve_preprocess_options', return_value={}):
                        with patch.object(service.pipeline_factory, 'create_pipeline') as mock_pipeline:
                            mock_pipeline.return_value.process.return_value = pl.DataFrame({
                                "ts_code": ["000001.SZ"],
                                "trade_date": ["20240101"],
                                "factor_value": [1.5]
                            })

                            result = service.compute_factor("test_factor", target_date="20240101", save_results=False)

                            assert result.success is True

    def test_compute_factor_with_date_range(self, service, mock_factor_definition):
        """测试日期范围计算"""
        with patch('app.services.factor_compute_service.discover_factors'):
            with patch('app.services.factor_compute_service.get_factor', return_value=mock_factor_definition):
                with patch.object(service, '_resolve_dates', return_value=("20240101", "20240131", "20231201")):
                    with patch.object(service, '_resolve_preprocess_options', return_value={}):
                        with patch.object(service.pipeline_factory, 'create_pipeline') as mock_pipeline:
                            mock_pipeline.return_value.process.return_value = pl.DataFrame({
                                "ts_code": ["000001.SZ"],
                                "trade_date": ["20240101"],
                                "factor_value": [1.5]
                            })

                            result = service.compute_factor(
                                "test_factor",
                                start_date="20240101",
                                end_date="20240131",
                                save_results=False
                            )

                            assert result.success is True


class TestDateResolution:
    """日期解析测试"""

    @pytest.fixture
    def service(self):
        db = Mock()
        db._ALL_TABLES = []
        db.query = Mock(return_value=pl.DataFrame())
        with patch('app.services.factor_compute_service.TradingCalendar'):
            with patch('app.services.factor_compute_service.DataConfigLoader'):
                with patch('app.services.factor_compute_service.get_preprocess_loader'):
                    with patch('app.services.factor_compute_service.PipelineFactory'):
                        return FactorComputeService(db)

    @pytest.fixture
    def mock_definition(self):
        definition = Mock()
        definition.lookback_days = 20
        definition.compute_mode = "incremental"
        return definition

    def test_resolve_dates_incremental_mode(self, service, mock_definition):
        """测试增量模式日期解析"""
        with patch.object(service.trading_cal, 'get_latest_trade_date', return_value="20240105"):
            with patch.object(service.trading_cal, 'offset_trade_date', return_value="20231201"):
                calc_start, calc_end, data_start = service._resolve_dates(
                    "test_factor", "incremental", None, None, None, mock_definition
                )

                assert calc_start is not None
                assert calc_end is not None
                assert data_start is not None

    def test_resolve_dates_full_mode(self, service, mock_definition):
        """测试全量模式日期解析"""
        with patch.object(service.trading_cal, 'get_latest_trade_date', return_value="20240105"):
            with patch.object(service.trading_cal, 'offset_trade_date', return_value="20200101"):
                calc_start, calc_end, data_start = service._resolve_dates(
                    "test_factor", "full", None, None, None, mock_definition
                )

                assert calc_start is not None
                assert calc_end is not None

    def test_resolve_dates_with_target_date(self, service, mock_definition):
        """测试指定目标日期"""
        with patch.object(service.trading_cal, 'offset_trade_date', return_value="20231201"):
            calc_start, calc_end, data_start = service._resolve_dates(
                "test_factor", "incremental", "20240101", None, None, mock_definition
            )

            assert calc_start == "20240101"
            assert calc_end == "20240101"

    def test_resolve_dates_with_range(self, service, mock_definition):
        """测试日期范围"""
        with patch.object(service.trading_cal, 'offset_trade_date', return_value="20231201"):
            calc_start, calc_end, data_start = service._resolve_dates(
                "test_factor", "full", None, "20240101", "20240131", mock_definition
            )

            assert calc_start == "20240101"
            assert calc_end == "20240131"


class TestPreprocessOptions:
    """预处理选项测试"""

    @pytest.fixture
    def service(self):
        db = Mock()
        db._ALL_TABLES = []
        db.query = Mock(return_value=pl.DataFrame())
        with patch('app.services.factor_compute_service.TradingCalendar'):
            with patch('app.services.factor_compute_service.DataConfigLoader'):
                with patch('app.services.factor_compute_service.get_preprocess_loader') as mock_loader:
                    mock_loader.return_value.get_profile.return_value = {
                        "adjust_price": "forward",
                        "filter_st": True
                    }
                    with patch('app.services.factor_compute_service.PipelineFactory'):
                        return FactorComputeService(db)

    @pytest.fixture
    def mock_definition(self):
        definition = Mock()
        definition.preprocess = None
        return definition

    def test_resolve_preprocess_explicit_options(self, service, mock_definition):
        """测试显式传入的预处理选项"""
        explicit = {"adjust_price": "backward", "filter_st": False}

        result = service._resolve_preprocess_options(
            "test_factor", mock_definition, explicit, None
        )

        assert result["adjust_price"] == "backward"
        assert result["filter_st"] is False

    def test_resolve_preprocess_profile(self, service, mock_definition):
        """测试使用 profile"""
        result = service._resolve_preprocess_options(
            "test_factor", mock_definition, None, "default"
        )

        assert "adjust_price" in result

    def test_resolve_preprocess_from_definition(self, service, mock_definition):
        """测试从因子定义获取"""
        mock_definition.preprocess = {"adjust_price": "none"}

        result = service._resolve_preprocess_options(
            "test_factor", mock_definition, None, None
        )

        # 应该使用定义中的配置
        assert result is not None


class TestResultSaving:
    """结果保存测试"""

    @pytest.fixture
    def service(self):
        db = Mock()
        db._ALL_TABLES = []
        db.upsert = Mock()
        with patch('app.services.factor_compute_service.TradingCalendar'):
            with patch('app.services.factor_compute_service.DataConfigLoader'):
                with patch('app.services.factor_compute_service.get_preprocess_loader'):
                    with patch('app.services.factor_compute_service.PipelineFactory'):
                        return FactorComputeService(db)

    def test_save_results(self, service):
        """测试保存结果"""
        df = pl.DataFrame({
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["20240101", "20240101"],
            "factor_value": [1.5, 2.0]
        })

        service._save_results("test_factor", df)

        # 验证调用了 upsert
        service.db.upsert.assert_called()

    def test_save_empty_results(self, service):
        """测试保存空结果"""
        df = pl.DataFrame()

        service._save_results("test_factor", df)

        # 空结果也应该尝试保存
        service.db.upsert.assert_called()


class TestRunRecordManagement:
    """运行记录管理测试"""

    @pytest.fixture
    def service(self):
        db = Mock()
        db._ALL_TABLES = []
        db.upsert = Mock()
        with patch('app.services.factor_compute_service.TradingCalendar'):
            with patch('app.services.factor_compute_service.DataConfigLoader'):
                with patch('app.services.factor_compute_service.get_preprocess_loader'):
                    with patch('app.services.factor_compute_service.PipelineFactory'):
                        return FactorComputeService(db)

    def test_create_run_record(self, service):
        """测试创建运行记录"""
        run_id = service._create_run_record(
            "test_factor", "incremental", "20240101", "20240101", {}
        )

        assert run_id is not None
        assert isinstance(run_id, str)
        service.db.upsert.assert_called()

    def test_finish_run_record_success(self, service):
        """测试完成运行记录（成功）"""
        started_at = datetime.now()

        service._finish_run_record("run_123", "success", 100, started_at)

        service.db.upsert.assert_called()

    def test_finish_run_record_failure(self, service):
        """测试完成运行记录（失败）"""
        started_at = datetime.now()

        service._finish_run_record("run_123", "failed", 0, started_at, "Error message")

        service.db.upsert.assert_called()


class TestMetadataUpdate:
    """元数据更新测试"""

    @pytest.fixture
    def service(self):
        db = Mock()
        db._ALL_TABLES = []
        db.upsert = Mock()
        with patch('app.services.factor_compute_service.TradingCalendar'):
            with patch('app.services.factor_compute_service.DataConfigLoader'):
                with patch('app.services.factor_compute_service.get_preprocess_loader'):
                    with patch('app.services.factor_compute_service.PipelineFactory'):
                        return FactorComputeService(db)

    @pytest.fixture
    def mock_definition(self):
        return Mock()

    def test_update_metadata(self, service, mock_definition):
        """测试更新元数据"""
        service._update_metadata("test_factor", mock_definition, "20240101", 100)

        service.db.upsert.assert_called()


class TestErrorHandling:
    """错误处理测试"""

    @pytest.fixture
    def service(self):
        db = Mock()
        db._ALL_TABLES = []
        db.query = Mock(return_value=pl.DataFrame())
        with patch('app.services.factor_compute_service.TradingCalendar'):
            with patch('app.services.factor_compute_service.DataConfigLoader'):
                with patch('app.services.factor_compute_service.get_preprocess_loader'):
                    with patch('app.services.factor_compute_service.PipelineFactory'):
                        return FactorComputeService(db)

    def test_compute_factor_exception_handling(self, service):
        """测试计算异常处理"""
        with patch('app.services.factor_compute_service.discover_factors'):
            with patch('app.services.factor_compute_service.get_factor', side_effect=Exception("Test error")):
                result = service.compute_factor("test_factor")

                assert result.success is False
                assert "error" in result.message.lower()

    def test_pipeline_exception_handling(self, service):
        """测试 Pipeline 异常处理"""
        mock_definition = Mock()
        mock_definition.compute_mode = "incremental"
        mock_definition.depends_on = ["daily"]
        mock_definition.lookback_days = 20

        with patch('app.services.factor_compute_service.discover_factors'):
            with patch('app.services.factor_compute_service.get_factor', return_value=mock_definition):
                with patch.object(service, '_resolve_dates', return_value=("20240101", "20240101", "20231201")):
                    with patch.object(service, '_resolve_preprocess_options', return_value={}):
                        with patch.object(service.pipeline_factory, 'create_pipeline') as mock_pipeline:
                            mock_pipeline.return_value.process.side_effect = Exception("Pipeline error")

                            result = service.compute_factor("test_factor", save_results=False)

                            assert result.success is False


class TestComputeResult:
    """ComputeResult 数据类测试"""

    def test_compute_result_success(self):
        """测试成功结果"""
        result = ComputeResult(
            success=True,
            factor_id="test_factor",
            rows=100,
            elapsed_seconds=1.5,
            calc_start="20240101",
            calc_end="20240131"
        )

        assert result.success is True
        assert result.factor_id == "test_factor"
        assert result.rows == 100
        assert result.message is None

    def test_compute_result_failure(self):
        """测试失败结果"""
        result = ComputeResult(
            success=False,
            factor_id="test_factor",
            rows=0,
            elapsed_seconds=0.5,
            calc_start="",
            calc_end="",
            message="Computation failed"
        )

        assert result.success is False
        assert result.message == "Computation failed"

    def test_compute_result_with_quality_metrics(self):
        """测试带质量指标的结果"""
        result = ComputeResult(
            success=True,
            factor_id="test_factor",
            rows=100,
            elapsed_seconds=2.0,
            calc_start="20240101",
            calc_end="20240131",
            quality_metrics={"null_rate": 0.05, "extreme_count": 2}
        )

        assert result.quality_metrics is not None
        assert result.quality_metrics["null_rate"] == 0.05
