"""
任务管理抽象层集成测试
测试 TaskService 的所有 CRUD 操作、版本控制功能
"""
import pytest
from unittest.mock import MagicMock, patch
import polars as pl
from datetime import datetime

from app.services.task_service import TaskService, sync_service, etl_service, factor_service
from app.models.base_task import SyncTaskConfig, ETLTaskConfig, FactorConfig


# ==================== Fixtures ====================

@pytest.fixture
def mock_db_client():
    """模拟 DolphinDB 客户端"""
    with patch('app.services.task_service.db_client') as mock:
        yield mock


@pytest.fixture
def sample_sync_task():
    """示例同步任务数据"""
    return {
        "task_id": "test_sync",
        "api_name": "daily",
        "api_limit": 5000,
        "fields": "ts_code,trade_date,close",
        "start_date": "20240101",
        "end_date": "20241231",
        "description": "Test sync task",
        "enabled": True,
        "version_number": 1,
        "is_current": True,
        "changed_by": "test_user",
        "change_reason": "Initial creation",
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }


@pytest.fixture
def sample_etl_task():
    """示例 ETL 任务数据"""
    return {
        "task_id": "test_etl",
        "source_table": "raw_data",
        "target_table": "processed_data",
        "script": "SELECT * FROM raw_data WHERE trade_date >= '20240101'",
        "schedule": "0 2 * * *",
        "description": "Test ETL task",
        "enabled": True,
        "version_number": 1,
        "is_current": True,
        "changed_by": "test_user",
        "change_reason": "Initial creation",
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }


@pytest.fixture
def sample_factor_task():
    """示例因子任务数据"""
    return {
        "factor_id": "test_factor",
        "code": "def calculate(df, params): return df.with_columns(pl.col('close').pct_change().alias('returns'))",
        "depends_on": "daily_basic",
        "params": '{"window": 20}',
        "lookback_days": 250,
        "description": "Test factor",
        "enabled": True,
        "version_number": 1,
        "is_current": True,
        "changed_by": "test_user",
        "change_reason": "Initial creation",
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }


# ==================== TaskService CRUD Tests ====================

class TestTaskServiceCRUD:
    """测试 TaskService 的基本 CRUD 操作"""

    def test_list_tasks_all(self, mock_db_client, sample_sync_task):
        """测试列出所有任务"""
        # 准备 mock 数据
        mock_df = pl.DataFrame([sample_sync_task])
        mock_db_client.query.return_value = mock_df

        # 执行测试
        tasks = sync_service.list_tasks(enabled_only=False)

        # 验证结果
        assert len(tasks) == 1
        assert tasks[0].task_id == "test_sync"
        assert tasks[0].api_name == "daily"
        mock_db_client.query.assert_called_once()
        sql = mock_db_client.query.call_args[0][0]
        assert "is_current = true" in sql
        assert "enabled = true" not in sql

    def test_list_tasks_enabled_only(self, mock_db_client, sample_sync_task):
        """测试只列出启用的任务"""
        mock_df = pl.DataFrame([sample_sync_task])
        mock_db_client.query.return_value = mock_df

        tasks = sync_service.list_tasks(enabled_only=True)

        assert len(tasks) == 1
        sql = mock_db_client.query.call_args[0][0]
        assert "is_current = true" in sql
        assert "enabled = true" in sql

    def test_list_tasks_empty(self, mock_db_client):
        """测试空任务列表"""
        mock_db_client.query.return_value = pl.DataFrame()

        tasks = sync_service.list_tasks()

        assert len(tasks) == 0

    def test_get_task_exists(self, mock_db_client, sample_sync_task):
        """测试获取存在的任务"""
        mock_df = pl.DataFrame([sample_sync_task])
        mock_db_client.query.return_value = mock_df

        task = sync_service.get_task("test_sync")

        assert task is not None
        assert task.task_id == "test_sync"
        assert task.api_name == "daily"
        mock_db_client.query.assert_called_once()
        call_args = mock_db_client.query.call_args
        # Check if params were passed as keyword argument or positional
        if len(call_args[0]) > 1:
            sql, params = call_args[0]
        else:
            sql = call_args[0][0]
            params = call_args[1].get('params', ())
        assert "task_id = %s" in sql or "task_id=%s" in sql
        assert "is_current = true" in sql or "is_current=true" in sql

    def test_get_task_not_exists(self, mock_db_client):
        """测试获取不存在的任务"""
        mock_db_client.query.return_value = pl.DataFrame()

        task = sync_service.get_task("nonexistent")

        assert task is None

    def test_create_task_success(self, mock_db_client, sample_sync_task):
        """测试成功创建任务"""
        # Mock: 检查任务不存在
        mock_db_client.query.return_value = pl.DataFrame()

        # Mock: 创建版本成功
        mock_db_client.create_task_version.return_value = 1

        # Mock: 返回创建的任务
        mock_db_client.query.side_effect = [
            pl.DataFrame(),  # 第一次查询：任务不存在
            pl.DataFrame([sample_sync_task])  # 第二次查询：返回创建的任务
        ]

        config_data = {
            "task_id": "test_sync",
            "api_name": "daily",
            "api_limit": 5000,
            "description": "Test sync task"
        }

        task = sync_service.create_task(
            config_data=config_data,
            changed_by="test_user",
            change_reason="Test creation"
        )

        assert task is not None
        assert task.task_id == "test_sync"
        mock_db_client.create_task_version.assert_called_once()
        call_args = mock_db_client.create_task_version.call_args[1]
        assert call_args["task_type"] == "sync"
        assert call_args["task_id"] == "test_sync"
        assert call_args["changed_by"] == "test_user"

    def test_create_task_already_exists(self, mock_db_client, sample_sync_task):
        """测试创建已存在的任务"""
        mock_df = pl.DataFrame([sample_sync_task])
        mock_db_client.query.return_value = mock_df

        config_data = {"task_id": "test_sync", "api_name": "daily"}

        with pytest.raises(ValueError, match="already exists"):
            sync_service.create_task(config_data)

    def test_update_task_success(self, mock_db_client, sample_sync_task):
        """测试成功更新任务"""
        # Mock: 任务存在
        mock_df = pl.DataFrame([sample_sync_task])

        # 更新后的任务
        updated_task = sample_sync_task.copy()
        updated_task["description"] = "Updated description"
        updated_task["version_number"] = 2

        mock_db_client.query.side_effect = [
            mock_df,  # 第一次：获取现有任务
            pl.DataFrame([updated_task])  # 第二次：返回更新后的任务
        ]
        mock_db_client.create_task_version.return_value = 2

        task = sync_service.update_task(
            task_id="test_sync",
            config_data={"description": "Updated description"},
            changed_by="test_user",
            change_reason="Update test"
        )

        assert task is not None
        assert task.description == "Updated description"
        mock_db_client.create_task_version.assert_called_once()

    def test_update_task_not_exists(self, mock_db_client):
        """测试更新不存在的任务"""
        mock_db_client.query.return_value = pl.DataFrame()

        with pytest.raises(ValueError, match="not found"):
            sync_service.update_task(
                task_id="nonexistent",
                config_data={"description": "New description"}
            )

    def test_delete_task_success(self, mock_db_client, sample_sync_task):
        """测试成功删除任务"""
        mock_df = pl.DataFrame([sample_sync_task])
        mock_db_client.query.return_value = mock_df
        mock_db_client.create_task_version.return_value = 2

        result = sync_service.delete_task(
            task_id="test_sync",
            changed_by="test_user",
            change_reason="Delete test"
        )

        assert result is True
        mock_db_client.create_task_version.assert_called_once()
        call_args = mock_db_client.create_task_version.call_args[1]
        assert call_args["config_data"]["enabled"] is False

    def test_delete_task_not_exists(self, mock_db_client):
        """测试删除不存在的任务"""
        mock_db_client.query.return_value = pl.DataFrame()

        with pytest.raises(ValueError, match="not found"):
            sync_service.delete_task("nonexistent")


# ==================== Version Control Tests ====================

class TestVersionControl:
    """测试版本控制功能 - 通过 db_client 直接调用"""

    def test_version_created_on_update(self, mock_db_client, sample_sync_task):
        """测试更新任务时创建新版本"""
        mock_df = pl.DataFrame([sample_sync_task])
        updated_task = sample_sync_task.copy()
        updated_task["version_number"] = 2

        mock_db_client.query.side_effect = [
            mock_df,  # 获取现有任务
            pl.DataFrame([updated_task])  # 返回更新后的任务
        ]
        mock_db_client.create_task_version.return_value = 2

        task = sync_service.update_task(
            task_id="test_sync",
            config_data={"description": "Updated"},
            changed_by="test_user",
            change_reason="Update test"
        )

        # 验证创建了新版本
        mock_db_client.create_task_version.assert_called_once()
        call_args = mock_db_client.create_task_version.call_args[1]
        assert call_args["task_type"] == "sync"
        assert call_args["task_id"] == "test_sync"

    def test_version_created_on_delete(self, mock_db_client, sample_sync_task):
        """测试删除任务时创建新版本（软删除）"""
        mock_df = pl.DataFrame([sample_sync_task])
        mock_db_client.query.return_value = mock_df
        mock_db_client.create_task_version.return_value = 2

        result = sync_service.delete_task(
            task_id="test_sync",
            changed_by="test_user",
            change_reason="Delete test"
        )

        assert result is True
        mock_db_client.create_task_version.assert_called_once()
        call_args = mock_db_client.create_task_version.call_args[1]
        # 验证设置了 enabled=False
        assert call_args["config_data"]["enabled"] is False

    def test_version_number_increments(self, mock_db_client, sample_sync_task):
        """测试版本号递增"""
        # 第一次更新
        mock_df = pl.DataFrame([sample_sync_task])
        updated_v2 = sample_sync_task.copy()
        updated_v2["version_number"] = 2

        mock_db_client.query.side_effect = [
            mock_df,  # 获取 v1
            pl.DataFrame([updated_v2])  # 返回 v2
        ]
        mock_db_client.create_task_version.return_value = 2

        task_v2 = sync_service.update_task(
            task_id="test_sync",
            config_data={"description": "Version 2"}
        )

        assert task_v2.version_number == 2

        # 第二次更新
        updated_v3 = updated_v2.copy()
        updated_v3["version_number"] = 3

        mock_db_client.query.side_effect = [
            pl.DataFrame([updated_v2]),  # 获取 v2
            pl.DataFrame([updated_v3])  # 返回 v3
        ]
        mock_db_client.create_task_version.return_value = 3

        task_v3 = sync_service.update_task(
            task_id="test_sync",
            config_data={"description": "Version 3"}
        )

        assert task_v3.version_number == 3


# ==================== Multi-Type Tests ====================

class TestMultipleTaskTypes:
    """测试三种任务类型的统一接口"""

    def test_sync_service_initialization(self):
        """测试同步任务服务初始化"""
        assert sync_service.task_type == "sync"
        assert sync_service.table_name == "sync_task_config"
        assert sync_service.id_field == "task_id"
        assert sync_service.model_class == SyncTaskConfig

    def test_etl_service_initialization(self):
        """测试 ETL 任务服务初始化"""
        assert etl_service.task_type == "etl"
        assert etl_service.table_name == "etl_task_config"
        assert etl_service.id_field == "task_id"
        assert etl_service.model_class == ETLTaskConfig

    def test_factor_service_initialization(self):
        """测试因子服务初始化"""
        assert factor_service.task_type == "factor"
        assert factor_service.table_name == "factor_metadata"
        assert factor_service.id_field == "factor_id"
        assert factor_service.model_class == FactorConfig

    def test_sync_task_validation(self):
        """测试同步任务数据验证"""
        valid_data = {
            "task_id": "test",
            "api_name": "daily",
            "api_limit": 5000
        }
        task = SyncTaskConfig(**valid_data)
        assert task.task_id == "test"
        assert task.api_name == "daily"

    def test_etl_task_validation(self):
        """测试 ETL 任务数据验证"""
        valid_data = {
            "task_id": "test",
            "source_table": "raw",
            "target_table": "processed",
            "script": "SELECT * FROM raw"
        }
        task = ETLTaskConfig(**valid_data)
        assert task.task_id == "test"
        assert task.source_table == "raw"

    def test_factor_task_validation(self):
        """测试因子任务数据验证"""
        valid_data = {
            "factor_id": "test",
            "code": "def calc(df): return df"
        }
        task = FactorConfig(**valid_data)
        assert task.factor_id == "test"
        assert task.code == "def calc(df): return df"


# ==================== Error Handling Tests ====================

class TestErrorHandling:
    """测试错误处理"""

    def test_invalid_task_data(self):
        """测试无效任务数据"""
        with pytest.raises(Exception):
            SyncTaskConfig(task_id="test")  # 缺少必需字段 api_name

    def test_database_error_handling(self, mock_db_client):
        """测试数据库错误处理"""
        mock_db_client.query.side_effect = Exception("Database connection failed")

        with pytest.raises(Exception, match="Database connection failed"):
            sync_service.list_tasks()

    def test_malformed_data_in_list(self, mock_db_client):
        """测试列表中包含格式错误的数据"""
        malformed_data = [
            {"task_id": "valid", "api_name": "daily", "version_number": 1, "is_current": True},
            {"task_id": "invalid"},  # 缺少必需字段
        ]
        mock_df = pl.DataFrame(malformed_data)
        mock_db_client.query.return_value = mock_df

        tasks = sync_service.list_tasks()

        # 应该只返回有效的任务，跳过无效的
        assert len(tasks) == 1
        assert tasks[0].task_id == "valid"
