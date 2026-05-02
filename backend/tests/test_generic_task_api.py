"""
通用任务 API 端点集成测试
测试所有新的 API 端点（GET/POST/PUT/DELETE）和错误处理
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import polars as pl
from datetime import datetime

from app.main import app
from app.models.base_task import SyncTaskConfig, ETLTaskConfig, FactorConfig


# ==================== Fixtures ====================

@pytest.fixture
def client():
    """FastAPI 测试客户端"""
    return TestClient(app)


@pytest.fixture
def mock_db_client():
    """模拟 DolphinDB 客户端"""
    with patch('app.services.task_service.db_client') as mock:
        yield mock


@pytest.fixture
def sample_sync_tasks():
    """示例同步任务列表"""
    return [
        {
            "task_id": "daily_basic",
            "api_name": "daily",
            "api_limit": 5000,
            "fields": "ts_code,trade_date,close",
            "description": "Daily basic data",
            "enabled": True,
            "version_number": 1,
            "is_current": True,
            "changed_by": "system",
            "change_reason": "Initial",
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        },
        {
            "task_id": "stock_basic",
            "api_name": "stock_basic",
            "api_limit": 5000,
            "fields": "",
            "description": "Stock basic info",
            "enabled": True,
            "version_number": 1,
            "is_current": True,
            "changed_by": "system",
            "change_reason": "Initial",
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
    ]


# ==================== Sync Task API Tests ====================

class TestSyncTaskAPI:
    """测试同步任务 API 端点"""

    def test_list_sync_tasks(self, client, mock_db_client, sample_sync_tasks):
        """测试 GET /api/v1/sync/tasks"""
        mock_df = pl.DataFrame(sample_sync_tasks)
        mock_db_client.query.return_value = mock_df

        response = client.get("/api/v1/sync/tasks")

        assert response.status_code == 200
        data = response.json()
        assert "tasks" in data
        assert len(data["tasks"]) == 2
        assert data["total"] == 2
        assert data["tasks"][0]["task_id"] == "daily_basic"

    def test_list_sync_tasks_enabled_only(self, client, mock_db_client, sample_sync_tasks):
        """测试 GET /api/v1/sync/tasks?enabled_only=true"""
        mock_df = pl.DataFrame(sample_sync_tasks)
        mock_db_client.query.return_value = mock_df

        response = client.get("/api/v1/sync/tasks?enabled_only=true")

        assert response.status_code == 200
        data = response.json()
        assert len(data["tasks"]) == 2

    def test_get_sync_task(self, client, mock_db_client, sample_sync_tasks):
        """测试 GET /api/v1/sync/tasks/{task_id}"""
        mock_df = pl.DataFrame([sample_sync_tasks[0]])
        mock_db_client.query.return_value = mock_df

        response = client.get("/api/v1/sync/tasks/daily_basic")

        assert response.status_code == 200
        data = response.json()
        assert "task" in data
        assert data["task"]["task_id"] == "daily_basic"
        assert data["task"]["api_name"] == "daily"

    def test_get_sync_task_not_found(self, client, mock_db_client):
        """测试 GET /api/v1/sync/tasks/{task_id} - 404"""
        mock_db_client.query.return_value = pl.DataFrame()

        response = client.get("/api/v1/sync/tasks/nonexistent")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"]

    def test_create_sync_task(self, client, mock_db_client, sample_sync_tasks):
        """测试 POST /api/v1/sync/tasks"""
        # Mock: 任务不存在
        mock_db_client.query.side_effect = [
            pl.DataFrame(),  # 检查不存在
            pl.DataFrame([sample_sync_tasks[0]])  # 返回创建的任务
        ]
        mock_db_client.create_task_version.return_value = 1

        payload = {
            "config_data": {
                "task_id": "daily_basic",
                "api_name": "daily",
                "api_limit": 5000,
                "description": "Daily basic data"
            },
            "changed_by": "test_user",
            "change_reason": "Create test"
        }

        response = client.post("/api/v1/sync/tasks", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert "task" in data
        assert data["task"]["task_id"] == "daily_basic"

    def test_create_sync_task_already_exists(self, client, mock_db_client, sample_sync_tasks):
        """测试 POST /api/v1/sync/tasks - 400 (已存在)"""
        mock_df = pl.DataFrame([sample_sync_tasks[0]])
        mock_db_client.query.return_value = mock_df

        payload = {
            "config_data": {
                "task_id": "daily_basic",
                "api_name": "daily"
            }
        }

        response = client.post("/api/v1/sync/tasks", json=payload)

        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]

    def test_update_sync_task(self, client, mock_db_client, sample_sync_tasks):
        """测试 PUT /api/v1/sync/tasks/{task_id}"""
        original = sample_sync_tasks[0].copy()
        updated = original.copy()
        updated["description"] = "Updated description"
        updated["version_number"] = 2

        mock_db_client.query.side_effect = [
            pl.DataFrame([original]),  # 获取现有任务
            pl.DataFrame([updated])    # 返回更新后的任务
        ]
        mock_db_client.create_task_version.return_value = 2

        payload = {
            "config_data": {
                "description": "Updated description"
            },
            "changed_by": "test_user",
            "change_reason": "Update test"
        }

        response = client.put("/api/v1/sync/tasks/daily_basic", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["task"]["description"] == "Updated description"
        assert data["task"]["version_number"] == 2

    def test_update_sync_task_not_found(self, client, mock_db_client):
        """测试 PUT /api/v1/sync/tasks/{task_id} - 400 (不存在)"""
        mock_db_client.query.return_value = pl.DataFrame()

        payload = {
            "config_data": {
                "description": "New description"
            }
        }

        response = client.put("/api/v1/sync/tasks/nonexistent", json=payload)

        assert response.status_code == 400
        assert "not found" in response.json()["detail"]

    def test_delete_sync_task(self, client, mock_db_client, sample_sync_tasks):
        """测试 DELETE /api/v1/sync/tasks/{task_id}"""
        mock_df = pl.DataFrame([sample_sync_tasks[0]])
        mock_db_client.query.return_value = mock_df
        mock_db_client.create_task_version.return_value = 2

        response = client.delete("/api/v1/sync/tasks/daily_basic")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "deleted successfully" in data["message"]

    def test_delete_sync_task_not_found(self, client, mock_db_client):
        """测试 DELETE /api/v1/sync/tasks/{task_id} - 400 (不存在)"""
        mock_db_client.query.return_value = pl.DataFrame()

        response = client.delete("/api/v1/sync/tasks/nonexistent")

        assert response.status_code == 400
        assert "not found" in response.json()["detail"]


# ==================== ETL Task API Tests ====================

class TestETLTaskAPI:
    """测试 ETL 任务 API 端点"""

    def test_list_etl_tasks(self, client, mock_db_client):
        """测试 GET /api/v1/etl/tasks"""
        etl_tasks = [
            {
                "task_id": "test_etl",
                "source_table": "raw_data",
                "target_table": "processed_data",
                "script": "SELECT * FROM raw_data",
                "schedule": "0 2 * * *",
                "description": "Test ETL",
                "enabled": True,
                "version_number": 1,
                "is_current": True,
                "changed_by": "system",
                "change_reason": "Initial",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
        ]
        mock_df = pl.DataFrame(etl_tasks)
        mock_db_client.query.return_value = mock_df

        response = client.get("/api/v1/etl/tasks")

        assert response.status_code == 200
        data = response.json()
        assert len(data["tasks"]) == 1
        assert data["tasks"][0]["task_id"] == "test_etl"

    def test_create_etl_task(self, client, mock_db_client):
        """测试 POST /api/v1/etl/tasks"""
        etl_task = {
            "task_id": "new_etl",
            "source_table": "raw",
            "target_table": "processed",
            "script": "SELECT * FROM raw",
            "schedule": "0 3 * * *",
            "version_number": 1,
            "is_current": True,
            "changed_by": "test_user",
            "change_reason": "Create",
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }

        mock_db_client.query.side_effect = [
            pl.DataFrame(),  # 不存在
            pl.DataFrame([etl_task])  # 返回创建的任务
        ]
        mock_db_client.create_task_version.return_value = 1

        payload = {
            "config_data": {
                "task_id": "new_etl",
                "source_table": "raw",
                "target_table": "processed",
                "script": "SELECT * FROM raw"
            }
        }

        response = client.post("/api/v1/etl/tasks", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["task"]["task_id"] == "new_etl"


# ==================== Factor API Tests ====================

class TestFactorAPI:
    """测试因子 API 端点"""

    def test_list_factors(self, client, mock_db_client):
        """测试 GET /api/v1/factors/tasks"""
        factors = [
            {
                "factor_id": "test_factor",
                "code": "def calc(df): return df",
                "depends_on": "daily_basic",
                "params": "{}",
                "lookback_days": 250,
                "description": "Test factor",
                "enabled": True,
                "version_number": 1,
                "is_current": True,
                "changed_by": "system",
                "change_reason": "Initial",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
        ]
        mock_df = pl.DataFrame(factors)
        mock_db_client.query.return_value = mock_df

        response = client.get("/api/v1/factors/tasks")

        assert response.status_code == 200
        data = response.json()
        assert len(data["tasks"]) == 1
        assert data["tasks"][0]["factor_id"] == "test_factor"

    def test_create_factor(self, client, mock_db_client):
        """测试 POST /api/v1/factors/tasks"""
        factor = {
            "factor_id": "new_factor",
            "code": "def calc(df): return df.with_columns(pl.col('close').pct_change())",
            "depends_on": "daily_basic",
            "params": '{"window": 20}',
            "lookback_days": 250,
            "version_number": 1,
            "is_current": True,
            "changed_by": "test_user",
            "change_reason": "Create",
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }

        mock_db_client.query.side_effect = [
            pl.DataFrame(),  # 不存在
            pl.DataFrame([factor])  # 返回创建的因子
        ]
        mock_db_client.create_task_version.return_value = 1

        payload = {
            "config_data": {
                "factor_id": "new_factor",
                "code": "def calc(df): return df.with_columns(pl.col('close').pct_change())"
            }
        }

        response = client.post("/api/v1/factors/tasks", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["task"]["factor_id"] == "new_factor"


# ==================== Error Handling Tests ====================

class TestAPIErrorHandling:
    """测试 API 错误处理"""

    def test_invalid_json_payload(self, client):
        """测试无效的 JSON 负载"""
        response = client.post(
            "/api/v1/sync/tasks",
            data="invalid json",
            headers={"Content-Type": "application/json"}
        )

        assert response.status_code == 422  # Unprocessable Entity

    def test_missing_required_fields(self, client, mock_db_client):
        """测试缺少必需字段"""
        mock_db_client.query.return_value = pl.DataFrame()

        payload = {
            "config_data": {
                "task_id": "test"
                # 缺少 api_name
            }
        }

        response = client.post("/api/v1/sync/tasks", json=payload)

        assert response.status_code == 400 or response.status_code == 422

    def test_database_error_500(self, client, mock_db_client):
        """测试数据库错误返回 500"""
        mock_db_client.query.side_effect = Exception("Database connection failed")

        response = client.get("/api/v1/sync/tasks")

        assert response.status_code == 500
        assert "Database connection failed" in response.json()["detail"]

    def test_invalid_task_id_format(self, client, mock_db_client):
        """测试无效的任务 ID 格式"""
        mock_db_client.query.return_value = pl.DataFrame()

        # 空字符串作为 task_id 会匹配到列表端点，返回 200
        response = client.get("/api/v1/sync/tasks/")

        # FastAPI 会将尾部斜杠重定向或匹配到列表端点
        assert response.status_code in [200, 307, 404, 405]


# ==================== Cross-Type Consistency Tests ====================

class TestCrossTypeConsistency:
    """测试三种任务类型的 API 一致性"""

    def test_all_types_have_list_endpoint(self, client, mock_db_client):
        """测试所有类型都有列表端点"""
        mock_db_client.query.return_value = pl.DataFrame()

        endpoints = [
            "/api/v1/sync/tasks",
            "/api/v1/etl/tasks",
            "/api/v1/factors/tasks"
        ]

        for endpoint in endpoints:
            response = client.get(endpoint)
            assert response.status_code == 200
            data = response.json()
            assert "tasks" in data
            assert "total" in data

    def test_all_types_have_create_endpoint(self, client, mock_db_client):
        """测试所有类型都有创建端点"""
        mock_db_client.query.return_value = pl.DataFrame()

        test_cases = [
            ("/api/v1/sync/tasks", {"task_id": "test", "api_name": "daily"}),
            ("/api/v1/etl/tasks", {"task_id": "test", "source_table": "raw", "target_table": "proc", "script": "SELECT 1"}),
            ("/api/v1/factors/tasks", {"factor_id": "test", "code": "def f(): pass"})
        ]

        for endpoint, config in test_cases:
            payload = {"config_data": config}
            response = client.post(endpoint, json=payload)
            # 应该返回 200 或 400 (如果验证失败)，但不应该是 404
            assert response.status_code != 404

    def test_response_format_consistency(self, client, mock_db_client):
        """测试响应格式一致性"""
        mock_db_client.query.return_value = pl.DataFrame()

        endpoints = [
            "/api/v1/sync/tasks",
            "/api/v1/etl/tasks",
            "/api/v1/factors/tasks"
        ]

        for endpoint in endpoints:
            response = client.get(endpoint)
            data = response.json()

            # 所有端点应该返回相同的结构
            assert "tasks" in data
            assert "total" in data
            assert isinstance(data["tasks"], list)
            assert isinstance(data["total"], int)
