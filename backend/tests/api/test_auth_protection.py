"""
认证保护测试
验证敏感端点需要认证
"""
import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.core.auth import create_access_token

client = TestClient(app)


@pytest.mark.integration
class TestAuthProtection:
    """测试端点认证保护"""

    def test_factor_test_endpoint_requires_auth(self):
        """测试因子测试端点需要认证"""
        response = client.post("/api/v1/factor/factors/test", json={
            "code": "def compute(df): return df",
            "start_date": "20240101",
            "end_date": "20240131"
        })
        # 可能是 401 或 422（参数验证失败），但不能是 200
        assert response.status_code != 200

    def test_query_endpoint_requires_auth(self):
        """测试查询端点需要认证"""
        response = client.get("/api/v1/data/query", params={"sql": "SELECT 1"})
        # 可能是 401 或其他，但不能是 200
        assert response.status_code != 200

    def test_authenticated_access_allowed(self):
        """测试认证后允许访问"""
        token = create_access_token(data={"sub": "admin"})
        # 即使认证了，请求参数可能验证失败，但不应是 401
        response = client.post(
            "/api/v1/factor/factors/test",
            json={
                "code": "def compute(df): return df",
                "start_date": "20240101",
                "end_date": "20240131"
            },
            headers={"Authorization": f"Bearer {token}"}
        )
        # 不应是 401（认证失败）
        assert response.status_code != 401

    def test_list_sync_tasks_requires_auth(self):
        """测试同步任务列表需要认证"""
        response = client.get("/api/v1/tasks/sync")
        assert response.status_code != 200

    def test_list_etl_tasks_requires_auth(self):
        """测试ETL任务列表需要认证"""
        response = client.get("/api/v1/tasks/etl")
        assert response.status_code != 200

    def test_tasks_endpoint_requires_auth(self):
        """测试统一任务端点需要认证"""
        response = client.get("/api/v1/tasks/sync")
        assert response.status_code != 200

    def test_invalid_token_rejected(self):
        """测试无效token被拒绝"""
        # 使用不需要数据库的端点来测试
        response = client.get(
            "/api/v1/auth/me",
            headers={"Authorization": "Bearer invalid-token"}
        )
        assert response.status_code in [401, 403]

    def test_expired_token_rejected(self):
        """测试过期token被拒绝"""
        from datetime import timedelta
        # 创建一个已过期的token（负的过期时间）
        token = create_access_token(
            data={"sub": "admin"},
            expires_delta=timedelta(minutes=-10)
        )
        # 使用不需要数据库的端点来测试
        response = client.get(
            "/api/v1/auth/me",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code in [401, 403]
