"""
认证模块测试
"""
import pytest
from datetime import timedelta
from jose import jwt
from unittest.mock import Mock, patch

from app.core.auth import (
    create_access_token,
    get_current_user,
    get_current_active_user,
    RoleChecker,
    require_admin,
    require_user,
    SECRET_KEY,
    ALGORITHM,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    fake_users_db,
    User,
)
from app.core.config import settings


@pytest.mark.unit
class TestJWTToken:
    """JWT Token 测试"""

    def test_create_access_token_default_expiry(self):
        """测试创建默认过期时间的 token"""
        data = {"sub": "testuser"}
        token = create_access_token(data=data)
        assert token is not None
        assert len(token) > 0

    def test_create_access_token_custom_expiry(self):
        """测试创建自定义过期时间的 token"""
        data = {"sub": "testuser"}
        expires_delta = timedelta(minutes=30)
        token = create_access_token(data=data, expires_delta=expires_delta)
        assert token is not None

    def test_token_contains_subject(self):
        """测试 token 包含正确的 subject"""
        username = "testuser"
        data = {"sub": username}
        token = create_access_token(data=data)

        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        assert payload.get("sub") == username

    def test_token_has_expiry(self):
        """测试 token 包含过期时间"""
        data = {"sub": "testuser"}
        token = create_access_token(data=data)

        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        assert "exp" in payload

    def test_token_uses_config_settings(self):
        """测试 token 使用配置中的设置"""
        assert SECRET_KEY == settings.auth.secret_key
        assert ALGORITHM == settings.auth.algorithm
        assert ACCESS_TOKEN_EXPIRE_MINUTES == settings.auth.access_token_expire_minutes


@pytest.mark.unit
class TestFakeUsersDB:
    """测试模拟用户数据库"""

    def test_admin_user_exists(self):
        """测试 admin 用户存在"""
        assert "admin" in fake_users_db
        admin = fake_users_db["admin"]
        assert admin["username"] == "admin"
        assert admin["role"] == "admin"

    def test_normal_user_exists(self):
        """测试普通用户存在"""
        assert "user" in fake_users_db
        user = fake_users_db["user"]
        assert user["username"] == "user"
        assert user["role"] == "user"

    def test_user_model_validation(self):
        """测试 User 模型验证"""
        user_data = {"username": "test", "role": "user"}
        user = User(**user_data)
        assert user.username == "test"
        assert user.role == "user"

    def test_user_model_default_role(self):
        """测试 User 模型默认角色"""
        user_data = {"username": "test"}
        user = User(**user_data)
        assert user.role == "user"


@pytest.mark.unit
class TestRoleChecker:
    """角色检查器测试"""

    @pytest.mark.asyncio
    async def test_role_checker_allows_valid_role(self):
        """测试角色检查器允许有效角色"""
        checker = RoleChecker(["admin", "user"])
        user = User(username="test", role="user")

        result = await checker(user)
        assert result == user

    @pytest.mark.asyncio
    async def test_role_checker_denies_invalid_role(self):
        """测试角色检查器拒绝无效角色"""
        from fastapi import HTTPException

        checker = RoleChecker(["admin"])
        user = User(username="test", role="user")

        with pytest.raises(HTTPException) as exc_info:
            await checker(user)
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_require_admin_allows_admin(self):
        """测试 require_admin 允许 admin"""
        user = User(username="admin", role="admin")
        result = await require_admin(user)
        assert result == user

    @pytest.mark.asyncio
    async def test_require_admin_denies_user(self):
        """测试 require_admin 拒绝普通用户"""
        from fastapi import HTTPException

        user = User(username="user", role="user")
        with pytest.raises(HTTPException) as exc_info:
            await require_admin(user)
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_require_user_allows_user(self):
        """测试 require_user 允许普通用户"""
        user = User(username="user", role="user")
        result = await require_user(user)
        assert result == user

    @pytest.mark.asyncio
    async def test_require_user_allows_admin(self):
        """测试 require_user 允许 admin"""
        user = User(username="admin", role="admin")
        result = await require_user(user)
        assert result == user


@pytest.mark.unit
class TestGetCurrentUser:
    """获取当前用户测试"""

    @pytest.mark.asyncio
    async def test_get_current_user_no_credentials(self):
        """测试没有凭证时返回 None"""
        result = await get_current_user(credentials=None)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_current_user_valid_token(self):
        """测试有效 token 返回用户"""
        from fastapi.security import HTTPAuthorizationCredentials

        # 创建有效 token
        token = create_access_token(data={"sub": "admin"})
        credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)

        result = await get_current_user(credentials=credentials)
        assert result is not None
        assert result.username == "admin"
        assert result.role == "admin"

    @pytest.mark.asyncio
    async def test_get_current_user_invalid_token(self):
        """测试无效 token 抛出异常"""
        from fastapi import HTTPException
        from fastapi.security import HTTPAuthorizationCredentials

        credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials="invalid_token")

        with pytest.raises(HTTPException) as exc_info:
            await get_current_user(credentials=credentials)
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_get_current_user_nonexistent_user(self):
        """测试不存在的用户抛出异常"""
        from fastapi import HTTPException
        from fastapi.security import HTTPAuthorizationCredentials

        token = create_access_token(data={"sub": "nonexistent"})
        credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)

        with pytest.raises(HTTPException) as exc_info:
            await get_current_user(credentials=credentials)
        assert exc_info.value.status_code == 401


@pytest.mark.unit
class TestGetCurrentActiveUser:
    """获取当前活跃用户测试"""

    @pytest.mark.asyncio
    async def test_get_current_active_user_with_user(self):
        """测试有用户时返回用户"""
        user = User(username="test", role="user")
        result = await get_current_active_user(current_user=user)
        assert result == user

    @pytest.mark.asyncio
    async def test_get_current_active_user_no_user(self):
        """测试没有用户时抛出异常"""
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            await get_current_active_user(current_user=None)
        assert exc_info.value.status_code == 401


@pytest.mark.integration
class TestAuthEndpoints:
    """认证端点集成测试"""

    def test_login_endpoint_success(self, client):
        """测试登录端点成功"""
        response = client.post(
            "/api/v1/auth/token",
            data={"username": "admin", "password": "notused"}  # password not validated in fake db
        )
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"

    def test_login_endpoint_invalid_user(self, client):
        """测试登录端点无效用户"""
        response = client.post(
            "/api/v1/auth/token",
            data={"username": "nonexistent", "password": "test"}
        )
        assert response.status_code == 401

    def test_get_me_endpoint_unauthenticated(self, client):
        """测试未认证时获取用户信息"""
        response = client.get("/api/v1/auth/me")
        assert response.status_code == 401

    def test_get_me_endpoint_authenticated(self, client):
        """测试认证后获取用户信息"""
        # 先登录获取 token
        login_response = client.post(
            "/api/v1/auth/token",
            data={"username": "admin", "password": "notused"}
        )
        token = login_response.json()["access_token"]

        # 使用 token 访问
        response = client.get(
            "/api/v1/auth/me",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "admin"
        assert data["role"] == "admin"
