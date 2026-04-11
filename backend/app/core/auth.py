"""JWT 认证中间件和工具"""
from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

from app.core.config import settings

# JWT 配置
SECRET_KEY = settings.auth.secret_key
ALGORITHM = settings.auth.algorithm
ACCESS_TOKEN_EXPIRE_MINUTES = settings.auth.access_token_expire_minutes

# 密码哈希
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Bearer token 安全方案
security = HTTPBearer(auto_error=False)


class TokenData(BaseModel):
    username: Optional[str] = None


class User(BaseModel):
    username: str
    role: str = "user"


# 模拟用户数据库（实际应从 PostgreSQL 查询）
fake_users_db = {
    "admin": {"username": "admin", "role": "admin"},
    "user": {"username": "user", "role": "user"},
}


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> Optional[User]:
    """获取当前用户（可选认证）"""
    if credentials is None:
        return None

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="无法验证凭据",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception

    user = fake_users_db.get(token_data.username)
    if user is None:
        raise credentials_exception
    return User(**user)


async def get_current_active_user(
    current_user: Optional[User] = Depends(get_current_user),
) -> User:
    """获取当前活跃用户（必需认证）"""
    if current_user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="需要认证",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return current_user


class RoleChecker:
    def __init__(self, allowed_roles: list[str]):
        self.allowed_roles = allowed_roles

    async def __call__(
        self, current_user: User = Depends(get_current_active_user),
    ) -> User:
        if current_user.role not in self.allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="权限不足"
            )
        return current_user


# 预定义的角色检查器
require_admin = RoleChecker(["admin"])
require_user = RoleChecker(["user", "admin"])
