"""
API 速率限制中间件

使用 slowapi + limits 提供 API 速率限制功能
"""
from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from app.core.config import settings
from app.core.logger import logger

# 创建速率限制器
# 使用远程地址作为标识符
limiter = Limiter(
    key_func=get_remote_address,
    storage_uri="memory://",
    enabled=settings.rate_limit.enabled,
)

# 默认速率限制配置
DEFAULT_RATE_LIMIT = settings.rate_limit.default_limit


def setup_rate_limiter(app):
    """
    为 FastAPI 应用设置速率限制

    Args:
        app: FastAPI 应用实例
    """
    if not settings.rate_limit.enabled:
        logger.info("Rate limiting is disabled")
        return

    # 添加速率限制器到应用状态
    app.state.limiter = limiter

    # 添加异常处理器
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    # 添加中间件
    app.add_middleware(SlowAPIMiddleware)

    logger.info(f"Rate limiting enabled with default limit: {DEFAULT_RATE_LIMIT}")


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    """
    自定义速率限制超限处理器

    返回更友好的中文错误消息
    """
    return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content={
            "detail": "请求过于频繁，请稍后再试",
            "status": "error",
            "retry_after": exc.detail.split(" ")[-1] if " " in exc.detail else "60",
        },
    )


# 常用的速率限制装饰器
def limit_per_minute(times: int):
    """
    每分钟限制请求次数的装饰器

    Args:
        times: 每分钟允许的请求次数

    Usage:
        @router.get("/endpoint")
        @limit_per_minute(100)
        async def my_endpoint():
            ...
    """
    return limiter.limit(f"{times}/minute")


def limit_per_hour(times: int):
    """
    每小时限制请求次数的装饰器

    Args:
        times: 每小时允许的请求次数
    """
    return limiter.limit(f"{times}/hour")


def limit_per_day(times: int):
    """
    每天限制请求次数的装饰器

    Args:
        times: 每天允许的请求次数
    """
    return limiter.limit(f"{times}/day")


# 预定义的速率限制配置
class RateLimits:
    """预定义的速率限制配置"""

    # 轻量级查询端点
    LIGHT_QUERY = "100/minute"

    # 普通查询端点
    NORMAL_QUERY = "60/minute"

    # 重量级计算端点
    HEAVY_COMPUTE = "20/minute"

    # 任务执行端点
    TASK_EXECUTION = "10/minute"

    # 认证端点
    AUTH = "10/minute"

    # 管理操作
    ADMIN = "30/minute"
