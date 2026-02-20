"""
自定义异常层次结构
提供详细的错误信息和上下文，便于调试和错误处理
"""
from typing import Any, Dict, Optional
from fastapi import Request, status
from fastapi.responses import JSONResponse

from app.core.logger import logger


class QuantException(Exception):
    """基础异常类"""

    def __init__(
        self,
        message: str,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.status_code = status_code
        self.detail = detail or message
        self.context = context or {}
        super().__init__(self.message)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "detail": self.detail,
            "context": self.context
        }


# ==================== 数据相关异常 ====================

class DataException(QuantException):
    """数据相关异常基类"""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            **kwargs
        )


class DataCollectionError(DataException):
    """数据采集失败"""

    def __init__(self, source: str, reason: str, **kwargs):
        super().__init__(
            message=f"Failed to collect data from {source}: {reason}",
            context={"source": source, "reason": reason, **kwargs.get("context", {})}
        )


class DataNotFoundError(DataException):
    """数据不存在"""

    def __init__(self, resource: str, identifier: Optional[str] = None, **kwargs):
        message = f"Data not found: {resource}"
        if identifier:
            message += f" (identifier: {identifier})"
        super().__init__(
            message,
            status_code=status.HTTP_404_NOT_FOUND,
            context={"resource": resource, "identifier": identifier, **kwargs.get("context", {})}
        )


class DataValidationError(DataException):
    """数据验证失败"""

    def __init__(self, field: str, reason: str, **kwargs):
        super().__init__(
            message=f"Data validation failed for field '{field}': {reason}",
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            context={"field": field, "reason": reason, **kwargs.get("context", {})}
        )


# ==================== 同步相关异常 ====================

class SyncException(QuantException):
    """同步相关异常基类"""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            **kwargs
        )


class SyncTaskNotFoundError(SyncException):
    """同步任务不存在"""

    def __init__(self, task_id: str, **kwargs):
        super().__init__(
            message=f"Sync task not found: {task_id}",
            status_code=status.HTTP_404_NOT_FOUND,
            context={"task_id": task_id, **kwargs.get("context", {})}
        )


class SyncConfigError(SyncException):
    """同步配置错误"""

    def __init__(self, reason: str, **kwargs):
        super().__init__(
            message=f"Sync configuration error: {reason}",
            status_code=status.HTTP_400_BAD_REQUEST,
            context={"reason": reason, **kwargs.get("context", {})}
        )


class RateLimitExceededError(SyncException):
    """速率限制超出"""

    def __init__(self, limit: int, period: int, **kwargs):
        super().__init__(
            message=f"Rate limit exceeded: {limit} calls per {period} seconds",
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            context={"limit": limit, "period": period, **kwargs.get("context", {})}
        )


# ==================== 回测相关异常 ====================

class BacktestException(QuantException):
    """回测相关异常基类"""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            **kwargs
        )


class BacktestConfigError(BacktestException):
    """回测配置错误"""

    def __init__(self, reason: str, **kwargs):
        super().__init__(
            message=f"Backtest configuration error: {reason}",
            status_code=status.HTTP_400_BAD_REQUEST,
            context={"reason": reason, **kwargs.get("context", {})}
        )


class InsufficientDataError(BacktestException):
    """数据不足"""

    def __init__(self, required: int, actual: int, **kwargs):
        super().__init__(
            message=f"Insufficient data for backtest: required {required}, got {actual}",
            status_code=status.HTTP_400_BAD_REQUEST,
            context={"required": required, "actual": actual, **kwargs.get("context", {})}
        )


# ==================== 因子相关异常 ====================

class FactorException(QuantException):
    """因子相关异常基类"""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            **kwargs
        )


class FactorComputationError(FactorException):
    """因子计算失败"""

    def __init__(self, factor_name: str, reason: str, **kwargs):
        super().__init__(
            message=f"Failed to compute factor '{factor_name}': {reason}",
            context={"factor_name": factor_name, "reason": reason, **kwargs.get("context", {})}
        )


class UnsupportedFactorError(FactorException):
    """不支持的因子"""

    def __init__(self, factor_name: str, **kwargs):
        super().__init__(
            message=f"Unsupported factor: {factor_name}",
            status_code=status.HTTP_400_BAD_REQUEST,
            context={"factor_name": factor_name, **kwargs.get("context", {})}
        )


# ==================== 机器学习相关异常 ====================

class MLException(QuantException):
    """机器学习相关异常基类"""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            **kwargs
        )


class ModelTrainingError(MLException):
    """模型训练失败"""

    def __init__(self, model_name: str, reason: str, **kwargs):
        super().__init__(
            message=f"Failed to train model '{model_name}': {reason}",
            context={"model_name": model_name, "reason": reason, **kwargs.get("context", {})}
        )


class ModelNotFoundError(MLException):
    """模型不存在"""

    def __init__(self, model_id: str, **kwargs):
        super().__init__(
            message=f"Model not found: {model_id}",
            status_code=status.HTTP_404_NOT_FOUND,
            context={"model_id": model_id, **kwargs.get("context", {})}
        )


# ==================== 配置相关异常 ====================

class ConfigurationError(QuantException):
    """配置错误"""

    def __init__(self, key: str, reason: str, **kwargs):
        super().__init__(
            message=f"Configuration error for '{key}': {reason}",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            context={"key": key, "reason": reason, **kwargs.get("context", {})}
        )


# ==================== 异常处理器 ====================

async def quant_exception_handler(request: Request, exc: QuantException) -> JSONResponse:
    """全局异常处理器"""
    logger.error(
        f"{exc.__class__.__name__}: {exc.message}",
        extra={
            "path": request.url.path,
            "method": request.method,
            "context": exc.context
        }
    )

    return JSONResponse(
        status_code=exc.status_code,
        content=exc.to_dict()
    )


async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """通用异常处理器"""
    logger.exception(
        f"Unhandled exception: {str(exc)}",
        extra={
            "path": request.url.path,
            "method": request.method
        }
    )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "InternalServerError",
            "message": "An unexpected error occurred",
            "detail": str(exc) if logger.level == "DEBUG" else "Internal server error"
        }
    )
