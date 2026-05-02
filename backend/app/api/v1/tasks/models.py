"""
任务管理 Pydantic 模型 + 辅助函数
"""
from typing import Dict, Any, Optional, Union
from fastapi import HTTPException
from pydantic import BaseModel, Field

from app.models.base_task import SyncTaskConfig, ETLTaskConfig, FactorConfig
from app.services.task_service import sync_service, etl_service, factor_service, TaskService

TaskConfigUnion = Union[SyncTaskConfig, ETLTaskConfig, FactorConfig]

_TASK_CONFIG_MODELS = {
    "sync": SyncTaskConfig,
    "etl": ETLTaskConfig,
    "factor": FactorConfig,
}


class TaskCreateRequest(BaseModel):
    config_data: Dict[str, Any] = Field(..., description="任务配置数据")


class TaskUpdateRequest(BaseModel):
    config_data: Dict[str, Any] = Field(..., description="更新的配置数据")


class TaskExecuteRequest(BaseModel):
    start_date: Optional[str] = Field(default=None, description="开始日期 (YYYYMMDD)")
    end_date: Optional[str] = Field(default=None, description="结束日期 (YYYYMMDD)")
    params: Dict[str, Any] = Field(default_factory=dict, description="执行参数")
    flow_run_id: Optional[int] = Field(default=None, description="关联的 flow_run_id")
    run_id: Optional[str] = Field(default=None, description="预生成的 run_id")


class TaskListResponse(BaseModel):
    tasks: list[Dict[str, Any]]
    total: int
    task_type: str


class TaskResponse(BaseModel):
    task: Dict[str, Any]
    task_type: str


class TaskExecuteResponse(BaseModel):
    status: str
    message: str
    task_id: str
    task_type: str
    result: Optional[Dict[str, Any]] = None


class DeleteResponse(BaseModel):
    success: bool
    message: str
    task_id: str
    task_type: str


class DataInspectionResponse(BaseModel):
    table_name: str
    exists: bool
    has_data: Optional[bool] = None
    date_field: Optional[str] = None
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    actual_dates: Optional[int] = None
    expected_dates: Optional[int] = None
    missing_dates: Optional[list[str]] = None
    missing_count: Optional[int] = None
    coverage_percent: Optional[float] = None
    trading_calendar_available: Optional[bool] = None
    message: Optional[str] = None


class RunningTaskResponse(BaseModel):
    tasks: list[Dict[str, Any]]
    total: int


class TaskHistoryResponse(BaseModel):
    tasks: list[Dict[str, Any]]
    total: int


def _get_service(task_type: str) -> TaskService:
    services = {"sync": sync_service, "etl": etl_service, "factor": factor_service}
    service = services.get(task_type)
    if not service:
        raise HTTPException(status_code=400, detail=f"Invalid task_type: {task_type}. Must be one of: sync, etl, factor")
    return service


def _normalize_task_config(config_data: Dict[str, Any]) -> Dict[str, Any]:
    """清理前端发送的额外字段"""
    data = dict(config_data)
    data.pop("confirm_schema_change", None)
    return data


def _parse_task_config(task_type: str, config_data: Dict[str, Any]) -> TaskConfigUnion:
    model_cls = _TASK_CONFIG_MODELS.get(task_type)
    if not model_cls:
        raise HTTPException(status_code=400, detail=f"Invalid task_type: {task_type}")
    return model_cls(**_normalize_task_config(config_data))


def _format_task_row(row: dict) -> dict:
    import json
    from zoneinfo import ZoneInfo
    _TZ = ZoneInfo("Asia/Shanghai")
    result = dict(row)
    for field in ["started_at", "finished_at", "created_at"]:
        if field in result and result[field] is not None:
            val = result[field]
            if hasattr(val, 'tzinfo'):
                if val.tzinfo is None:
                    val = val.replace(tzinfo=_TZ)
                result[field] = val.isoformat()
            else:
                result[field] = str(val)
    for field in ["params", "extra"]:
        if field in result and isinstance(result[field], str) and result[field]:
            try:
                result[field] = json.loads(result[field])
            except (json.JSONDecodeError, ValueError):
                pass
    # 将数据库字段名映射到前端期望的字段名
    if "error_message" in result and "error" not in result:
        result["error"] = result["error_message"]
    elif "error_message" in result and "error" in result:
        if not result["error"]:
            result["error"] = result["error_message"]
    # 确保 rows 字段是整数类型
    if "rows" in result and result["rows"] is not None:
        result["rows"] = int(result["rows"])
    return result
