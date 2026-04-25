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
    """将前端发送的字段名规范化为模型期望的字段名。

    前端可能发送 primary_keys (list) 和 schema (dict)，
    模型期望 primary_keys_json (list) 和 schema_json (dict)。
    """
    data = dict(config_data)
    if "primary_keys" in data and "primary_keys_json" not in data:
        data["primary_keys_json"] = data.pop("primary_keys")
    if "schema" in data and "schema_json" not in data:
        data["schema_json"] = data.pop("schema")
    data.pop("confirm_schema_change", None)
    return data


def _parse_task_config(task_type: str, config_data: Dict[str, Any]) -> TaskConfigUnion:
    model_cls = _TASK_CONFIG_MODELS.get(task_type)
    if not model_cls:
        raise HTTPException(status_code=400, detail=f"Invalid task_type: {task_type}")
    return model_cls(**_normalize_task_config(config_data))


def _format_task_row(row: dict) -> dict:
    from zoneinfo import ZoneInfo
    _TZ = ZoneInfo("Asia/Shanghai")
    for field in ["started_at", "finished_at", "created_at"]:
        if field in row and row[field] is not None:
            val = row[field]
            if hasattr(val, 'tzinfo'):
                if val.tzinfo is None:
                    val = val.replace(tzinfo=_TZ)
                row[field] = val.isoformat()
            else:
                row[field] = str(val)
    return row
