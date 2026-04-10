"""
调度器数据模型
"""
from enum import Enum
from typing import Optional, List, Any
from datetime import datetime
from pydantic import BaseModel, Field


class FlowStatus(str, Enum):
    """Flow 执行状态"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskStatus(str, Enum):
    """Task 执行状态"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class TriggerType(str, Enum):
    """触发类型"""
    CRON = "cron"
    MANUAL = "manual"
    PARENT_FLOW = "parent_flow"


class FlowRun(BaseModel):
    """Flow 执行记录"""
    id: Optional[int] = None
    flow_name: str
    parent_flow_run_id: Optional[int] = None
    status: FlowStatus = FlowStatus.PENDING
    trigger_type: TriggerType
    target_date: Optional[str] = None
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None


class TaskRun(BaseModel):
    """Task 执行记录"""
    id: Optional[int] = None
    flow_run_id: int
    task_id: str
    task_type: str  # sync/etl/factor/flow
    status: TaskStatus = TaskStatus.PENDING
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
