"""
Flow Configuration Pydantic Models
"""
from typing import List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime


class TaskInDAG(BaseModel):
    """Task within a DAG"""
    id: str = Field(..., description="Task ID (e.g., sync_daily, factor_ma_20)")
    type: str = Field(..., description="Task type: sync, etl, factor, flow")
    depends_on: List[str] = Field(default_factory=list, description="Dependencies (task IDs)")
    flow_name: Optional[str] = Field(None, description="Flow name (only for type=flow)")


class FlowConfigBase(BaseModel):
    """Base Flow Configuration"""
    name: str = Field(..., description="Flow name (unique)")
    description: str = Field(default="", description="Flow description")
    cron: Optional[str] = Field(default=None, description="Cron expression (empty = manual only)")
    tags: List[str] = Field(default_factory=list, description="Tags")
    enabled: bool = Field(default=True, description="Whether flow is enabled")
    date_offset_days: int = Field(default=0, description="Date offset: -1=yesterday, 0=today, 1=tomorrow")
    tasks: List[TaskInDAG] = Field(default_factory=list, description="Tasks in DAG")


class FlowConfigCreate(FlowConfigBase):
    """Create Flow Configuration"""
    pass


class FlowConfigUpdate(BaseModel):
    """Update Flow Configuration"""
    description: Optional[str] = None
    cron: Optional[str] = None
    tags: Optional[List[str]] = None
    enabled: Optional[bool] = None
    date_offset_days: Optional[int] = None
    tasks: Optional[List[TaskInDAG]] = None


class FlowConfigInDB(FlowConfigBase):
    """Flow Configuration from Database"""
    created_at: datetime
    updated_at: datetime
    version: int

    class Config:
        from_attributes = True


class FlowConfigListItem(BaseModel):
    """Flow Configuration for List View"""
    name: str
    description: str
    cron: str
    tags: List[str]
    enabled: bool
    date_offset_days: int
    task_count: int
    updated_at: datetime
