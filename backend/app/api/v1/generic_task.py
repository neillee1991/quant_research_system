"""
通用任务 API 路由工厂
提供统一的 CRUD 路由生成
"""
from typing import TypeVar, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.models.base_task import BaseTaskConfig
from app.services.task_service import TaskService
from app.core.logger import logger

T = TypeVar('T', bound=BaseTaskConfig)


class TaskCreateRequest(BaseModel):
    """创建任务请求"""
    config_data: Dict[str, Any] = Field(..., description="任务配置数据")
    changed_by: str = Field(default="api", description="修改人")
    change_reason: str = Field(default="Create new task", description="修改原因")


class TaskUpdateRequest(BaseModel):
    """更新任务请求"""
    config_data: Dict[str, Any] = Field(..., description="更新的配置数据")
    changed_by: str = Field(default="api", description="修改人")
    change_reason: str = Field(default="Update task", description="修改原因")


class TaskDeleteRequest(BaseModel):
    """删除任务请求"""
    changed_by: str = Field(default="api", description="修改人")
    change_reason: str = Field(default="Delete task", description="修改原因")


class TaskListResponse(BaseModel):
    """任务列表响应"""
    tasks: List[Dict[str, Any]]
    total: int


class TaskResponse(BaseModel):
    """单个任务响应"""
    task: Dict[str, Any]


class DeleteResponse(BaseModel):
    """删除响应"""
    success: bool
    message: str


def create_task_router(
    service: TaskService[T],
    prefix: str,
    tags: List[str]
) -> APIRouter:
    """
    创建通用任务路由

    Args:
        service: 任务服务实例
        prefix: 路由前缀 (如 /api/v1/sync)
        tags: API 标签

    Returns:
        配置好的 APIRouter
    """
    router = APIRouter(prefix=prefix, tags=tags)

    @router.get("/tasks", response_model=TaskListResponse)
    def list_tasks(
        enabled_only: bool = Query(default=False, description="是否只返回启用的任务")
    ):
        """列出所有任务"""
        try:
            tasks = service.list_tasks(enabled_only=enabled_only)
            task_dicts = [task.model_dump() for task in tasks]
            return TaskListResponse(tasks=task_dicts, total=len(task_dicts))
        except Exception as e:
            logger.error(f"Failed to list {service.task_type} tasks: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/tasks/{task_id}", response_model=TaskResponse)
    def get_task(task_id: str):
        """获取单个任务"""
        try:
            task = service.get_task(task_id)
            if not task:
                raise HTTPException(
                    status_code=404,
                    detail=f"Task {task_id} not found"
                )
            return TaskResponse(task=task.model_dump())
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to get {service.task_type} task {task_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/tasks", response_model=TaskResponse)
    def create_task(request: TaskCreateRequest):
        """创建新任务"""
        try:
            task = service.create_task(
                config_data=request.config_data,
                changed_by=request.changed_by,
                change_reason=request.change_reason
            )
            return TaskResponse(task=task.model_dump())
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Failed to create {service.task_type} task: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.put("/tasks/{task_id}", response_model=TaskResponse)
    def update_task(task_id: str, request: TaskUpdateRequest):
        """更新任务"""
        try:
            task = service.update_task(
                task_id=task_id,
                config_data=request.config_data,
                changed_by=request.changed_by,
                change_reason=request.change_reason
            )
            return TaskResponse(task=task.model_dump())
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Failed to update {service.task_type} task {task_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.delete("/tasks/{task_id}", response_model=DeleteResponse)
    def delete_task(task_id: str, request: TaskDeleteRequest = None):
        """删除任务（软删除）"""
        try:
            if request is None:
                request = TaskDeleteRequest()

            success = service.delete_task(
                task_id=task_id,
                changed_by=request.changed_by,
                change_reason=request.change_reason
            )
            return DeleteResponse(
                success=success,
                message=f"Task {task_id} deleted successfully"
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Failed to delete {service.task_type} task {task_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    return router
