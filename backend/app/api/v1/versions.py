"""
版本管理 API
提供任务配置版本控制功能
"""
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.logger import logger
from store.dolphindb_client import db_client


router = APIRouter()


class VersionCreateRequest(BaseModel):
    """创建版本请求"""
    config_data: Dict[str, Any] = Field(..., description="配置数据")
    changed_by: str = Field(default="api", description="修改人")
    change_reason: str = Field(default="", description="修改原因")


class VersionResponse(BaseModel):
    """版本响应"""
    version_number: int
    message: str


class RollbackRequest(BaseModel):
    """回滚请求"""
    changed_by: str = Field(default="api", description="修改人")
    change_reason: str = Field(default="Rollback", description="修改原因")


@router.post("/tasks/{task_type}/versions", response_model=VersionResponse)
def create_task_version(
    task_type: str,
    request: VersionCreateRequest
):
    """创建任务配置新版本

    Args:
        task_type: 任务类型 (sync/etl/factor)
        request: 版本创建请求

    Returns:
        新版本号和消息
    """
    try:
        # 从 config_data 中提取 task_id
        id_field = "task_id" if task_type in ["sync", "etl"] else "factor_id"
        task_id = request.config_data.get(id_field)

        if not task_id:
            raise HTTPException(
                status_code=400,
                detail=f"Missing {id_field} in config_data"
            )

        version = db_client.create_task_version(
            task_type=task_type,
            task_id=task_id,
            config_data=request.config_data,
            changed_by=request.changed_by,
            change_reason=request.change_reason
        )

        return VersionResponse(
            version_number=version,
            message=f"Created version {version} for {task_type} task {task_id}"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create task version: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_type}/{task_id}/versions")
def get_task_versions(
    task_type: str,
    task_id: str
):
    """获取任务的所有版本历史

    Args:
        task_type: 任务类型 (sync/etl/factor)
        task_id: 任务ID

    Returns:
        版本历史列表
    """
    try:
        df = db_client.get_task_versions(task_type, task_id)

        if df.is_empty():
            return {"versions": [], "total": 0}

        versions = df.to_dicts()
        return {
            "versions": versions,
            "total": len(versions)
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to get task versions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_type}/{task_id}/versions/{version}")
def get_task_version(
    task_type: str,
    task_id: str,
    version: int
):
    """获取任务的特定版本

    Args:
        task_type: 任务类型 (sync/etl/factor)
        task_id: 任务ID
        version: 版本号

    Returns:
        版本配置
    """
    try:
        config = db_client.get_task_version(task_type, task_id, version)

        if config is None:
            raise HTTPException(
                status_code=404,
                detail=f"Version {version} not found for {task_type} task {task_id}"
            )

        return config

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get task version: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_type}/{task_id}/current")
def get_current_task_version(
    task_type: str,
    task_id: str
):
    """获取任务的当前版本

    Args:
        task_type: 任务类型 (sync/etl/factor)
        task_id: 任务ID

    Returns:
        当前版本配置
    """
    try:
        config = db_client.get_current_task_version(task_type, task_id)

        if config is None:
            raise HTTPException(
                status_code=404,
                detail=f"No current version found for {task_type} task {task_id}"
            )

        return config

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get current task version: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_type}/{task_id}/rollback/{version}", response_model=VersionResponse)
def rollback_task_version(
    task_type: str,
    task_id: str,
    version: int,
    request: RollbackRequest
):
    """回滚任务到指定版本

    Args:
        task_type: 任务类型 (sync/etl/factor)
        task_id: 任务ID
        version: 目标版本号
        request: 回滚请求

    Returns:
        新版本号和消息
    """
    try:
        new_version = db_client.rollback_task_version(
            task_type=task_type,
            task_id=task_id,
            target_version=version,
            changed_by=request.changed_by,
            change_reason=request.change_reason
        )

        return VersionResponse(
            version_number=new_version,
            message=f"Rolled back {task_type} task {task_id} to version {version} (new version: {new_version})"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to rollback task version: {e}")
        raise HTTPException(status_code=500, detail=str(e))
