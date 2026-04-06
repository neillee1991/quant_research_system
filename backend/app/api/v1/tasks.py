"""
统一任务管理 API 路由
提供跨任务类型的统一 RESTful API 端点
"""
from typing import Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Query, Path, BackgroundTasks
from pydantic import BaseModel, Field
import time
import json

from app.services.task_service import sync_service, etl_service, factor_service, TaskService
from app.services.task_runner import TaskRunner, tracked_task
from app.core.logger import logger

router = APIRouter()


# ==================== 请求/响应模型 ====================

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
    drop_table: bool = Field(default=False, description="是否删除关联表")
    changed_by: str = Field(default="api", description="修改人")
    change_reason: str = Field(default="Delete task", description="修改原因")


class TaskExecuteRequest(BaseModel):
    """执行任务请求"""
    start_date: Optional[str] = Field(default=None, description="开始日期 (YYYYMMDD)")
    end_date: Optional[str] = Field(default=None, description="结束日期 (YYYYMMDD)")
    params: Dict[str, Any] = Field(default_factory=dict, description="执行参数")


class TaskListResponse(BaseModel):
    """任务列表响应"""
    tasks: list[Dict[str, Any]]
    total: int
    task_type: str


class TaskResponse(BaseModel):
    """单个任务响应"""
    task: Dict[str, Any]
    task_type: str


class TaskExecuteResponse(BaseModel):
    """任务执行响应"""
    status: str
    message: str
    task_id: str
    task_type: str
    result: Optional[Dict[str, Any]] = None


class DeleteResponse(BaseModel):
    """删除响应"""
    success: bool
    message: str
    task_id: str
    task_type: str


class VersionResponse(BaseModel):
    """版本响应"""
    version: int
    message: str


class DataInspectionResponse(BaseModel):
    """数据探查响应"""
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


# ==================== 辅助函数 ====================

def _get_service(task_type: str) -> TaskService:
    """根据任务类型获取对应的服务"""
    services = {
        "sync": sync_service,
        "etl": etl_service,
        "factor": factor_service,
    }

    service = services.get(task_type)
    if not service:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid task_type: {task_type}. Must be one of: sync, etl, factor"
        )

    return service


# ==================== API 端点 ====================

class RunningTaskResponse(BaseModel):
    """正在运行的任务响应"""
    tasks: list[Dict[str, Any]]
    total: int


class TaskHistoryResponse(BaseModel):
    """历史任务响应"""
    tasks: list[Dict[str, Any]]
    total: int


@router.get("/tasks/running", response_model=RunningTaskResponse)
def get_running_tasks(
    task_type: Optional[str] = Query(default=None, description="按任务类型过滤 (sync/etl/factor)"),
    task_id: Optional[str] = Query(default=None, description="按任务ID过滤")
):
    """获取所有正在运行的任务（查询统一 task_runs 表）"""
    try:
        from store.dolphindb_client import db_client

        try:
            where_conditions = ["status = 'running'"]
            params = []

            if task_type:
                where_conditions.append("task_type = %s")
                params.append(task_type)

            if task_id:
                where_conditions.append("task_id = %s")
                params.append(task_id)

            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

            df = db_client.query(f"""
                SELECT * FROM task_runs
                WHERE {where_clause}
                ORDER BY started_at DESC
            """, tuple(params) if params else None)
            tasks = []
            if not df.is_empty():
                for row in df.to_dicts():
                    for field in ["started_at", "finished_at"]:
                        if field in row and row[field]:
                            row[field] = str(row[field])
                    tasks.append(row)
        except Exception as e:
            logger.warning(f"task_runs not available, falling back: {e}")
            tasks = []

        return RunningTaskResponse(tasks=tasks, total=len(tasks))

    except Exception as e:
        logger.error(f"Failed to get running tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/history", response_model=TaskHistoryResponse)
def get_task_history(
    limit: int = Query(default=50, ge=1, le=200),
    task_type: Optional[str] = Query(default=None, description="按任务类型过滤 (sync/etl/factor)"),
    task_id: Optional[str] = Query(default=None, description="按任务ID过滤"),
    start_date: Optional[str] = Query(default=None, description="开始日期 (YYYYMMDD)，按 started_at 过滤"),
    end_date: Optional[str] = Query(default=None, description="结束日期 (YYYYMMDD)，按 started_at 过滤"),
):
    """获取最近完成/失败的任务历史（查询统一 task_runs 表）"""
    try:
        from store.dolphindb_client import db_client

        try:
            where_conditions = ["status IN ('success', 'failed')"]
            params = []

            if task_type:
                where_conditions.append("task_type = %s")
                params.append(task_type)

            if task_id:
                where_conditions.append("task_id = %s")
                params.append(task_id)

            if start_date:
                where_conditions.append("started_at >= temporalParse(%s, 'yyyyMMdd')")
                params.append(start_date)

            if end_date:
                where_conditions.append("started_at < temporalParse(%s, 'yyyyMMdd') + 1")
                params.append(end_date)

            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

            df = db_client.query(f"""
                SELECT * FROM task_runs
                WHERE {where_clause}
                ORDER BY finished_at DESC
                LIMIT {limit}
            """, tuple(params) if params else None)
            tasks = []
            if not df.is_empty():
                for row in df.to_dicts():
                    for field in ["started_at", "finished_at"]:
                        if field in row and row[field]:
                            row[field] = str(row[field])
                    tasks.append(row)
        except Exception as e:
            logger.warning(f"task_runs not available: {e}")
            tasks = []

        return TaskHistoryResponse(tasks=tasks, total=len(tasks))

    except Exception as e:
        logger.error(f"Failed to get task history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/cleanup")
def cleanup_stale_tasks(timeout_minutes: int = Query(default=0, ge=0, description="超时分钟数，0=清理所有running")):
    """清理僵尸任务（将长时间 running 的记录标记为 failed）"""
    try:
        from app.services.task_runner import TaskRunner
        cleaned = TaskRunner.cleanup_stale(timeout_minutes=timeout_minutes, reason=f"manual cleanup (timeout={timeout_minutes}min)")
        return {"status": "success", "data": {"cleaned": cleaned}}
    except Exception as e:
        logger.error(f"Failed to cleanup stale tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/version", response_model=VersionResponse)
def get_config_version():
    """获取配置版本号（基于最新更新时间的时间戳）"""
    try:
        from store.dolphindb_client import db_client

        # 查询所有配置表的最新更新时间
        tables = ["sync_task_config", "etl_task_config", "factor_metadata"]
        max_timestamp = 0

        for table in tables:
            try:
                sql = f"SELECT max(updated_at) as max_time FROM {table}"
                df = db_client.query(sql)
                if not df.is_empty():
                    max_time = df["max_time"][0]
                    if max_time:
                        timestamp = int(max_time.timestamp() * 1000)
                        max_timestamp = max(max_timestamp, timestamp)
            except Exception as e:
                logger.warning(f"Failed to get version from {table}: {e}")

        return VersionResponse(
            version=max_timestamp,
            message=f"Configuration version: {max_timestamp}"
        )

    except Exception as e:
        logger.error(f"Failed to get config version: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_type}", response_model=TaskListResponse)
def list_tasks(
    task_type: str = Path(..., description="任务类型: sync, etl, factor"),
    enabled_only: bool = Query(default=False, description="是否只返回启用的任务")
):
    """列出所有任务"""
    try:
        service = _get_service(task_type)
        tasks = service.list_tasks(enabled_only=enabled_only)
        task_dicts = [task.model_dump() for task in tasks]

        return TaskListResponse(
            tasks=task_dicts,
            total=len(task_dicts),
            task_type=task_type
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list {task_type} tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_type}/status/{run_id}")
async def get_task_status(
    task_type: str = Path(..., description="任务类型: sync, etl, factor"),
    run_id: str = Path(..., description="运行ID")
):
    """查询任务执行状态（查询统一 task_runs 表）"""
    try:
        from store.dolphindb_client import db_client

        # 从统一 task_runs 表查询
        df = db_client.query("""
            SELECT * FROM task_runs
            WHERE run_id = %s
            ORDER BY started_at DESC
            LIMIT 1
        """, (run_id,))

        if df.is_empty():
            raise HTTPException(
                status_code=404,
                detail=f"Task status not found for run_id: {run_id}"
            )

        record = df.to_dicts()[0]

        # 格式化时间戳（处理可能缺失的字段）
        for field in ["created_at", "updated_at", "started_at", "finished_at"]:
            if field in record and record[field]:
                record[field] = str(record[field])
            elif field not in record:
                record[field] = None

        return {"status": "success", "data": record}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get task status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_type}/{task_id}", response_model=TaskResponse)
def get_task(
    task_type: str = Path(..., description="任务类型: sync, etl, factor"),
    task_id: str = Path(..., description="任务ID")
):
    """获取单个任务"""
    try:
        service = _get_service(task_type)
        task = service.get_task(task_id)

        if not task:
            raise HTTPException(
                status_code=404,
                detail=f"Task {task_id} not found in {task_type}"
            )

        return TaskResponse(
            task=task.model_dump(),
            task_type=task_type
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get {task_type} task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_type}", response_model=TaskResponse)
def create_task(
    task_type: str = Path(..., description="任务类型: sync, etl, factor"),
    request: TaskCreateRequest = None
):
    """创建新任务"""
    try:
        service = _get_service(task_type)
        task = service.create_task(
            config_data=request.config_data,
            changed_by=request.changed_by,
            change_reason=request.change_reason
        )

        return TaskResponse(
            task=task.model_dump(),
            task_type=task_type
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create {task_type} task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/tasks/{task_type}/{task_id}", response_model=TaskResponse)
def update_task(
    task_type: str = Path(..., description="任务类型: sync, etl, factor"),
    task_id: str = Path(..., description="任务ID"),
    request: TaskUpdateRequest = None
):
    """更新任务"""
    try:
        service = _get_service(task_type)
        task = service.update_task(
            task_id=task_id,
            config_data=request.config_data,
            changed_by=request.changed_by,
            change_reason=request.change_reason
        )

        return TaskResponse(
            task=task.model_dump(),
            task_type=task_type
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to update {task_type} task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/tasks/{task_type}/{task_id}", response_model=DeleteResponse)
def delete_task(
    task_type: str = Path(..., description="任务类型: sync, etl, factor"),
    task_id: str = Path(..., description="任务ID"),
    drop_table: bool = Query(default=False, description="是否删除关联表"),
    changed_by: str = Query(default="api", description="修改人"),
    change_reason: str = Query(default="Delete task", description="修改原因")
):
    """删除任务（软删除，设置 enabled=false）"""
    try:
        service = _get_service(task_type)

        # 如果需要删除表，先处理
        if drop_table:
            task = service.get_task(task_id)
            if task:
                table_name = None
                if task_type == "sync":
                    table_name = getattr(task, "table_name", None)
                elif task_type == "etl":
                    table_name = getattr(task, "table_name", None)
                elif task_type == "factor":
                    # 因子使用统一的 factor_values 表，不删除
                    logger.warning(f"Factor tasks use shared table, skipping drop_table for {task_id}")

                if table_name:
                    from store.dolphindb_client import db_client
                    try:
                        db_client.drop_table(table_name)
                        logger.info(f"Dropped table {table_name} for task {task_id}")
                    except Exception as e:
                        logger.warning(f"Failed to drop table {table_name}: {e}")

        # 执行软删除
        success = service.delete_task(
            task_id=task_id,
            changed_by=changed_by,
            change_reason=change_reason
        )

        return DeleteResponse(
            success=success,
            message=f"Task {task_id} deleted successfully" + (" (table dropped)" if drop_table else ""),
            task_id=task_id,
            task_type=task_type
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete {task_type} task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@tracked_task("sync", task_id_kwarg="task_id")
def _execute_sync_task_background(task_id: str, start_date: Optional[str], end_date: Optional[str], run_id: str):
    """后台执行同步任务"""
    try:
        from data_manager.refactored_sync_engine import sync_engine
        result = sync_engine.sync_task(
            task_id=task_id,
            start_date=start_date,
            end_date=end_date
        )
        logger.info(f"Sync task {task_id} completed: run_id={run_id}, success={result}")
    except Exception as e:
        logger.error(f"Sync task {task_id} failed: run_id={run_id}, error={e}")


@tracked_task("etl", task_id_kwarg="task_id")
def _execute_etl_task_background(task_id: str, start_date: Optional[str], end_date: Optional[str], run_id: str):
    """后台执行 ETL 任务"""
    try:
        from data_manager.etl_engine import etl_engine
        result = etl_engine.run_etl_task(
            task_id=task_id,
            start_date=start_date,
            end_date=end_date
        )
        logger.info(f"ETL task {task_id} completed: run_id={run_id}, success={result}")
    except Exception as e:
        logger.error(f"ETL task {task_id} failed: run_id={run_id}, error={e}")


@tracked_task("factor", task_id_kwarg="task_id")
def _execute_factor_task_background(task_id: str, start_date: Optional[str], end_date: Optional[str], run_id: str):
    """后台执行因子任务"""
    try:
        from app.services.factor_compute_service import FactorComputeService
        from store.dolphindb_client import db_client
        service = FactorComputeService(db_client)
        compute_result = service.compute_factor(
            factor_id=task_id,
            start_date=start_date,
            end_date=end_date,
            mode="full" if start_date else "incremental"
        )
        logger.info(f"Factor task {task_id} completed: run_id={run_id}, success={compute_result.success}, rows={compute_result.rows}")
    except Exception as e:
        logger.error(f"Factor task {task_id} failed: run_id={run_id}, error={e}")


@router.post("/tasks/{task_type}/{task_id}/execute", response_model=TaskExecuteResponse)
async def execute_task(
    task_type: str = Path(..., description="任务类型: sync, etl, factor"),
    task_id: str = Path(..., description="任务ID"),
    request: TaskExecuteRequest = None,
    background_tasks: BackgroundTasks = None
):
    """执行任务（异步）

    立即返回 run_id，后台执行任务。使用 /tasks/{task_type}/status/{run_id} 查询状态。
    """
    try:
        service = _get_service(task_type)

        # 验证任务存在
        task = service.get_task(task_id)
        if not task:
            raise HTTPException(
                status_code=404,
                detail=f"Task {task_id} not found in {task_type}"
            )

        # 生成 run_id
        run_id = f"{task_id}_{int(time.time() * 1000)}"

        # Write to task_runs table
        TaskRunner.start(
            run_id,
            task_type,
            task_id,
            f"{task_type.upper()} 任务: {task_id}",
            params=json.dumps(request.params if request else {})
        )

        # 根据任务类型添加后台任务
        if task_type == "sync":
            background_tasks.add_task(
                _execute_sync_task_background,
                task_id=task_id,
                start_date=request.start_date if request else None,
                end_date=request.end_date if request else None,
                run_id=run_id
            )
            message = f"Sync task {task_id} started in background"

        elif task_type == "etl":
            background_tasks.add_task(
                _execute_etl_task_background,
                task_id=task_id,
                start_date=request.start_date if request else None,
                end_date=request.end_date if request else None,
                run_id=run_id
            )
            message = f"ETL task {task_id} started in background"

        elif task_type == "factor":
            background_tasks.add_task(
                _execute_factor_task_background,
                task_id=task_id,
                start_date=request.start_date if request else None,
                end_date=request.end_date if request else None,
                run_id=run_id
            )
            message = f"Factor task {task_id} started in background"

        return TaskExecuteResponse(
            status="success",
            message=message,
            task_id=task_id,
            task_type=task_type,
            result={"run_id": run_id, "status": "pending"}
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute {task_type} task {task_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Task execution failed: {str(e)}"
        )


@router.get("/tasks/{task_type}/{task_id}/inspect", response_model=DataInspectionResponse)
def inspect_task_data(
    task_type: str = Path(..., description="任务类型 (sync/etl/factor)"),
    task_id: str = Path(..., description="任务 ID")
):
    """
    数据探查：检查任务表的数据完整性

    返回：
    - 表的存在性
    - 数据日期范围（最早/最晚日期）
    - 实际数据天数 vs 预期交易日天数
    - 缺失的交易日列表
    - 数据覆盖率
    """
    try:
        service = _get_service(task_type)
        result = service.inspect_data(task_id)

        return DataInspectionResponse(**result)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to inspect data for {task_type} task {task_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Data inspection failed: {str(e)}"
        )
