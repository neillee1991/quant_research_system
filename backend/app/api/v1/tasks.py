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
    flow_run_id: Optional[int] = Field(default=None, description="关联的 flow_run_id（调度器传入）")
    run_id: Optional[str] = Field(default=None, description="预生成的 run_id（调度器传入，用于合并记录）")


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


def _normalize_task_config(config_data: Dict[str, Any]) -> Dict[str, Any]:
    """将前端发送的字段名规范化为模型期望的字段名。

    前端 SyncTaskDrawer/ETLTaskDrawer 发送 primary_keys (list) 和 schema (dict)，
    但 SyncTaskConfig/ETLTaskConfig 期望 primary_keys_json (string) 和 schema_json (string)。
    """
    data = dict(config_data)
    if "primary_keys" in data and "primary_keys_json" not in data:
        pk = data.pop("primary_keys")
        data["primary_keys_json"] = json.dumps(pk) if isinstance(pk, list) else str(pk)
    if "schema" in data and "schema_json" not in data:
        schema = data.pop("schema")
        data["schema_json"] = json.dumps(schema, ensure_ascii=False) if isinstance(schema, dict) else str(schema)
    data.pop("confirm_schema_change", None)
    return data


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
async def get_running_tasks(
    task_type: Optional[str] = Query(default=None, description="按任务类型过滤 (sync/etl/factor)"),
    task_id: Optional[str] = Query(default=None, description="按任务ID过滤")
):
    """获取所有正在运行的任务（查询 PostgreSQL task_runs 表）"""
    try:
        from scheduler.db import DatabasePool

        conditions = ["status = 'running'"]
        params: list = []
        idx = 1

        if task_type:
            conditions.append(f"task_type = ${idx}")
            params.append(task_type)
            idx += 1
        if task_id:
            conditions.append(f"task_id = ${idx}")
            params.append(task_id)
            idx += 1

        where = " AND ".join(conditions)
        rows = await DatabasePool.fetch(
            f"SELECT * FROM task_runs WHERE {where} ORDER BY started_at DESC",
            *params,
        )
        tasks = [_format_task_row(dict(r)) for r in rows]
        return RunningTaskResponse(tasks=tasks, total=len(tasks))

    except Exception as e:
        logger.error(f"Failed to get running tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/history", response_model=TaskHistoryResponse)
async def get_task_history(
    limit: int = Query(default=50, ge=1, le=200),
    task_type: Optional[str] = Query(default=None, description="按任务类型过滤 (sync/etl/factor)"),
    task_id: Optional[str] = Query(default=None, description="按任务ID过滤"),
    start_date: Optional[str] = Query(default=None, description="开始日期 (YYYYMMDD)，按 started_at 过滤"),
    end_date: Optional[str] = Query(default=None, description="结束日期 (YYYYMMDD)，按 started_at 过滤"),
):
    """获取最近完成/失败的任务历史（查询 PostgreSQL task_runs 表）"""
    try:
        from scheduler.db import DatabasePool

        conditions = ["status IN ('success', 'failed')"]
        params: list = []
        idx = 1

        if task_type:
            conditions.append(f"task_type = ${idx}")
            params.append(task_type)
            idx += 1
        if task_id:
            conditions.append(f"task_id = ${idx}")
            params.append(task_id)
            idx += 1
        if start_date:
            conditions.append(f"started_at >= TO_DATE(${idx}, 'YYYYMMDD')")
            params.append(start_date)
            idx += 1
        if end_date:
            conditions.append(f"started_at < TO_DATE(${idx}, 'YYYYMMDD') + INTERVAL '1 day'")
            params.append(end_date)
            idx += 1

        where = " AND ".join(conditions)
        rows = await DatabasePool.fetch(
            f"SELECT * FROM task_runs WHERE {where} ORDER BY COALESCE(finished_at, started_at) DESC LIMIT {limit}",
            *params,
        )
        tasks = [_format_task_row(dict(r)) for r in rows]
        return TaskHistoryResponse(tasks=tasks, total=len(tasks))

    except Exception as e:
        logger.error(f"Failed to get task history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/cleanup")
async def cleanup_stale_tasks(timeout_minutes: int = Query(default=0, ge=0, description="超时分钟数，0=清理所有running")):
    """清理僵尸任务（将长时间 running 的记录标记为 failed）"""
    try:
        from app.services.task_runner import TaskRunner
        cleaned = await TaskRunner.cleanup_stale(
            timeout_minutes=timeout_minutes,
            reason=f"manual cleanup (timeout={timeout_minutes}min)"
        )
        return {"status": "success", "data": {"cleaned": cleaned}}
    except Exception as e:
        logger.error(f"Failed to cleanup stale tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _format_task_row(row: dict) -> dict:
    """Format datetime fields to ISO strings for JSON serialization."""
    for field in ["started_at", "finished_at", "created_at"]:
        if field in row and row[field] is not None:
            row[field] = str(row[field])
    return row


@router.get("/tasks/version", response_model=VersionResponse)
async def get_config_version():
    """获取配置版本号（基于最新更新时间的时间戳）"""
    try:
        from scheduler.db import DatabasePool

        tables = ["sync_task_configs", "etl_task_configs", "factor_configs"]
        max_timestamp = 0

        for table in tables:
            try:
                row = await DatabasePool.fetchrow(
                    f"SELECT MAX(updated_at) AS max_time FROM {table}"
                )
                if row and row["max_time"]:
                    timestamp = int(row["max_time"].timestamp() * 1000)
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


# ==================== ETL / Sync 专用端点 ====================

@router.post("/tasks/etl/test")
async def test_etl_script(payload: dict):
    """测试 ETL 脚本，返回执行结果及字段类型"""
    from app.api.v1.data.etl_api import test_etl_script as _test
    return await _test(payload)


@router.post("/tasks/sync/all")
async def sync_all_tasks(
    target_date: Optional[str] = Query(None, description="目标日期 YYYYMMDD")
):
    """同步所有启用的任务（后台执行）"""
    import threading
    from data_manager.refactored_sync_engine import sync_engine

    try:
        tasks = await sync_service.list_tasks(enabled_only=True)
        for task in tasks:
            t_run_id = f"{task.task_id}_{int(time.time() * 1000)}"
            await TaskRunner.start(
                t_run_id, "sync", task.task_id, f"数据同步: {task.task_id}",
                params=json.dumps({"target_date": target_date}),
            )
    except Exception as log_err:
        logger.warning(f"Failed to start tasks for sync_all: {log_err}")

    def run_sync():
        try:
            results = sync_engine.sync_all_enabled_tasks(target_date)
            logger.info(f"Background sync completed: {results}")
        except Exception as e:
            logger.error(f"Background sync failed: {e}")

    threading.Thread(target=run_sync, daemon=True).start()
    return {"status": "started", "message": "All enabled tasks sync started in background"}


@router.post("/tasks/etl/{task_id}/backfill")
async def backfill_etl_task(
    task_id: str = Path(..., description="任务ID"),
    start_date: str = Query("", description="开始日期 YYYYMMDD"),
    end_date: str = Query("", description="结束日期 YYYYMMDD"),
):
    """回溯执行 ETL 任务"""
    from app.api.v1.data.etl_api import backfill_etl_task as _backfill
    return await _backfill(task_id, start_date, end_date)


@router.post("/tasks/etl/{task_id}/create-table")
async def create_etl_table(
    task_id: str = Path(..., description="任务ID"),
    payload: dict = None,
):
    """根据字段定义创建 ETL 目标表"""
    from app.api.v1.data.etl_api import create_etl_table as _create
    return await _create(task_id, payload or {})


@router.get("/tasks/etl/{task_id}/schema")
async def get_etl_table_schema(
    task_id: str = Path(..., description="任务ID"),
):
    """获取 ETL 任务的字段定义"""
    from app.api.v1.data.etl_api import get_etl_table_schema as _schema
    return await _schema(task_id)


@router.get("/tasks/{task_type}/{task_id}/status")
async def get_task_data_status(
    task_type: str = Path(..., description="任务类型: sync, etl"),
    task_id: str = Path(..., description="任务ID"),
):
    """获取任务数据状态（最新数据日期、上次同步时间）"""
    try:
        from scheduler.db import DatabasePool

        # 获取任务配置
        service = _get_service(task_type)
        task = await service.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found in {task_type}")

        task_dict = task.model_dump()
        table_name = task_dict.get("table_name")
        date_field = task_dict.get("date_field") or ""

        # 获取最新数据日期（从目标表）
        table_latest_date = None
        if table_name:
            try:
                db_client.register_meta_table(table_name)
                if date_field and db_client.table_exists(table_name):
                    sql = f'SELECT MAX({date_field}) as max_date FROM {table_name}'
                    result = db_client.query(sql)
                    if not result.is_empty() and result["max_date"][0] is not None:
                        max_date_val = result["max_date"][0]
                        if isinstance(max_date_val, str):
                            s = max_date_val.replace("-", "").replace(" ", "")[:8]
                            if len(s) == 8 and s.isdigit():
                                table_latest_date = s
                        elif isinstance(max_date_val, int):
                            s = str(max_date_val)
                            if len(s) == 8:
                                table_latest_date = s
                        elif hasattr(max_date_val, "strftime"):
                            table_latest_date = max_date_val.strftime("%Y%m%d")
            except Exception as e:
                logger.warning(f"Failed to get latest date for {task_type} table {table_name}: {e}")

        # 获取上次同步时间（从 PostgreSQL task_runs 表）
        last_sync_time = None
        try:
            last_run = await DatabasePool.fetchrow("""
                SELECT finished_at FROM task_runs
                WHERE task_type = $1 AND task_id = $2 AND status = 'success'
                ORDER BY COALESCE(finished_at, started_at) DESC
                LIMIT 1
            """, task_type, task_id)
            if last_run and last_run.get("finished_at"):
                finished_at = last_run["finished_at"]
                last_sync_time = finished_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(finished_at, "strftime") else str(finished_at)
        except Exception as e:
            logger.warning(f"Failed to get last sync time for {task_type} task {task_id}: {e}")

        # 构建响应（统一格式）
        result = {
            "task_id": task_dict.get("task_id"),
            "description": task_dict.get("description", ""),
            "sync_type": task_dict.get("sync_type", ""),
            "table_name": table_name,
            "enabled": task_dict.get("enabled", True),
            "last_sync_date": table_latest_date,
            "last_sync_time": last_sync_time,
            "table_latest_date": table_latest_date,  # 别名，兼容前端
        }
        return {"status": "success", "data": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get {task_type} task status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_type}", response_model=TaskListResponse)
async def list_tasks(
    task_type: str = Path(..., description="任务类型: sync, etl, factor"),
    enabled_only: bool = Query(default=False, description="是否只返回启用的任务")
):
    """列出所有任务"""
    try:
        service = _get_service(task_type)
        tasks = await service.list_tasks(enabled_only=enabled_only)
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
    """查询任务执行状态（查询 PostgreSQL task_runs 表）"""
    try:
        from scheduler.db import DatabasePool

        row = await DatabasePool.fetchrow(
            "SELECT * FROM task_runs WHERE run_id = $1 LIMIT 1",
            run_id,
        )
        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Task status not found for run_id: {run_id}"
            )
        return {"status": "success", "data": _format_task_row(dict(row))}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get task status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_type}/{task_id}", response_model=TaskResponse)
async def get_task(
    task_type: str = Path(..., description="任务类型: sync, etl, factor"),
    task_id: str = Path(..., description="任务ID")
):
    """获取单个任务"""
    try:
        service = _get_service(task_type)
        task = await service.get_task(task_id)

        if not task:
            raise HTTPException(
                status_code=404,
                detail=f"Task {task_id} not found in {task_type}"
            )

        return TaskResponse(task=task.model_dump(), task_type=task_type)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get {task_type} task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_type}", response_model=TaskResponse)
async def create_task(
    task_type: str = Path(..., description="任务类型: sync, etl, factor"),
    request: TaskCreateRequest = None
):
    """创建新任务"""
    try:
        service = _get_service(task_type)
        task = await service.create_task(
            config_data=_normalize_task_config(request.config_data),
            changed_by=request.changed_by,
            change_reason=request.change_reason
        )
        return TaskResponse(task=task.model_dump(), task_type=task_type)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create {task_type} task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/tasks/{task_type}/{task_id}", response_model=TaskResponse)
async def update_task(
    task_type: str = Path(..., description="任务类型: sync, etl, factor"),
    task_id: str = Path(..., description="任务ID"),
    request: TaskUpdateRequest = None
):
    """更新任务"""
    try:
        service = _get_service(task_type)
        task = await service.update_task(
            task_id=task_id,
            config_data=_normalize_task_config(request.config_data),
            changed_by=request.changed_by,
            change_reason=request.change_reason
        )
        return TaskResponse(task=task.model_dump(), task_type=task_type)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to update {task_type} task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/tasks/{task_type}/{task_id}", response_model=DeleteResponse)
async def delete_task(
    task_type: str = Path(..., description="任务类型: sync, etl, factor"),
    task_id: str = Path(..., description="任务ID"),
    drop_table: bool = Query(default=False, description="是否删除关联表"),
    changed_by: str = Query(default="api", description="修改人"),
    change_reason: str = Query(default="Delete task", description="修改原因")
):
    """删除任务（软删除，设置 enabled=false）"""
    try:
        service = _get_service(task_type)

        if drop_table:
            task = await service.get_task(task_id)
            if task:
                table_name = None
                if task_type in ("sync", "etl"):
                    table_name = getattr(task, "table_name", None)
                elif task_type == "factor":
                    logger.warning(f"Factor tasks use shared table, skipping drop_table for {task_id}")

                if table_name:
                    from store.dolphindb_client import db_client
                    try:
                        db_client.drop_table(table_name)
                        logger.info(f"Dropped table {table_name} for task {task_id}")
                    except Exception as e:
                        logger.warning(f"Failed to drop table {table_name}: {e}")

        success = await service.delete_task(
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
async def _execute_sync_task_background(task_id: str, start_date: Optional[str], end_date: Optional[str], run_id: str):
    """后台执行同步任务"""
    import asyncio
    from data_manager.refactored_sync_engine import sync_engine
    rows = await asyncio.get_event_loop().run_in_executor(
        None, lambda: sync_engine.sync_task(task_id=task_id, target_date=start_date, end_date=end_date)
    )
    if rows < 0:
        raise RuntimeError(f"Sync task {task_id} failed")
    logger.info(f"Sync task {task_id} completed: run_id={run_id}, rows={rows}")
    return {"rows": rows, "extra": {"start_date": start_date, "end_date": end_date}}


@tracked_task("etl", task_id_kwarg="task_id")
async def _execute_etl_task_background(task_id: str, start_date: Optional[str], end_date: Optional[str], run_id: str):
    """后台执行 ETL 任务"""
    import asyncio

    def _run_etl():
        import psycopg2
        import psycopg2.extras
        from app.core.config import settings
        from app.api.v1.data.etl_api import _etl_execute_and_write

        conn = psycopg2.connect(
            host=settings.postgresql.postgres_host,
            port=settings.postgresql.postgres_port,
            dbname=settings.postgresql.postgres_db,
            user=settings.postgresql.postgres_user,
            password=settings.postgresql.postgres_password,
        )
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT * FROM etl_task_configs WHERE task_id = %s", (task_id,))
                    row = cur.fetchone()
                    if row is None:
                        raise ValueError(f"ETL task {task_id} not found")
                    task = dict(row)
        finally:
            conn.close()

        script_template = task.get("script", "")
        if not script_template or not script_template.strip():
            raise ValueError("ETL script is empty")

        # 将 YYYYMMDD 转为 DolphinDB 日期格式 YYYY.MM.DD
        if start_date and len(start_date) == 8:
            date_str = f"{start_date[:4]}.{start_date[4:6]}.{start_date[6:]}"
        else:
            from datetime import datetime
            date_str = datetime.now().strftime("%Y.%m.%d")

        script = script_template.replace("{date}", date_str)
        return _etl_execute_and_write(task_id, script, task)

    rows = await asyncio.get_event_loop().run_in_executor(None, _run_etl)
    logger.info(f"ETL task {task_id} completed: run_id={run_id}, rows={rows}")
    return {"rows": rows if isinstance(rows, int) else 0, "extra": {"start_date": start_date, "end_date": end_date}}


@tracked_task("factor", task_id_kwarg="task_id")
async def _execute_factor_task_background(task_id: str, start_date: Optional[str], end_date: Optional[str], run_id: str):
    """后台执行因子任务"""
    import asyncio
    from app.services.factor_service import FactorComputeService
    from store.dolphindb_client import db_client
    service = FactorComputeService(db_client)
    compute_result = await asyncio.get_event_loop().run_in_executor(
        None, lambda: service.compute_factor(
            factor_id=task_id,
            start_date=start_date,
            end_date=end_date,
            mode="full" if start_date else "incremental"
        )
    )
    rows = getattr(compute_result, "rows", 0)
    logger.info(f"Factor task {task_id} completed: run_id={run_id}, rows={rows}")
    return {"rows": rows, "extra": {"start_date": start_date, "end_date": end_date}}


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

        task = await service.get_task(task_id)
        if not task:
            raise HTTPException(
                status_code=404,
                detail=f"Task {task_id} not found in {task_type}"
            )

        # 调度器传入 run_id 时直接使用，否则自己生成
        run_id = (request.run_id if request and request.run_id
                  else f"{task_id}_{int(time.time() * 1000)}")

        params_dict = {
            "start_date": request.start_date if request else None,
            "end_date": request.end_date if request else None,
            **(request.params if request else {}),
        }
        await TaskRunner.start(
            run_id,
            task_type,
            task_id,
            f"{task_type.upper()} 任务: {task_id}",
            params=json.dumps(params_dict),
            flow_run_id=request.flow_run_id if request else None,
        )

        task_kwargs = dict(
            task_id=task_id,
            start_date=request.start_date if request else None,
            end_date=request.end_date if request else None,
            run_id=run_id,
        )

        if task_type == "sync":
            background_tasks.add_task(_execute_sync_task_background, **task_kwargs)
            message = f"Sync task {task_id} started in background"
        elif task_type == "etl":
            background_tasks.add_task(_execute_etl_task_background, **task_kwargs)
            message = f"ETL task {task_id} started in background"
        elif task_type == "factor":
            background_tasks.add_task(_execute_factor_task_background, **task_kwargs)
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
async def inspect_task_data(
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
        result = await service.inspect_data(task_id)

        return DataInspectionResponse(**result)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to inspect data for {task_type} task {task_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Data inspection failed: {str(e)}"
        )
