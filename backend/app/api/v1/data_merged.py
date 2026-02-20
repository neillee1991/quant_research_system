"""
数据 API 路由（合并版）
整合了数据查询和配置化同步功能
"""
from typing import List, Optional
from fastapi import APIRouter, Query, HTTPException, status
from pydantic import BaseModel, Field

from store.postgres_client import db_client
from data_manager.refactored_sync_engine import sync_engine
from app.core.logger import logger


router = APIRouter()


# ==================== 请求/响应模型 ====================

class SyncTaskInfo(BaseModel):
    """同步任务信息"""
    task_id: str
    description: str
    sync_type: str
    schedule: str
    enabled: bool
    table_name: str


class SyncTaskListResponse(BaseModel):
    """同步任务列表响应"""
    tasks: List[SyncTaskInfo]
    total: int


class SyncResponse(BaseModel):
    """同步响应"""
    status: str
    message: str
    task_id: Optional[str] = None
    target_date: Optional[str] = None


# ==================== 数据查询接口 ====================

@router.get("/data/stocks")
def list_stocks():
    """获取股票列表"""
    try:
        df = db_client.query("SELECT ts_code FROM stock_basic ORDER BY ts_code")
        return {"stocks": df["ts_code"].to_list()}
    except Exception as e:
        logger.error(f"Failed to list stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/daily")
def get_daily(
    ts_code: str = Query(..., description="股票代码，如 000001.SZ"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYYMMDD"),
    limit: int = Query(500, le=5000, description="返回记录数限制"),
):
    """获取日线行情数据（OHLC完整数据，从 daily_data 表）"""
    try:
        conditions = ["ts_code = %s"]
        params = [ts_code]
        if start_date:
            conditions.append("trade_date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= %s")
            params.append(end_date)
        where = " AND ".join(conditions)
        sql = f"SELECT * FROM daily_data WHERE {where} ORDER BY trade_date DESC LIMIT {limit}"
        df = db_client.query(sql, tuple(params))
        return {"data": df.to_dicts(), "count": len(df)}
    except Exception as e:
        logger.error(f"Failed to get daily data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 配置化同步接口 ====================

@router.get("/data/sync/tasks", response_model=SyncTaskListResponse)
def list_sync_tasks():
    """
    列出所有同步任务配置

    返回系统中配置的所有数据同步任务，包括任务状态、同步类型等信息
    """
    try:
        tasks = sync_engine.get_all_tasks()
        task_list = [
            SyncTaskInfo(
                task_id=t["task_id"],
                description=t.get("description", ""),
                sync_type=t.get("sync_type", ""),
                schedule=t.get("schedule", ""),
                enabled=t.get("enabled", True),
                table_name=t.get("table_name", "")
            )
            for t in tasks
        ]
        return SyncTaskListResponse(tasks=task_list, total=len(task_list))
    except Exception as e:
        logger.error(f"Failed to list sync tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/sync/task/{task_id}", response_model=SyncResponse)
def sync_single_task(
    task_id: str,
    target_date: Optional[str] = Query(None, description="目标日期 YYYYMMDD")
):
    """
    同步指定任务

    - **task_id**: 任务ID（如 daily_basic, stock_basic）
    - **target_date**: 目标日期，不指定则同步到今天
    """
    try:
        success = sync_engine.sync_task(task_id, target_date)

        if not success:
            raise HTTPException(
                status_code=500,
                detail=f"Task {task_id} sync failed"
            )

        return SyncResponse(
            status="success",
            message=f"Task {task_id} synced successfully",
            task_id=task_id,
            target_date=target_date
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Sync task {task_id} error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/sync/all", response_model=SyncResponse)
def sync_all_tasks(
    target_date: Optional[str] = Query(None, description="目标日期 YYYYMMDD")
):
    """
    同步所有启用的任务

    后台执行，避免超时。同步所有在配置文件中启用的任务。

    - **target_date**: 目标日期，不指定则同步到今天
    """
    try:
        import threading

        def run_sync():
            try:
                results = sync_engine.sync_all_enabled_tasks(target_date)
                logger.info(f"Background sync completed: {results}")
            except Exception as e:
                logger.error(f"Background sync failed: {e}")

        thread = threading.Thread(target=run_sync, daemon=True)
        thread.start()

        return SyncResponse(
            status="started",
            message="All enabled tasks sync started in background",
            target_date=target_date
        )
    except Exception as e:
        logger.error(f"Failed to start sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/sync/schedule/{schedule}", response_model=SyncResponse)
def sync_by_schedule(
    schedule: str,
    target_date: Optional[str] = Query(None, description="目标日期 YYYYMMDD")
):
    """
    按调度类型同步

    - **schedule**: 调度类型（daily/weekly/monthly）
    - **target_date**: 目标日期，不指定则同步到今天
    """
    if schedule not in ["daily", "weekly", "monthly"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid schedule type. Must be one of: daily, weekly, monthly"
        )

    try:
        import threading

        def run_sync():
            try:
                results = sync_engine.sync_by_schedule(schedule, target_date)
                logger.info(f"Schedule sync completed: {results}")
            except Exception as e:
                logger.error(f"Schedule sync failed: {e}")

        thread = threading.Thread(target=run_sync, daemon=True)
        thread.start()

        return SyncResponse(
            status="started",
            message=f"Sync for schedule '{schedule}' started in background",
            target_date=target_date
        )
    except Exception as e:
        logger.error(f"Failed to start schedule sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/sync/status")
def get_sync_status(
    limit: int = Query(100, le=1000, description="返回记录数限制")
):
    """
    获取同步状态

    返回最近的同步日志，包括各个任务的最后同步时间
    """
    try:
        df = db_client.query(
            f"SELECT * FROM sync_log ORDER BY updated_at DESC LIMIT {limit}"
        )
        return {
            "logs": df.to_dicts() if not df.is_empty() else [],
            "count": len(df)
        }
    except Exception as e:
        logger.error(f"Failed to get sync status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/sync/status/{task_id}")
def get_task_status(task_id: str):
    """
    获取指定任务的同步状态

    - **task_id**: 任务ID
    """
    try:
        status_info = sync_engine.get_task_status(task_id)

        if "error" in status_info:
            raise HTTPException(
                status_code=404,
                detail=f"Task {task_id} not found"
            )

        return status_info
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get task status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 数据库管理接口 ====================

@router.get("/data/tables")
def list_tables():
    """
    列出数据库中的所有表

    返回数据库中所有表的名称和行数
    """
    try:
        # 获取所有表名
        tables_df = db_client.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        )

        tables_info = []
        for table_name in tables_df["table_name"].to_list():
            try:
                # 获取表的行数
                count_df = db_client.query(f"SELECT COUNT(*) as count FROM {table_name}")
                row_count = count_df["count"][0] if not count_df.is_empty() else 0

                # 获取表结构
                schema_df = db_client.query(f"PRAGMA table_info({table_name})")
                columns = schema_df["name"].to_list() if not schema_df.is_empty() else []

                tables_info.append({
                    "table_name": table_name,
                    "row_count": int(row_count),
                    "column_count": len(columns),
                    "columns": columns
                })
            except Exception as e:
                logger.warning(f"Failed to get info for table {table_name}: {e}")
                tables_info.append({
                    "table_name": table_name,
                    "row_count": 0,
                    "column_count": 0,
                    "columns": []
                })

        return {
            "tables": tables_info,
            "total": len(tables_info)
        }
    except Exception as e:
        logger.error(f"Failed to list tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/query")
def execute_query(
    sql: str = Query(..., description="SQL 查询语句"),
    limit: int = Query(1000, le=10000, description="返回记录数限制")
):
    """
    执行 SQL 查询

    - **sql**: SQL 查询语句（只支持 SELECT）
    - **limit**: 返回记录数限制（最大 10000）

    ⚠️ 安全限制：
    - 只允许 SELECT 查询
    - 不允许 DROP, DELETE, UPDATE, INSERT 等修改操作
    """
    try:
        # 安全检查：只允许 SELECT 查询
        sql_upper = sql.strip().upper()
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']

        if not sql_upper.startswith('SELECT'):
            raise HTTPException(
                status_code=400,
                detail="Only SELECT queries are allowed"
            )

        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                raise HTTPException(
                    status_code=400,
                    detail=f"Dangerous keyword '{keyword}' is not allowed"
                )

        # 添加 LIMIT 限制
        if 'LIMIT' not in sql_upper:
            sql = f"{sql.rstrip(';')} LIMIT {limit}"

        # 执行查询
        df = db_client.query(sql)

        return {
            "data": df.to_dicts() if not df.is_empty() else [],
            "count": len(df),
            "columns": df.columns if not df.is_empty() else []
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
