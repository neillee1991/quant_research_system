"""
数据 API 路由（合并版）
整合了数据查询和配置化同步功能
"""
from typing import List, Optional
from fastapi import APIRouter, Query, HTTPException, status
from pydantic import BaseModel, Field

import httpx
import polars as pl
from store.dolphindb_client import db_client
from data_manager.refactored_sync_engine import sync_engine
from app.core.config import settings
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
    target_date: Optional[str] = Query(None, description="目标日期 YYYYMMDD，不指定则同步最新一天"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYYMMDD")
):
    """
    同步指定任务

    - **task_id**: 任务ID（如 daily_basic, stock_basic）
    - **target_date**: 目标日期，不指定则只同步最新一天（增量任务）或执行一次（全量任务）
    - **start_date**: 开始日期（可选），用于指定同步的起始日期
    - **end_date**: 结束日期（可选），用于指定同步的结束日期

    注意：如果同时指定了 start_date 和 end_date，将使用这两个参数；否则使用 target_date
    """
    try:
        # 如果指定了 start_date 和 end_date，使用 start_date 作为 target_date
        # 后端会从 start_date 同步到 end_date
        if start_date and end_date:
            sync_date = start_date
        elif start_date:
            sync_date = start_date
        else:
            sync_date = target_date

        success = sync_engine.sync_task(task_id, sync_date, end_date)

        if not success:
            raise HTTPException(
                status_code=500,
                detail=f"Task {task_id} sync failed"
            )

        return SyncResponse(
            status="success",
            message=f"Task {task_id} synced successfully",
            task_id=task_id,
            target_date=sync_date
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Sync task {task_id} error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/sync/all", response_model=SyncResponse)
def sync_all_tasks(
    target_date: Optional[str] = Query(None, description="目标日期 YYYYMMDD，不指定则只同步最新一天")
):
    """
    同步所有启用的任务（仅最新数据）

    后台执行，避免超时。同步所有在配置文件中启用的任务。
    - 增量任务：只同步最新一天
    - 全量任务：执行一次完整同步

    - **target_date**: 目标日期，不指定则同步最新一天
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
    limit: int = Query(1000, le=10000, description="返回记录数限制"),
    source: Optional[str] = Query(None, description="按来源筛选"),
    data_type: Optional[str] = Query(None, description="按数据类型筛选"),
    start_date: Optional[str] = Query(None, description="按同步日期筛选（起始）"),
    end_date: Optional[str] = Query(None, description="按同步日期筛选（结束）")
):
    """
    获取同步历史记录（支持筛选）

    返回所有同步历史记录，支持按来源、类型、日期筛选
    """
    try:
        conditions = []
        params = []

        if source:
            conditions.append("source = %s")
            params.append(source)
        if data_type:
            conditions.append("data_type = %s")
            params.append(data_type)
        if start_date:
            conditions.append("sync_date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("sync_date <= %s")
            params.append(end_date)

        where_clause = " AND ".join(conditions) if conditions else ""
        where_part = f"WHERE {where_clause}" if where_clause else ""
        sql = f"""
            SELECT source, data_type, last_date, sync_date, rows_synced, status, created_at
            FROM sync_log_history
            {where_part}
            ORDER BY created_at DESC
            LIMIT {limit}
        """

        df = db_client.query(sql, tuple(params) if params else ())
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
    获取指定任务的同步状态（包含表中最新日期）

    - **task_id**: 任务ID
    """
    try:
        status_info = sync_engine.get_task_status(task_id)

        if "error" in status_info:
            raise HTTPException(
                status_code=404,
                detail=f"Task {task_id} not found"
            )

        # 获取表中最新日期
        table_name = status_info.get("table_name")
        date_field = status_info.get("date_field", "trade_date")  # 使用配置中的日期字段
        if table_name and date_field:
            try:
                # 使用配置中指定的日期字段
                db_path = db_client._resolve_db_path(table_name)
                max_date_sql = f'SELECT MAX({date_field}) as max_date FROM loadTable("{db_path}", "{table_name}")'
                df = db_client.query(max_date_sql)
                if not df.is_empty() and df["max_date"][0]:
                    status_info["table_latest_date"] = df["max_date"][0]
                else:
                    status_info["table_latest_date"] = None
            except Exception as e:
                logger.warning(f"Failed to get latest date for {table_name}: {e}")
                status_info["table_latest_date"] = None

        return status_info
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get task status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 任务配置管理接口 ====================

# ==================== Prefect 调度接口 ====================

@router.post("/data/flow/run/{flow_name}")
async def trigger_flow_run(
    flow_name: str,
    target_date: Optional[str] = Query(None, description="目标日期 YYYYMMDD"),
):
    """触发 Prefect Flow 运行"""
    try:
        async with httpx.AsyncClient() as client:
            # 查找 deployment
            resp = await client.get(
                f"{settings.prefect_api_url}/deployments/filter",
                json={"deployments": {"name": {"any_": [f"{flow_name}-deployment"]}}}
            )
            resp.raise_for_status()
            deployments = resp.json()

            if not deployments:
                raise HTTPException(status_code=404, detail=f"Flow deployment '{flow_name}' not found")

            deployment_id = deployments[0]["id"]

            # 创建 flow run
            params = {}
            if target_date:
                params["target_date"] = target_date

            resp = await client.post(
                f"{settings.prefect_api_url}/deployments/{deployment_id}/create_flow_run",
                json={"parameters": params}
            )
            resp.raise_for_status()
            flow_run = resp.json()

            return {
                "status": "scheduled",
                "flow_run_id": flow_run["id"],
                "flow_name": flow_name,
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger flow run: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/flow/runs")
async def list_flow_runs(
    limit: int = Query(20, le=100, description="返回记录数"),
):
    """获取最近的 Flow 运行记录"""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{settings.prefect_api_url}/flow_runs/filter",
                json={
                    "sort": "EXPECTED_START_TIME_DESC",
                    "limit": limit,
                }
            )
            resp.raise_for_status()
            runs = resp.json()

            return {
                "runs": [
                    {
                        "id": r["id"],
                        "name": r.get("name", ""),
                        "flow_id": r.get("flow_id", ""),
                        "state_type": r.get("state_type", ""),
                        "state_name": r.get("state_name", ""),
                        "start_time": r.get("start_time"),
                        "end_time": r.get("end_time"),
                        "parameters": r.get("parameters", {}),
                    }
                    for r in runs
                ],
                "total": len(runs),
            }
    except Exception as e:
        logger.error(f"Failed to list flow runs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/flow/deployments")
async def list_deployments():
    """获取所有 Flow 部署"""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{settings.prefect_api_url}/deployments/filter",
                json={}
            )
            resp.raise_for_status()
            deployments = resp.json()

            return {
                "deployments": [
                    {
                        "id": d["id"],
                        "name": d.get("name", ""),
                        "flow_id": d.get("flow_id", ""),
                        "schedule": d.get("schedule"),
                        "is_schedule_active": d.get("is_schedule_active", False),
                        "tags": d.get("tags", []),
                        "description": d.get("description", ""),
                    }
                    for d in deployments
                ],
                "total": len(deployments),
            }
    except Exception as e:
        logger.error(f"Failed to list deployments: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/sync/task/{task_id}/config")
def get_task_config(task_id: str):
    """
    获取指定任务的完整配置

    - **task_id**: 任务ID
    """
    try:
        task = sync_engine.config_manager.get_task(task_id)
        return {"config": task}
    except Exception as e:
        logger.error(f"Failed to get task config: {e}")
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")


@router.put("/data/sync/task/{task_id}/config")
def update_task_config(task_id: str, config: dict):
    """
    更新指定任务的配置

    - **task_id**: 任务ID
    - **config**: 任务配置JSON
    """
    try:
        import json as _json
        from datetime import datetime as _dt
        from store.dolphindb_client import db_client

        # 确认任务存在
        existing = db_client.query(f"SELECT * FROM sync_task_config WHERE task_id = '{task_id}'")
        if existing.is_empty():
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

        # 构建更新行
        now = _dt.now()
        row = pl.DataFrame({
            "task_id": [config.get("task_id", task_id)],
            "api_name": [config.get("api_name", "")],
            "description": [config.get("description", "")],
            "sync_type": [config.get("sync_type", "incremental")],
            "params_json": [_json.dumps(config.get("params", {}), ensure_ascii=False)],
            "date_field": [config.get("date_field", "")],
            "primary_keys_json": [_json.dumps(config.get("primary_keys", []))],
            "table_name": [config.get("table_name", "")],
            "schema_json": [_json.dumps(config.get("schema", {}), ensure_ascii=False)],
            "enabled": [config.get("enabled", True)],
            "api_limit": [config.get("api_limit", 5000)],
            "schedule": [config.get("schedule", "daily")],
            "created_at": existing["created_at"].to_list(),
            "updated_at": [now],
        })
        db_client.upsert("sync_task_config", row, ["task_id"])
        sync_engine.config_manager.reload()

        logger.info(f"Updated config for task {task_id}")
        return {"status": "success", "message": f"Task {task_id} config updated"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update task config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/sync/tasks")
def create_task(config: dict):
    """
    创建新的同步任务

    - **config**: 任务配置JSON
    """
    try:
        import json as _json
        from datetime import datetime as _dt
        from store.dolphindb_client import db_client

        # 验证必需字段
        required_fields = ["task_id", "api_name", "sync_type", "table_name", "primary_keys"]
        for field in required_fields:
            if field not in config:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

        # 检查任务ID是否已存在
        existing = db_client.query(f"SELECT task_id FROM sync_task_config WHERE task_id = '{config['task_id']}'")
        if not existing.is_empty():
            raise HTTPException(status_code=400, detail=f"Task {config['task_id']} already exists")

        # 插入新任务
        now = _dt.now()
        row = pl.DataFrame({
            "task_id": [config["task_id"]],
            "api_name": [config["api_name"]],
            "description": [config.get("description", "")],
            "sync_type": [config["sync_type"]],
            "params_json": [_json.dumps(config.get("params", {}), ensure_ascii=False)],
            "date_field": [config.get("date_field", "")],
            "primary_keys_json": [_json.dumps(config["primary_keys"])],
            "table_name": [config["table_name"]],
            "schema_json": [_json.dumps(config.get("schema", {}), ensure_ascii=False)],
            "enabled": [config.get("enabled", True)],
            "api_limit": [config.get("api_limit", 5000)],
            "schedule": [config.get("schedule", "daily")],
            "created_at": [now],
            "updated_at": [now],
        })
        db_client.upsert("sync_task_config", row, ["task_id"])
        sync_engine.config_manager.reload()

        logger.info(f"Created new task {config['task_id']}")
        return {"status": "success", "message": f"Task {config['task_id']} created"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/data/sync/tasks/{task_id}")
def delete_task(task_id: str):
    """
    删除指定的同步任务

    - **task_id**: 任务ID
    """
    try:
        from store.dolphindb_client import db_client

        # 确认任务存在
        existing = db_client.query(f"SELECT task_id FROM sync_task_config WHERE task_id = '{task_id}'")
        if existing.is_empty():
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

        # 从数据库删除
        db_client.execute(f"DELETE FROM sync_task_config WHERE task_id = '{task_id}'")
        sync_engine.config_manager.reload()

        logger.info(f"Deleted task {task_id}")
        return {"status": "success", "message": f"Task {task_id} deleted"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 数据库管理接口 ====================

@router.delete("/data/tables/{table_name}")
def truncate_table(table_name: str):
    """
    清空指定表的所有数据

    - **table_name**: 表名

    ⚠️ 警告：此操作会删除表中的所有数据，但保留表结构
    """
    try:
        # 安全检查：只允许清空特定的数据表，不允许清空系统表
        system_tables = ['sync_log', 'sync_log_history']
        if table_name in system_tables:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot truncate system table: {table_name}"
            )

        # 验证表名是否为已知表
        if table_name not in db_client._ALL_TABLES:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown table: {table_name}"
            )

        # 执行清空操作：DolphinDB 用 dropPartition 或 delete 全量
        db_path = db_client._resolve_db_path(table_name)
        sql = f'DELETE FROM loadTable("{db_path}", "{table_name}")'
        db_client.execute(sql)

        logger.info(f"Truncated table {table_name}")
        return {
            "status": "success",
            "message": f"Table {table_name} has been truncated",
            "table_name": table_name
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to truncate table {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/tables")
def list_tables():
    """
    列出数据库中的所有表

    返回 DolphinDB 中所有已知表的名称、行数和列信息
    """
    try:
        tables_info = db_client.list_tables()
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
