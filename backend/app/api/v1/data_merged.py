"""
数据 API 路由（合并版）
整合了数据查询和配置化同步功能
"""
import json
from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import APIRouter, Query, HTTPException, status
from pydantic import BaseModel, Field

import httpx
import polars as pl
from store.dolphindb_client import db_client
from data_manager.refactored_sync_engine import sync_engine
from app.core.config import settings
from app.core.logger import logger


# Polars 类型名 -> DolphinDB 类型名（用于 ETL 自动建表和脚本测试）
POLARS_TO_DDB_TYPE_MAP = {
    "Utf8": "STRING", "String": "STRING",
    "Int8": "CHAR", "Int16": "SHORT", "Int32": "INT", "Int64": "LONG",
    "UInt8": "SHORT", "UInt16": "INT", "UInt32": "LONG", "UInt64": "LONG",
    "Float32": "FLOAT", "Float64": "DOUBLE",
    "Boolean": "BOOL", "Bool": "BOOL",
    "Date": "DATE", "Datetime": "TIMESTAMP", "Time": "TIME",
}


router = APIRouter()


# ==================== 请求/响应模型 ====================

class SyncTaskInfo(BaseModel):
    """同步任务信息"""
    task_id: str
    description: str
    sync_type: str
    table_name: str
    source: str = "tushare"
    enabled: bool = True


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


# ==================== 内部辅助函数 ====================

def _check_shared_and_drop_table(
    table_name: str, exclude_task_id: str, exclude_config_table: str
) -> bool:
    """
    检查表是否被其他任务共用；如果不共用则删除表。

    Args:
        table_name: 要删除的表名
        exclude_task_id: 当前正在删除的任务ID（排除在共用检查之外）
        exclude_config_table: 当前任务所在的配置表名（"sync_task_config" 或 "etl_task_config"）

    Returns:
        是否成功删除了表

    Raises:
        HTTPException(409): 如果表被其他任务共用
    """
    if not db_client.table_exists(table_name):
        return False

    # 查询两张配置表中引用该表的其他任务
    shared_tasks = []
    for config_table in ("sync_task_config", "etl_task_config"):
        exclude_clause = f" AND task_id != '{exclude_task_id}'" if config_table == exclude_config_table else ""
        others = db_client.query(
            f"SELECT task_id FROM {config_table} WHERE table_name = '{table_name}'{exclude_clause}"
        )
        if not others.is_empty():
            shared_tasks += others["task_id"].to_list()

    if shared_tasks:
        raise HTTPException(
            status_code=409,
            detail=f"表 {table_name} 被其他任务共用: {', '.join(shared_tasks)}。请先删除或修改这些任务，或选择仅删除配置。"
        )

    db_client.drop_table(table_name)
    return True


# ==================== 数据查询接口 ====================

@router.get("/data/stocks")
def list_stocks():
    """获取股票列表"""
    try:
        if not db_client.table_exists("sync_stock_basic"):
            return {"stocks": []}
        df = db_client.query("SELECT ts_code FROM sync_stock_basic ORDER BY ts_code")
        return {"stocks": df["ts_code"].to_list() if not df.is_empty() else []}
    except Exception as e:
        logger.error(f"Failed to list stocks: {e}")
        return {"stocks": []}


@router.get("/data/daily")
def get_daily(
    ts_code: str = Query(..., description="股票代码，如 000001.SZ"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYYMMDD"),
    limit: int = Query(500, le=5000, description="返回记录数限制"),
):
    """获取日线行情数据（OHLC完整数据，从 sync_daily_data 表）"""
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
        sql = f"SELECT * FROM sync_daily_data WHERE {where} ORDER BY trade_date DESC LIMIT {limit}"
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
                table_name=t.get("table_name", ""),
                source=t.get("source", "tushare"),
                enabled=t.get("enabled", True)
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

    - **task_id**: 任务ID（如 sync_daily_basic, sync_stock_basic）
    - **target_date**: 目标日期，不指定则只同步最新一天（增量任务）或执行一次（全量任务）
    - **start_date**: 开始日期（可选），用于指定同步的起始日期
    - **end_date**: 结束日期（可选），用于指定同步的结束日期

    注意：如果同时指定了 start_date 和 end_date，将使用这两个参数；否则使用 target_date
    """
    try:
        sync_date = start_date if start_date else target_date

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


@router.get("/data/sync/status")
def get_sync_status(
    limit: int = Query(1000, le=10000, description="返回记录数限制"),
    source: Optional[str] = Query(None, description="按来源筛选"),
    data_type: Optional[str] = Query(None, description="按数据类型筛选"),
    start_date: Optional[str] = Query(None, description="按同步创建时间筛选（起始，YYYYMMDD）"),
    end_date: Optional[str] = Query(None, description="按同步创建时间筛选（结束，YYYYMMDD）")
):
    """
    获取同步历史记录（支持筛���）

    返回所有同步历史记录，支持按来源、类型、创建时间筛选
    """
    try:
        conditions = ["source != 'etl'"]
        params = []

        if source:
            conditions.append("source = %s")
            params.append(source)
        if data_type:
            conditions.append("data_type = %s")
            params.append(data_type)
        if start_date:
            # 使用 date() 提取日期部分进行比较，DolphinDB 日期格式: YYYY.MM.DD
            conditions.append(f"date(created_at) >= {start_date[:4]}.{start_date[4:6]}.{start_date[6:8]}")
        if end_date:
            conditions.append(f"date(created_at) <= {end_date[:4]}.{end_date[4:6]}.{end_date[6:8]}")

        where_clause = " AND ".join(conditions) if conditions else ""
        where_part = f"WHERE {where_clause}" if where_clause else ""
        sql = f"""
            SELECT source, data_type, last_date, sync_date, rows_synced, status, error_message, params, created_at
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
    """
    try:
        # 确认任务存在
        existing = db_client.query(f"SELECT * FROM sync_task_config WHERE task_id = '{task_id}'")
        if existing.is_empty():
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

        # 构建更新行 — 基于 existing 的第一行，覆盖需要更新的字段
        now = datetime.now()
        first = existing.head(1)
        row = first.with_columns(
            pl.lit(config.get("task_id", task_id)).alias("task_id"),
            pl.lit(config.get("api_name", "")).alias("api_name"),
            pl.lit(config.get("description", "")).alias("description"),
            pl.lit(config.get("sync_type", "incremental")).alias("sync_type"),
            pl.lit(json.dumps(config.get("params", {}), ensure_ascii=False)).alias("params_json"),
            pl.lit(config.get("date_field", "")).alias("date_field"),
            pl.lit(json.dumps(config.get("primary_keys", []))).alias("primary_keys_json"),
            pl.lit(config.get("table_name", "")).alias("table_name"),
            pl.lit(json.dumps(config.get("schema", {}), ensure_ascii=False)).alias("schema_json"),
            pl.lit(config.get("enabled", True)).alias("enabled"),
            pl.lit(config.get("api_limit", 5000)).alias("api_limit"),
            pl.lit(now).alias("updated_at"),
        )
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
        now = datetime.now()
        row = pl.DataFrame({
            "task_id": [config["task_id"]],
            "api_name": [config["api_name"]],
            "description": [config.get("description", "")],
            "sync_type": [config["sync_type"]],
            "params_json": [json.dumps(config.get("params", {}), ensure_ascii=False)],
            "date_field": [config.get("date_field", "")],
            "primary_keys_json": [json.dumps(config["primary_keys"])],
            "table_name": [config["table_name"]],
            "schema_json": [json.dumps(config.get("schema", {}), ensure_ascii=False)],
            "enabled": [config.get("enabled", True)],
            "api_limit": [config.get("api_limit", 5000)],
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
def delete_task(task_id: str, drop_table: bool = False):
    """
    删除指定的同步任务

    - **task_id**: 任务ID
    - **drop_table**: 是否同时删除数据库表
    """
    try:
        # 确认任务存在并获取表名
        existing = db_client.query(f"SELECT task_id, table_name FROM sync_task_config WHERE task_id = '{task_id}'")
        if existing.is_empty():
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

        # 从配置中获取实际表名
        table_name = existing["table_name"][0] if "table_name" in existing.columns and existing["table_name"][0] else task_id
        table_dropped = False
        if drop_table:
            table_dropped = _check_shared_and_drop_table(table_name, task_id, "sync_task_config")

        # 从数据库删除配置
        db_client.execute(f"DELETE FROM sync_task_config WHERE task_id = '{task_id}'")
        sync_engine.config_manager.reload()

        msg = f"Task {task_id} deleted"
        if table_dropped:
            msg += f", table {table_name} dropped"
        logger.info(msg)
        return {"status": "success", "message": msg}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/sync/task/{task_id}/create-table")
def create_sync_task_table(task_id: str):
    """根据任务配置创建数据表"""
    try:
        task_config = sync_engine.config_manager.get_task(task_id)
        if not task_config:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        sync_engine.table_manager.ensure_table_exists(task_config)
        return {"status": "success", "message": f"表 {task_config.get('table_name')} 创建成功"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create table for task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 数据库管理接口 ====================

@router.get("/data/tables/{table_name}/info")
def get_table_info(table_name: str):
    """检查表是否存在，返回列名列表"""
    try:
        exists = db_client.table_exists(table_name)
        if not exists:
            return {"exists": False, "columns": []}
        columns = db_client.get_table_columns(table_name)
        return {"exists": True, "columns": columns}
    except Exception as e:
        logger.warning(f"Failed to get table info for {table_name}: {e}")
        return {"exists": False, "columns": []}


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


# ==================== ETL 任务管理 ====================

def _etl_log_sync(task_id: str, sync_date: str, rows_synced: int, status: str = "success", error_message: str = "", params: str = ""):
    """记录 ETL 同步日志"""
    try:
        # sync_log (最新状态)
        db_client.upsert("sync_log", pl.DataFrame({
            "source": ["etl"],
            "data_type": [task_id],
            "last_date": [sync_date],
            "updated_at": [datetime.now()]
        }), ["source", "data_type"])
        # sync_log_history (历史)
        db_client.bulk_copy("sync_log_history", pl.DataFrame({
            "source": ["etl"],
            "data_type": [task_id],
            "last_date": [sync_date],
            "sync_date": [sync_date],
            "rows_synced": [rows_synced],
            "status": [status],
            "error_message": [error_message],
            "params": [params],
            "created_at": [datetime.now()]
        }))
    except Exception as e:
        logger.error(f"ETL log failed for {task_id}: {e}")


def _etl_execute_and_write(task_id: str, script: str, task_config: dict):
    """执行 ETL 脚本并写入目标表，返回行数"""
    result_df = db_client.query(script)
    if result_df.is_empty():
        return 0

    table_name = task_config.get("table_name", task_id)
    sync_type = task_config.get("sync_type", "incremental")
    primary_keys = json.loads(task_config.get("primary_keys_json", "[]")) if isinstance(task_config.get("primary_keys_json"), str) else task_config.get("primary_keys", [])

    # 确保动态 ETL 表被识别为 meta 表
    db_client.register_meta_table(table_name)

    # 如果目标表不存在，根据 DataFrame schema 自动创建
    db_path = db_client._db_path
    try:
        exists = db_client._session.run(f"existsTable('{db_path}', '{table_name}')")
    except Exception:
        exists = False
    if not exists:
        col_defs = []
        for col in result_df.columns:
            base = str(result_df[col].dtype).split("(")[0].replace("pl.", "")
            ddb_type = POLARS_TO_DDB_TYPE_MAP.get(base, "STRING")
            col_defs.append(f"array({ddb_type},0) as {col}")
        if not col_defs:
            raise RuntimeError(f"ETL 脚本结果没有列，无法自动建表 [{table_name}]")

        # TSDB 引擎要求 primaryKey 最后一列为时间或整数类型
        _TEMPORAL_INT = {"DATE", "DATETIME", "TIMESTAMP", "INT", "LONG", "SHORT"}
        pk_list = list(primary_keys) if primary_keys else [result_df.columns[0]]
        # 检查最后一列类型
        last_col = pk_list[-1]
        last_type = POLARS_TO_DDB_TYPE_MAP.get(str(result_df[last_col].dtype).split("(")[0].replace("pl.", ""), "STRING") if last_col in result_df.columns else "STRING"
        if last_type not in _TEMPORAL_INT:
            for col in result_df.columns:
                base = str(result_df[col].dtype).split("(")[0].replace("pl.", "")
                ddb_t = POLARS_TO_DDB_TYPE_MAP.get(base, "STRING")
                if ddb_t in _TEMPORAL_INT and col not in pk_list:
                    pk_list.append(col)
                    break
            else:
                col_defs.append("array(TIMESTAMP,0) as created_at")
                pk_list.append("created_at")
        pk_str = "`" + "`".join(pk_list)

        create_script = (
            f"dbMeta = database('{db_path}');"
            f"schema_{table_name} = table({','.join(col_defs)});"
            f"createTable(dbHandle=dbMeta, table=schema_{table_name}, tableName=`{table_name}, primaryKey={pk_str});"
        )
        db_client.execute(create_script)
        logger.info(f"Auto-created ETL table {table_name} with columns: {result_df.columns}")
        # 刚建表，直接使用已知列顺序，跳过 schema 查询
        known_cols = list(result_df.columns)
    else:
        known_cols = None  # 表已存在，让 upsert/bulk_copy 自行查询 schema

    if sync_type == "full":
        # 全量：清空后写入
        try:
            db_client.execute(f"DELETE FROM {table_name} WHERE 1=1")
        except Exception:
            pass
        db_client.bulk_copy(table_name, result_df, known_columns=known_cols)
    else:
        # 增量：upsert
        db_client.upsert(table_name, result_df, primary_keys, known_columns=known_cols)

    return len(result_df)


@router.get("/data/etl/tasks")
def list_etl_tasks():
    """列出所有 ETL 任务，附带最新同步状态"""
    try:
        df = db_client.query("SELECT * FROM etl_task_config")
        if df.is_empty():
            return {"tasks": [], "total": 0}
        tasks = df.to_dicts()

        # 批量获取 ETL 任务的最新同步状态
        try:
            status_sql = (
                f'SELECT data_type, last_date, updated_at '
                f'FROM loadTable("{db_client._db_path}", "sync_log") '
                f'WHERE source = "etl"'
            )
            status_df = db_client.query(status_sql)
            if not status_df.is_empty():
                status_map = {
                    row["data_type"]: {
                        "last_date": row.get("last_date"),
                        "last_sync_time": str(row.get("updated_at")) if row.get("updated_at") else None,
                    }
                    for row in status_df.to_dicts()
                }
                for task in tasks:
                    s = status_map.get(task.get("task_id"), {})
                    task["last_date"] = s.get("last_date")
                    task["last_sync_time"] = s.get("last_sync_time")
        except Exception as e:
            logger.warning(f"Failed to load ETL task statuses: {e}")

        return {"tasks": tasks, "total": len(tasks)}
    except Exception as e:
        logger.error(f"Failed to list ETL tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/etl/tasks")
def create_etl_task(config: dict):
    """创建 ETL 任务"""
    try:
        task_id = config.get("task_id")
        if not task_id:
            raise HTTPException(status_code=400, detail="Missing required field: task_id")

        existing = db_client.query(f"SELECT task_id FROM etl_task_config WHERE task_id = '{task_id}'")
        if not existing.is_empty():
            raise HTTPException(status_code=400, detail=f"ETL task {task_id} already exists")

        now = datetime.now()
        row = pl.DataFrame({
            "task_id": [task_id],
            "description": [config.get("description", "")],
            "script": [config.get("script", "")],
            "sync_type": [config.get("sync_type", "incremental")],
            "date_field": [config.get("date_field", "")],
            "primary_keys_json": [json.dumps(config.get("primary_keys", []))],
            "table_name": [config.get("table_name", task_id)],
            "enabled": [config.get("enabled", True)],
            "created_at": [now],
            "updated_at": [now],
        })
        db_client.upsert("etl_task_config", row, ["task_id"])
        logger.info(f"Created ETL task {task_id}")
        return {"status": "success", "message": f"ETL task {task_id} created"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create ETL task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/data/etl/task/{task_id}")
def update_etl_task(task_id: str, config: dict):
    """更新 ETL 任务"""
    try:
        existing = db_client.query(f"SELECT * FROM etl_task_config WHERE task_id = '{task_id}'")
        if existing.is_empty():
            raise HTTPException(status_code=404, detail=f"ETL task {task_id} not found")

        now = datetime.now()
        # 保留原始 created_at
        created_at = existing["created_at"][0] if "created_at" in existing.columns else now
        row = pl.DataFrame({
            "task_id": [config.get("task_id", task_id)],
            "description": [config.get("description", "")],
            "script": [config.get("script", "")],
            "sync_type": [config.get("sync_type", "incremental")],
            "date_field": [config.get("date_field", "")],
            "primary_keys_json": [json.dumps(config.get("primary_keys", []))],
            "table_name": [config.get("table_name", task_id)],
            "enabled": [config.get("enabled", True)],
            "created_at": [created_at],
            "updated_at": [now],
        })
        db_client.upsert("etl_task_config", row, ["task_id"])
        logger.info(f"Updated ETL task {task_id}")
        return {"status": "success", "message": f"ETL task {task_id} updated"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update ETL task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/data/etl/task/{task_id}")
def delete_etl_task(task_id: str, drop_table: bool = False):
    """删除 ETL 任务"""
    try:
        existing = db_client.query(f"SELECT task_id FROM etl_task_config WHERE task_id = '{task_id}'")
        if existing.is_empty():
            raise HTTPException(status_code=404, detail=f"ETL task {task_id} not found")

        table_dropped = False
        if drop_table:
            # 容错：table_name 列可能不存在（旧表结构，启动后 ensure_meta_tables 会补列）
            table_name = None
            try:
                res = db_client.query(f"SELECT table_name FROM etl_task_config WHERE task_id = '{task_id}'")
                if not res.is_empty() and "table_name" in res.columns:
                    table_name = res["table_name"][0]
            except Exception:
                pass
            if table_name:
                table_dropped = _check_shared_and_drop_table(table_name, task_id, "etl_task_config")

        db_client.execute(f"DELETE FROM etl_task_config WHERE task_id = '{task_id}'")
        msg = f"ETL task {task_id} deleted"
        if table_dropped:
            msg += f", table dropped"
        logger.info(msg)
        return {"status": "success", "message": msg}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete ETL task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/etl/task/{task_id}/status")
def get_etl_task_status(task_id: str):
    """获取 ETL 任务的最新数据日期和上次同步时间"""
    try:
        sql = (
            f'SELECT last_date, updated_at '
            f'FROM loadTable("{db_client._db_path}", "sync_log") '
            f'WHERE source = "etl" AND data_type = "{task_id}" LIMIT 1'
        )
        df = db_client.query(sql)
        if df.is_empty():
            return {"last_date": None, "last_sync_time": None}
        row = df.to_dicts()[0]
        return {
            "last_date": row.get("last_date"),
            "last_sync_time": str(row.get("updated_at")) if row.get("updated_at") else None,
        }
    except Exception as e:
        logger.warning(f"Failed to get ETL task status for {task_id}: {e}")
        return {"last_date": None, "last_sync_time": None}


@router.post("/data/etl/task/{task_id}/run")
def run_etl_task(task_id: str):
    """执行 ETL 任务脚本，{date} 替换为当天日期"""
    try:
        df = db_client.query(f"SELECT * FROM etl_task_config WHERE task_id = '{task_id}'")
        if df.is_empty():
            raise HTTPException(status_code=404, detail=f"ETL task {task_id} not found")

        task = df.to_dicts()[0]
        script = task.get("script", "")
        if not script or not script.strip():
            raise HTTPException(status_code=400, detail="ETL script is empty")

        today = datetime.now().strftime("%Y.%m.%d")
        script = script.replace("{date}", today)
        run_params = f"type=run, date={today}"

        rows = _etl_execute_and_write(task_id, script, task)
        _etl_log_sync(task_id, datetime.now().strftime("%Y%m%d"), rows, params=run_params)

        logger.info(f"ETL task {task_id} executed: {rows} rows")
        return {"status": "success", "message": f"ETL task {task_id} 执行成功, {rows} 行"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"ETL task {task_id} execution failed: {e}")
        _etl_log_sync(task_id, datetime.now().strftime("%Y%m%d"), 0, "failed", str(e), params=f"type=run, date={datetime.now().strftime('%Y.%m.%d')}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/etl/test")
def test_etl_script(payload: dict):
    """测试 ETL 脚本，返回执行结果及字段类型"""
    try:
        script = payload.get("script", "")
        if not script or not script.strip():
            raise HTTPException(status_code=400, detail="脚本为空")

        test_date = payload.get("date", "")
        if test_date:
            try:
                date_obj = datetime.strptime(test_date, "%Y%m%d")
                date_str = date_obj.strftime("%Y.%m.%d")
            except ValueError:
                date_str = test_date
        else:
            date_str = datetime.now().strftime("%Y.%m.%d")
        script = script.replace("{date}", date_str)

        result_df = db_client.query(script)
        rows = len(result_df) if not result_df.is_empty() else 0
        columns = result_df.columns if not result_df.is_empty() else []
        preview = result_df.head(5).to_dicts() if not result_df.is_empty() else []

        # 推断 DolphinDB 类型
        field_types = []
        if not result_df.is_empty():
            for col in columns:
                dtype_str = str(result_df[col].dtype)
                base = dtype_str.split("(")[0].replace("pl.", "")
                ddb_type = POLARS_TO_DDB_TYPE_MAP.get(base, "STRING")
                field_types.append({"name": col, "type": ddb_type})

        return {
            "status": "success",
            "rows": rows,
            "columns": columns,
            "field_types": field_types,
            "preview": preview,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"ETL script test failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/etl/task/{task_id}/backfill")
def backfill_etl_task(
    task_id: str,
    start_date: str = Query(..., description="开始日期 YYYYMMDD"),
    end_date: str = Query(..., description="结束日期 YYYYMMDD"),
):
    """回溯执行 ETL 任务，逐天替换 {date} 执行脚本并写入目标表"""
    try:
        df = db_client.query(f"SELECT * FROM etl_task_config WHERE task_id = '{task_id}'")
        if df.is_empty():
            raise HTTPException(status_code=404, detail=f"ETL task {task_id} not found")

        task = df.to_dicts()[0]
        script_template = task.get("script", "")
        if not script_template or not script_template.strip():
            raise HTTPException(status_code=400, detail="ETL script is empty")

        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
        if start > end:
            raise HTTPException(status_code=400, detail="start_date must be <= end_date")

        results = []
        current = start
        while current <= end:
            date_str = current.strftime("%Y.%m.%d")
            date_yyyymmdd = current.strftime("%Y%m%d")
            backfill_params = f"type=backfill, date={date_str}, range={start_date}~{end_date}"
            script = script_template.replace("{date}", date_str)
            try:
                rows = _etl_execute_and_write(task_id, script, task)
                _etl_log_sync(task_id, date_yyyymmdd, rows, params=backfill_params)
                results.append({"date": date_yyyymmdd, "status": "success", "rows": rows})
            except Exception as ex:
                _etl_log_sync(task_id, date_yyyymmdd, 0, "failed", str(ex), params=backfill_params)
                results.append({"date": date_yyyymmdd, "status": "failed", "error": str(ex)})
            current += timedelta(days=1)

        success_count = sum(1 for r in results if r["status"] == "success")
        logger.info(f"ETL backfill {task_id}: {success_count}/{len(results)} days succeeded")
        return {
            "status": "success",
            "message": f"回溯完成: {success_count}/{len(results)} 天成功",
            "results": results,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"ETL backfill {task_id} failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/etl/task/{task_id}/create-table")
def create_etl_table(task_id: str, payload: dict):
    """根据字段定义创建 ETL 目标表"""
    try:
        table_name = payload.get("table_name", task_id)
        fields = payload.get("fields", [])  # [{"name": "col", "type": "STRING"}, ...]
        if not fields:
            raise HTTPException(status_code=400, detail="字段定义为空")

        # 注册到元数据表集合以便 SQL 适配器识别
        db_client.register_meta_table(table_name)

        # 检查表是否已存在
        db_path = db_client._db_path
        exists = db_client._session.run(f"existsTable('{db_path}', '{table_name}')")
        if exists:
            return {"status": "success", "message": f"表 {table_name} 已存在"}

        # 构建建表脚本
        col_defs = ",".join([f"array({f['type']},0) as {f['name']}" for f in fields])

        # TSDB 引擎要求 primaryKey 最后一列为时间或整数类型
        _TEMPORAL_INT = {"DATE", "DATETIME", "TIMESTAMP", "INT", "LONG", "SHORT"}
        pk_list = [fields[0]["name"]]
        if fields[0]["type"] not in _TEMPORAL_INT:
            for f in fields:
                if f["type"] in _TEMPORAL_INT and f["name"] != pk_list[0]:
                    pk_list.append(f["name"])
                    break
            else:
                col_defs += ",array(TIMESTAMP,0) as created_at"
                pk_list.append("created_at")
        pk_str = "`" + "`".join(pk_list)

        script = (
            f"dbMeta = database('{db_path}');"
            f"schema_{table_name} = table({col_defs});"
            f"createTable(dbHandle=dbMeta, table=schema_{table_name}, tableName=`{table_name}, primaryKey={pk_str});"
        )
        db_client.execute(script)
        logger.info(f"Created ETL table {table_name}")
        return {"status": "success", "message": f"表 {table_name} 创建成功"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create ETL table: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/etl/task/{task_id}/schema")
def get_etl_table_schema(task_id: str):
    """获取 ETL 目标表的字段名和类型"""
    try:
        # 查表名
        res = db_client.query(f"SELECT table_name FROM etl_task_config WHERE task_id = '{task_id}'")
        table_name = res["table_name"][0] if not res.is_empty() and "table_name" in res.columns else task_id
        if not table_name:
            table_name = task_id

        db_path = db_client._db_path
        exists = db_client._session.run(f"existsTable('{db_path}', '{table_name}')")
        if not exists:
            return {"fields": []}

        import pandas as pd
        schema_info = db_client._session.run(f"schema(loadTable('{db_path}', '{table_name}'))")
        if isinstance(schema_info, dict) and "colDefs" in schema_info:
            col_defs = schema_info["colDefs"]
            if isinstance(col_defs, pd.DataFrame) and "name" in col_defs.columns:
                type_col = "typeString" if "typeString" in col_defs.columns else "type"
                fields = [
                    {"name": row["name"], "type": str(row.get(type_col, ""))}
                    for _, row in col_defs.iterrows()
                ]
                return {"fields": fields}
        return {"fields": []}
    except Exception as e:
        logger.warning(f"Failed to get ETL table schema for {task_id}: {e}")
        return {"fields": []}


@router.get("/data/etl/logs")
def get_etl_logs(
    task_id: Optional[str] = Query(None, description="按任务ID筛选"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYYMMDD"),
    limit: int = Query(1000, le=10000),
):
    """获取 ETL 任务同步日志"""
    try:
        conditions = ["source = 'etl'"]
        if task_id:
            conditions.append(f"data_type = '{task_id}'")
        if start_date:
            conditions.append(f"date(created_at) >= {start_date[:4]}.{start_date[4:6]}.{start_date[6:8]}")
        if end_date:
            conditions.append(f"date(created_at) <= {end_date[:4]}.{end_date[4:6]}.{end_date[6:8]}")

        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT source, data_type, last_date, sync_date, rows_synced, status, error_message, params, created_at
            FROM sync_log_history
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT {limit}
        """
        df = db_client.query(sql)
        return {
            "logs": df.to_dicts() if not df.is_empty() else [],
            "count": len(df)
        }
    except Exception as e:
        logger.error(f"Failed to get ETL logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))
