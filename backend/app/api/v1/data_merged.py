"""
数据 API 路由（合并版）
整合了数据查询和配置化同步功能
"""
from typing import List, Optional
from fastapi import APIRouter, Query, HTTPException, status
from pydantic import BaseModel, Field

from store.postgres_client import db_client
from data_manager.refactored_sync_engine import sync_engine
from data_manager.scheduler import sync_scheduler
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

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        sql = f"""
            SELECT id, source, data_type, last_date, sync_date, rows_synced, status, created_at
            FROM sync_log_history
            WHERE {where_clause}
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
        if table_name:
            try:
                # 使用配置中指定的日期字段
                max_date_sql = f"SELECT MAX({date_field}) as max_date FROM {table_name}"
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

# ==================== 调度管理接口 ====================

@router.post("/data/sync/scheduler/start")
def start_scheduler():
    """启动调度器"""
    try:
        sync_scheduler.start()
        return {"status": "success", "message": "Scheduler started"}
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/sync/scheduler/stop")
def stop_scheduler():
    """停止调度器"""
    try:
        sync_scheduler.shutdown()
        return {"status": "success", "message": "Scheduler stopped"}
    except Exception as e:
        logger.error(f"Failed to stop scheduler: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/sync/scheduler/load")
def load_schedules():
    """从配置文件加载所有任务调度"""
    try:
        count = sync_scheduler.load_schedules_from_config()
        return {
            "status": "success",
            "message": f"Loaded {count} task schedules",
            "count": count
        }
    except Exception as e:
        logger.error(f"Failed to load schedules: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/sync/scheduler/schedules")
def get_all_schedules():
    """获取所有调度任务信息"""
    try:
        schedules = sync_scheduler.get_all_schedules()
        return {
            "schedules": schedules,
            "count": len(schedules)
        }
    except Exception as e:
        logger.error(f"Failed to get schedules: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/sync/scheduler/task/{task_id}/enable")
def enable_task_schedule(
    task_id: str,
    schedule: str = Query(..., description="调度类型: daily/weekly/monthly/custom"),
    cron_expression: Optional[str] = Query(None, description="自定义 cron 表达式（schedule=custom 时使用）")
):
    """
    启用任务调度

    - **task_id**: 任务ID
    - **schedule**: 调度类型 (daily/weekly/monthly/custom)
    - **cron_expression**: 自定义 cron 表达式（仅当 schedule=custom 时需要）
    """
    try:
        success = sync_scheduler.add_task_schedule(task_id, schedule, cron_expression)
        if success:
            return {
                "status": "success",
                "message": f"Schedule enabled for task {task_id}",
                "task_id": task_id,
                "schedule": schedule
            }
        else:
            raise HTTPException(status_code=400, detail="Failed to enable schedule")
    except Exception as e:
        logger.error(f"Failed to enable schedule for task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/sync/scheduler/task/{task_id}/disable")
def disable_task_schedule(task_id: str):
    """
    禁用任务调度

    - **task_id**: 任务ID
    """
    try:
        success = sync_scheduler.remove_task_schedule(task_id)
        if success:
            return {
                "status": "success",
                "message": f"Schedule disabled for task {task_id}",
                "task_id": task_id
            }
        else:
            return {
                "status": "warning",
                "message": f"No schedule found for task {task_id}",
                "task_id": task_id
            }
    except Exception as e:
        logger.error(f"Failed to disable schedule for task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/sync/scheduler/task/{task_id}")
def get_task_schedule_info(task_id: str):
    """
    获取任务调度信息（包含历史统计）

    - **task_id**: 任务ID
    """
    try:
        info = sync_scheduler.get_task_schedule_info(task_id)

        # 获取调度历史统计
        try:
            # 获取上次运行时间和成功次数
            history_sql = """
                SELECT
                    MAX(created_at) as last_run_time,
                    COUNT(*) FILTER (WHERE status = 'success') as success_count
                FROM sync_log_history
                WHERE source = 'tushare_config' AND data_type = %s
            """
            df = db_client.query(history_sql, (task_id,))

            if not df.is_empty():
                if info is None:
                    info = {"task_id": task_id}

                last_run = df["last_run_time"][0]
                info["last_run_time"] = last_run.isoformat() if last_run else None
                info["success_count"] = int(df["success_count"][0]) if df["success_count"][0] else 0
        except Exception as e:
            logger.warning(f"Failed to get schedule history for {task_id}: {e}")

        if info:
            return info
        else:
            return {
                "task_id": task_id,
                "message": "No schedule configured for this task"
            }
    except Exception as e:
        logger.error(f"Failed to get schedule info for task {task_id}: {e}")
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
        import json
        from pathlib import Path

        # 读取当前配置文件
        config_path = sync_engine.config_manager.config_path
        with open(config_path, 'r', encoding='utf-8') as f:
            full_config = json.load(f)

        # 查找并更新任务
        tasks = full_config.get("sync_tasks", [])
        task_found = False
        for i, task in enumerate(tasks):
            if task.get("task_id") == task_id:
                tasks[i] = config
                task_found = True
                break

        if not task_found:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

        # 保存配置文件
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(full_config, f, indent=2, ensure_ascii=False)

        # 重新加载配置
        sync_engine.config_manager.config = sync_engine.config_manager._load_config()

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
        import json

        # 验证必需字段
        required_fields = ["task_id", "api_name", "sync_type", "table_name", "primary_keys"]
        for field in required_fields:
            if field not in config:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

        # 读取当前配置文件
        config_path = sync_engine.config_manager.config_path
        with open(config_path, 'r', encoding='utf-8') as f:
            full_config = json.load(f)

        # 检查任务ID是否已存在
        tasks = full_config.get("sync_tasks", [])
        for task in tasks:
            if task.get("task_id") == config["task_id"]:
                raise HTTPException(status_code=400, detail=f"Task {config['task_id']} already exists")

        # 添加新任务
        tasks.append(config)

        # 保存配置文件
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(full_config, f, indent=2, ensure_ascii=False)

        # 重新加载配置
        sync_engine.config_manager.config = sync_engine.config_manager._load_config()

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
        import json

        # 读取当前配置文件
        config_path = sync_engine.config_manager.config_path
        with open(config_path, 'r', encoding='utf-8') as f:
            full_config = json.load(f)

        # 查找并删除任务
        tasks = full_config.get("sync_tasks", [])
        task_found = False
        new_tasks = []
        for task in tasks:
            if task.get("task_id") == task_id:
                task_found = True
                logger.info(f"Deleting task {task_id}")
            else:
                new_tasks.append(task)

        if not task_found:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

        # 更新配置
        full_config["sync_tasks"] = new_tasks

        # 保存配置文件
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(full_config, f, indent=2, ensure_ascii=False)

        # 重新加载配置
        sync_engine.config_manager.config = sync_engine.config_manager._load_config()

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

        # 执行清空操作
        sql = f"DELETE FROM {table_name}"
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

    返回数据库中所有表的名称和行数
    """
    try:
        # 获取所有表名（PostgreSQL 语法）
        tables_df = db_client.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'"
        )

        # 表名白名单（防止SQL注入）
        # 使用模式匹配支持分区表
        import re
        ALLOWED_TABLE_PATTERNS = [
            r'^stock_basic$',
            r'^daily_data$',
            r'^daily_basic$',
            r'^factor_values(_\d{6})?$',  # 支持 factor_values 和 factor_values_YYYYMM
            r'^factor_metadata$',
            r'^factor_analysis$',
            r'^sync_log(_history)?$',  # 支持 sync_log 和 sync_log_history
            r'^production_task_run$',
            r'^dag_run_log$',
            r'^dag_task_log$',
            r'^migration_history$',
            r'^adj_factor$',
            r'^index_daily$',
            r'^moneyflow$',
            r'^trade_cal$',
        ]

        def is_table_allowed(table_name: str) -> bool:
            """检查表名是否在白名单中"""
            return any(re.match(pattern, table_name) for pattern in ALLOWED_TABLE_PATTERNS)

        tables_info = []
        for table_name in tables_df["table_name"].to_list():
            # 验证表名
            if not is_table_allowed(table_name):
                logger.warning(f"Skipping unauthorized table: {table_name}")
                continue

            try:
                # 获取表的行数（使用参数化查询）
                count_df = db_client.query(
                    "SELECT COUNT(*) as count FROM " + table_name
                )
                row_count = count_df["count"][0] if not count_df.is_empty() else 0

                # 获取表结构（使用参数化查询）
                schema_df = db_client.query("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=%s
                    ORDER BY ordinal_position
                """, (table_name,))
                columns = schema_df["column_name"].to_list() if not schema_df.is_empty() else []

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
