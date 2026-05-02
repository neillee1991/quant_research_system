"""
调度器数据访问层（Async）
"""
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
import uuid

from app.core.logger import logger
from .db import DatabasePool
from .models import FlowRun, TaskRun, FlowStatus, TaskStatus, TriggerType


class FlowRepository:
    """Flow 配置数据访问"""

    @staticmethod
    async def list_all(enabled_only: bool = False) -> List[Dict[str, Any]]:
        """列出所有 Flow"""
        where_clause = "WHERE enabled = true" if enabled_only else ""
        query = f"""
            SELECT id, name, description, cron, timezone, tags, tasks,
                   date_offset_days, enabled, version, created_at, updated_at
            FROM flow_configs
            {where_clause}
            ORDER BY updated_at DESC
        """
        rows = await DatabasePool.fetch(query)
        result = []
        for row in rows:
            row_dict = dict(row)
            # Parse JSON fields if they come as strings
            if isinstance(row_dict.get('tasks'), str):
                row_dict['tasks'] = json.loads(row_dict['tasks'])
            if isinstance(row_dict.get('tags'), str):
                row_dict['tags'] = json.loads(row_dict['tags'])
            result.append(row_dict)
        return result

    @staticmethod
    async def get_by_name(name: str) -> Optional[Dict[str, Any]]:
        """根据名称获取 Flow"""
        query = """
            SELECT id, name, description, cron, timezone, tags, tasks,
                   date_offset_days, enabled, version, created_at, updated_at
            FROM flow_configs
            WHERE name = $1
        """
        row = await DatabasePool.fetchrow(query, name)
        if row:
            row_dict = dict(row)
            # Parse JSON fields if they come as strings
            if isinstance(row_dict.get('tasks'), str):
                row_dict['tasks'] = json.loads(row_dict['tasks'])
            if isinstance(row_dict.get('tags'), str):
                row_dict['tags'] = json.loads(row_dict['tags'])
            return row_dict
        return None

    @staticmethod
    async def get_latest_updated_at() -> Optional[datetime]:
        """获取最新的 updated_at 时间"""
        query = "SELECT MAX(updated_at) AS latest FROM flow_configs"
        row = await DatabasePool.fetchrow(query)
        return row["latest"] if row else None


class FlowRunRepository:
    """FlowRun 数据访问"""

    @staticmethod
    async def create(flow_run: FlowRun) -> int:
        """创建 FlowRun，返回整数 ID"""
        run_id = flow_run.run_id or uuid.uuid4().hex
        query = """
            INSERT INTO flow_runs
            (run_id, flow_name, parent_flow_run_id, status, trigger_type, target_date,
             scheduled_at, started_at, ended_at, error_message, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING id
        """
        now = datetime.now()
        flow_run_id = await DatabasePool.fetchval(
            query,
            run_id,
            flow_run.flow_name,
            flow_run.parent_flow_run_id,
            flow_run.status.value,
            flow_run.trigger_type.value,
            flow_run.target_date,
            flow_run.scheduled_at,
            flow_run.started_at,
            flow_run.ended_at,
            flow_run.error_message,
            flow_run.created_at or now,
        )
        flow_run.run_id = run_id
        return flow_run_id

    @staticmethod
    async def update_status(flow_run_id: int, status: FlowStatus,
                           error_message: Optional[str] = None) -> bool:
        """更新 FlowRun 状态"""
        set_clause = ["status = $2"]
        params = [flow_run_id, status.value]

        if status in [FlowStatus.RUNNING]:
            set_clause.append("started_at = $3")
            params.append(datetime.now())
        elif status in [FlowStatus.SUCCESS, FlowStatus.FAILED, FlowStatus.CANCELLED]:
            set_clause.append("ended_at = $3")
            params.append(datetime.now())

        if error_message:
            param_idx = len(params) + 1
            set_clause.append(f"error_message = ${param_idx}")
            params.append(error_message)

        query = f"""
            UPDATE flow_runs
            SET {', '.join(set_clause)}
            WHERE id = $1
        """
        await DatabasePool.execute(query, *params)
        return True

    @staticmethod
    async def get_by_id(flow_run_id: int) -> Optional[Dict[str, Any]]:
        """根据 ID 获取 FlowRun"""
        row = await DatabasePool.fetchrow(
            "SELECT * FROM flow_runs WHERE id = $1", flow_run_id
        )
        return dict(row) if row else None

    @staticmethod
    async def list_by_flow_name(flow_name: str, limit: int = 50) -> List[Dict[str, Any]]:
        """列出 Flow 的执行历史"""
        query = """
            SELECT id, run_id, flow_name, parent_flow_run_id, status, trigger_type,
                   target_date, scheduled_at, started_at, ended_at, error_message, created_at
            FROM flow_runs
            WHERE flow_name = $1
            ORDER BY created_at DESC
            LIMIT $2
        """
        rows = await DatabasePool.fetch(query, flow_name, limit)
        return [dict(row) for row in rows]


class TaskRunRepository:
    """TaskRun 数据访问"""

    @staticmethod
    async def create(task_run: TaskRun) -> int:
        """创建 TaskRun，返回 ID"""
        query = """
            INSERT INTO task_runs
            (flow_run_id, task_id, task_type, target_date, status, started_at, ended_at, error_message, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
        """
        now = datetime.now()
        task_run_id = await DatabasePool.fetchval(
            query,
            task_run.flow_run_id,
            task_run.task_id,
            task_run.task_type,
            task_run.target_date,
            task_run.status.value,
            task_run.started_at,
            task_run.ended_at,
            task_run.error_message,
            task_run.created_at or now,
        )
        return task_run_id

    @staticmethod
    async def update_status(task_run_id: int, status: TaskStatus,
                           error_message: Optional[str] = None) -> bool:
        """更新 TaskRun 状态"""
        set_clause = ["status = $2"]
        params = [task_run_id, status.value]

        if status in [TaskStatus.RUNNING]:
            set_clause.append("started_at = $3")
            params.append(datetime.now())
        elif status in [TaskStatus.SUCCESS, TaskStatus.FAILED]:
            set_clause.append("ended_at = $3")
            params.append(datetime.now())

        if error_message:
            param_idx = len(params) + 1
            set_clause.append(f"error_message = ${param_idx}")
            params.append(error_message)

        query = f"""
            UPDATE task_runs
            SET {', '.join(set_clause)}
            WHERE id = $1
        """
        await DatabasePool.execute(query, *params)
        return True

    @staticmethod
    async def list_by_flow_run_id(flow_run_id: int) -> List[Dict[str, Any]]:
        """列出 FlowRun 的所有 TaskRun"""
        query = """
            SELECT id, flow_run_id, task_id, task_type, status,
                   started_at, ended_at, error_message, created_at
            FROM task_runs
            WHERE flow_run_id = $1
            ORDER BY created_at
        """
        rows = await DatabasePool.fetch(query, flow_run_id)
        return [dict(row) for row in rows]
