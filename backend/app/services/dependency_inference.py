"""
Dependency Inference Service
Automatically infers task dependencies based on task configurations
"""
import re
from typing import List
from app.core.logger import logger
from app.models.flow_config import TaskInDAG


class DependencyInferenceService:
    """Service for inferring task dependencies"""

    async def infer_dependencies(self, tasks: List[TaskInDAG]) -> List[TaskInDAG]:
        """Infer dependencies for tasks"""
        logger.info(f"Received tasks for inference: {[(t.id, t.type, t.depends_on) for t in tasks]}")
        result = []

        for task in tasks:
            task_copy = TaskInDAG(
                id=task.id,
                type=task.type,
                depends_on=task.depends_on.copy() if task.depends_on else [],
            )

            if not task.depends_on:
                inferred = await self._infer_for_task(task)
                if inferred:
                    task_copy.depends_on = inferred
                    logger.info(f"Inferred dependencies for {task.id}: {inferred}")

            result.append(task_copy)

        logger.info(f"Returning tasks after inference: {[(t.id, t.type, t.depends_on) for t in result]}")
        return result

    async def _infer_for_task(self, task: TaskInDAG) -> List[str]:
        """Infer dependencies for a single task"""
        if task.type == "factor":
            return await self._infer_for_factor(task.id)
        elif task.type == "etl":
            return await self._infer_for_etl(task.id)
        return []

    async def _infer_for_factor(self, factor_id: str) -> List[str]:
        """Infer dependencies for a factor task from factor_configs (PostgreSQL)"""
        try:
            from scheduler.db import DatabasePool
            import json

            row = await DatabasePool.fetchrow(
                "SELECT depends_on FROM factor_configs WHERE factor_id = $1", factor_id
            )
            if row and row["depends_on"]:
                depends_on_raw = row["depends_on"]
                if isinstance(depends_on_raw, str):
                    return json.loads(depends_on_raw)
                elif isinstance(depends_on_raw, list):
                    return depends_on_raw
                else:
                    return []
        except Exception as e:
            logger.warning(f"Failed to infer factor dependencies for {factor_id}: {e}")
        return []

    async def _infer_for_etl(self, etl_id: str) -> List[str]:
        """Infer dependencies for an ETL task from etl_task_configs (PostgreSQL)"""
        try:
            from scheduler.db import DatabasePool
            logger.info(f"Starting ETL dependency inference for {etl_id}")

            row = await DatabasePool.fetchrow(
                "SELECT script FROM etl_task_configs WHERE task_id = $1", etl_id
            )
            if row and row["script"]:
                source_tables = self._extract_source_tables(row["script"])
                logger.info(f"Extracted source tables for {etl_id}: {source_tables}")
                deps = await self._map_tables_to_tasks(source_tables)
                logger.info(f"Mapped tasks for {etl_id}: {deps}")
                return deps
        except Exception as e:
            logger.warning(f"Failed to infer ETL dependencies for {etl_id}: {e}", exc_info=True)
        return []

    def _extract_source_tables(self, script: str) -> List[str]:
        """Extract source table references from ETL script (simplified)"""
        tables = []
        patterns = [
            r"FROM\s+(\w+)", r"from\s+(\w+)",
            r"JOIN\s+(\w+)", r"join\s+(\w+)",
            r'loadTable\("[^"]*",\s*"(\w+)"\)',
            r"loadTable\('[^']*',\s*'(\w+)'\)",
        ]
        for pattern in patterns:
            tables.extend(re.findall(pattern, script))
        return list(set(tables))

    async def _map_tables_to_tasks(self, tables: List[str]) -> List[str]:
        """Map table names to sync or etl task IDs (PostgreSQL)"""
        try:
            from scheduler.db import DatabasePool

            sync_rows = await DatabasePool.fetch(
                "SELECT task_id, table_name FROM sync_task_configs WHERE enabled = true"
            )
            etl_rows = await DatabasePool.fetch(
                "SELECT task_id, table_name FROM etl_task_configs WHERE enabled = true"
            )

            table_to_task = {}
            for row in sync_rows:
                if row["table_name"] and row["task_id"]:
                    table_to_task[row["table_name"]] = row["task_id"]
            for row in etl_rows:
                if row["table_name"] and row["task_id"]:
                    table_to_task[row["table_name"]] = row["task_id"]

            task_ids = []
            for table in tables:
                if table in table_to_task:
                    task_ids.append(table_to_task[table])
                else:
                    logger.warning(f"No task found for table: {table}")
            return task_ids
        except Exception as e:
            logger.warning(f"Failed to map tables to tasks: {e}", exc_info=True)
            return []


# Singleton instance
dependency_inference_service = DependencyInferenceService()
