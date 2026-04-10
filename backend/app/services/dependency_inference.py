"""
Dependency Inference Service
Automatically infers task dependencies based on task configurations
"""
from typing import List
from app.core.logger import logger
from app.models.flow_config import TaskInDAG


class DependencyInferenceService:
    """Service for inferring task dependencies"""

    def infer_dependencies(self, tasks: List[TaskInDAG]) -> List[TaskInDAG]:
        """
        Infer dependencies for tasks

        Args:
            tasks: List of tasks with empty depends_on

        Returns:
            Tasks with inferred dependencies
        """
        import json
        logger.info(f"Received tasks for inference: {[(t.id, t.type, t.depends_on) for t in tasks]}")
        result = []

        for task in tasks:
            task_copy = TaskInDAG(
                id=task.id,
                type=task.type,
                depends_on=task.depends_on.copy() if task.depends_on else [],
            )

            if not task.depends_on:
                # Try to infer dependencies
                inferred = self._infer_for_task(task)
                if inferred:
                    task_copy.depends_on = inferred
                    logger.info(f"Inferred dependencies for {task.id}: {inferred}")

            result.append(task_copy)

        logger.info(f"Returning tasks after inference: {[(t.id, t.type, t.depends_on) for t in result]}")
        # Note: We don't validate dependencies here because a task might depend on
        # another task that's not included in the current selection. The DAGEditor
        # should handle this gracefully.
        return result

    def _infer_for_task(self, task: TaskInDAG) -> List[str]:
        """Infer dependencies for a single task"""
        if task.type == "factor":
            return self._infer_for_factor(task.id)
        elif task.type == "etl":
            return self._infer_for_etl(task.id)
        elif task.type == "sync":
            # Sync tasks usually don't have dependencies
            return []
        return []

    def _infer_for_factor(self, factor_id: str) -> List[str]:
        """Infer dependencies for a factor task"""
        try:
            from store.dolphindb_client import db_client
            import json

            df = db_client.query("""
                SELECT depends_on FROM factor_metadata WHERE factor_id = %s
            """, (factor_id,))

            if not df.is_empty():
                depends_on_json = df["depends_on"][0]
                if depends_on_json:
                    depends_on = json.loads(depends_on_json)
                    # Return ALL dependencies (sync, etl, factor)
                    return depends_on
        except Exception as e:
            logger.warning(f"Failed to infer factor dependencies for {factor_id}: {e}")

        return []

    def _infer_for_etl(self, etl_id: str) -> List[str]:
        """Infer dependencies for an ETL task"""
        try:
            from store.dolphindb_client import db_client
            logger.info(f"Starting ETL dependency inference for {etl_id}")

            df = db_client.query("""
                SELECT script FROM etl_task_config WHERE task_id = %s
            """, (etl_id,))

            logger.info(f"ETL query result for {etl_id}: {df}")

            if not df.is_empty():
                script = df["script"][0]
                logger.info(f"ETL script for {etl_id}: {script}")
                if script:
                    # Simple parsing: look for table references
                    # In production, would use SQL parser
                    source_tables = self._extract_source_tables(script)
                    logger.info(f"Extracted source tables for {etl_id}: {source_tables}")
                    # Map tables to tasks (sync or etl)
                    deps = self._map_tables_to_tasks(source_tables)
                    logger.info(f"Mapped tasks for {etl_id}: {deps}")
                    return deps
        except Exception as e:
            logger.warning(f"Failed to infer ETL dependencies for {etl_id}: {e}", exc_info=True)

        return []

    def _extract_source_tables(self, script: str) -> List[str]:
        """Extract source table references from ETL script (simplified)"""
        # This is a simplified version - production would use proper SQL parsing
        import re
        tables = []
        # Look for common patterns
        patterns = [
            r"FROM\s+(\w+)",
            r"from\s+(\w+)",
            r"JOIN\s+(\w+)",
            r"join\s+(\w+)",
            # DolphinDB loadTable patterns: loadTable("db", "table_name")
            r'loadTable\("[^"]*",\s*"(\w+)"\)',
            r"loadTable\('[^']*',\s*'(\w+)'\)",
            r'loadTable\("[^"]*",\s*(\w+)\)',
            r"loadTable\('[^']*',\s*(\w+)\)",
        ]
        for pattern in patterns:
            matches = re.findall(pattern, script)
            tables.extend(matches)
        return list(set(tables))

    def _map_tables_to_tasks(self, tables: List[str]) -> List[str]:
        """Map table names to sync or etl task IDs"""
        try:
            from store.dolphindb_client import db_client
            logger.info(f"Mapping tables to tasks: {tables}")

            # First, check sync tasks
            sync_df = db_client.query("""
                SELECT task_id, table_name FROM sync_task_config WHERE enabled = true
            """)

            logger.info(f"Available sync tasks: {sync_df}")

            # Then, check ETL tasks
            etl_df = db_client.query("""
                SELECT task_id, table_name FROM etl_task_config WHERE enabled = true
            """)

            logger.info(f"Available ETL tasks: {etl_df}")

            table_to_task = {}
            # Add sync tasks
            if not sync_df.is_empty():
                for row in sync_df.to_dicts():
                    table_name = row.get("table_name")
                    task_id = row.get("task_id")
                    if table_name and task_id:
                        table_to_task[table_name] = task_id

            # Add ETL tasks
            if not etl_df.is_empty():
                for row in etl_df.to_dicts():
                    table_name = row.get("table_name")
                    task_id = row.get("task_id")
                    if table_name and task_id:
                        table_to_task[table_name] = task_id

            logger.info(f"Table to task mapping: {table_to_task}")

            task_ids = []
            for table in tables:
                if table in table_to_task:
                    task_ids.append(table_to_task[table])
                else:
                    logger.warning(f"No task found for table: {table}")

            logger.info(f"Final task IDs: {task_ids}")
            return task_ids
        except Exception as e:
            logger.warning(f"Failed to map tables to tasks: {e}", exc_info=True)
            return []


# Singleton instance
dependency_inference_service = DependencyInferenceService()
