"""
Flow Configuration Service (PostgreSQL 版本)
"""
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
import psycopg2
from psycopg2.extras import RealDictCursor

from app.core.logger import logger
from app.core.config import settings
from app.models.flow_config import (
    FlowConfigCreate,
    FlowConfigUpdate,
    FlowConfigInDB,
    FlowConfigListItem,
    TaskInDAG,
)


def get_db_connection():
    """获取 PostgreSQL 连接（同步）"""
    return psycopg2.connect(
        host=settings.postgresql.postgres_host,
        port=settings.postgresql.postgres_port,
        database=settings.postgresql.postgres_db,
        user=settings.postgresql.postgres_user,
        password=settings.postgresql.postgres_password,
    )


class FlowService:
    """Flow Configuration CRUD Service (PostgreSQL)"""

    @staticmethod
    def _parse_db_row(row: Dict[str, Any]) -> FlowConfigInDB:
        """Parse database row to FlowConfigInDB"""
        # Parse JSON fields
        tags = row.get("tags", []) or []
        tasks_data = row.get("tasks", []) or []
        tasks = [TaskInDAG(**t) for t in tasks_data]

        return FlowConfigInDB(
            name=row["name"],
            description=row.get("description", ""),
            cron=row.get("cron"),
            tags=tags,
            enabled=bool(row.get("enabled", True)),
            date_offset_days=int(row.get("date_offset_days", 0)),
            tasks=tasks,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            version=int(row.get("version", 1)),
        )

    @staticmethod
    def list_flows(enabled_only: bool = False) -> List[FlowConfigListItem]:
        """List all flows"""
        try:
            with get_db_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    if enabled_only:
                        cur.execute(
                            "SELECT name, description, cron, tags, enabled, date_offset_days, tasks, updated_at "
                            "FROM flow_config WHERE enabled = true ORDER BY updated_at DESC"
                        )
                    else:
                        cur.execute(
                            "SELECT name, description, cron, tags, enabled, date_offset_days, tasks, updated_at "
                            "FROM flow_config ORDER BY updated_at DESC"
                        )
                    rows = cur.fetchall()

            flows = []
            for row in rows:
                tasks_data = row.get("tasks", []) or []
                tags = row.get("tags", []) or []
                flows.append(FlowConfigListItem(
                    name=row["name"],
                    description=row.get("description", ""),
                    cron=row.get("cron") or "",
                    tags=tags,
                    enabled=bool(row.get("enabled", True)),
                    date_offset_days=int(row.get("date_offset_days", 0)),
                    task_count=len(tasks_data),
                    updated_at=row["updated_at"],
                ))
            return flows
        except Exception as e:
            logger.error(f"Failed to list flows: {e}", exc_info=True)
            raise

    @staticmethod
    def get_flow(name: str) -> Optional[FlowConfigInDB]:
        """Get a single flow by name"""
        try:
            with get_db_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT * FROM flow_config WHERE name = %s", (name,))
                    row = cur.fetchone()

            if not row:
                return None

            return FlowService._parse_db_row(dict(row))
        except Exception as e:
            logger.error(f"Failed to get flow {name}: {e}", exc_info=True)
            raise

    @staticmethod
    def create_flow(config: FlowConfigCreate) -> FlowConfigInDB:
        """Create a new flow"""
        try:
            # Check if flow already exists
            existing = FlowService.get_flow(config.name)
            if existing:
                raise ValueError(f"Flow with name '{config.name}' already exists")

            now = datetime.now()
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO flow_config
                        (name, description, cron, tags, tasks, enabled, date_offset_days,
                         version, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            config.name,
                            config.description,
                            config.cron,
                            json.dumps(config.tags) if config.tags else "[]",
                            json.dumps([t.model_dump() for t in config.tasks]) if config.tasks else "[]",
                            config.enabled,
                            config.date_offset_days,
                            1,
                            now,
                            now,
                        ),
                    )
                conn.commit()

            logger.info(f"Created flow: {config.name}")
            return FlowService.get_flow(config.name)
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Failed to create flow: {e}", exc_info=True)
            raise

    @staticmethod
    def update_flow(name: str, config: FlowConfigUpdate) -> FlowConfigInDB:
        """Update an existing flow"""
        try:
            # Get existing flow
            existing = FlowService.get_flow(name)
            if not existing:
                raise ValueError(f"Flow '{name}' not found")

            # Build update data
            update_data = {}
            if config.description is not None:
                update_data["description"] = config.description
            if config.cron is not None:
                update_data["cron"] = config.cron
            if config.tags is not None:
                update_data["tags"] = json.dumps(config.tags)
            if config.enabled is not None:
                update_data["enabled"] = config.enabled
            if config.date_offset_days is not None:
                update_data["date_offset_days"] = config.date_offset_days
            if config.tasks is not None:
                update_data["tasks"] = json.dumps([t.model_dump() for t in config.tasks])

            if not update_data:
                return existing

            # Update in database
            update_data["updated_at"] = datetime.now()
            update_data["version"] = existing.version + 1

            # Build update SQL
            set_clause = ", ".join([f"{k} = %s" for k in update_data.keys()])
            params = list(update_data.values()) + [name]

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"UPDATE flow_config SET {set_clause} WHERE name = %s", params)
                conn.commit()

            logger.info(f"Updated flow: {name}")
            return FlowService.get_flow(name)
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Failed to update flow {name}: {e}", exc_info=True)
            raise

    @staticmethod
    def delete_flow(name: str, soft_delete: bool = True) -> bool:
        """Delete a flow (soft delete by disabling, or hard delete)"""
        try:
            existing = FlowService.get_flow(name)
            if not existing:
                raise ValueError(f"Flow '{name}' not found")

            now = datetime.now()
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    if soft_delete:
                        # Soft delete: just disable
                        cur.execute(
                            "UPDATE flow_config SET enabled = false, updated_at = %s WHERE name = %s",
                            (now, name)
                        )
                        logger.info(f"Disabled (soft deleted) flow: {name}")
                    else:
                        # Hard delete
                        cur.execute("DELETE FROM flow_config WHERE name = %s", (name,))
                        logger.info(f"Hard deleted flow: {name}")
                conn.commit()

            return True
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Failed to delete flow {name}: {e}", exc_info=True)
            raise


# Singleton instance
flow_service = FlowService()
