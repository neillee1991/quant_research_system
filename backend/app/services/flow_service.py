"""
Flow Configuration Service (asyncpg 版本)
"""
from typing import List, Optional, Dict, Any
from datetime import datetime
from zoneinfo import ZoneInfo

from app.core.logger import logger
from app.models.flow_config import (
    FlowConfigCreate,
    FlowConfigUpdate,
    FlowConfigInDB,
    FlowConfigListItem,
    TaskInDAG,
)
from scheduler.db import DatabasePool

_TZ = ZoneInfo("Asia/Shanghai")


def _now() -> datetime:
    return datetime.now(_TZ)


class FlowService:
    """Flow Configuration CRUD Service (asyncpg)"""

    @staticmethod
    def _parse_db_row(row: Dict[str, Any]) -> FlowConfigInDB:
        tags = row.get("tags") or []
        tasks_data = row.get("tasks") or []
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
    async def list_flows(enabled_only: bool = False) -> List[FlowConfigListItem]:
        try:
            if enabled_only:
                rows = await DatabasePool.fetch(
                    "SELECT name, description, cron, tags, enabled, date_offset_days, tasks, updated_at "
                    "FROM flow_configs WHERE enabled = true ORDER BY updated_at DESC"
                )
            else:
                rows = await DatabasePool.fetch(
                    "SELECT name, description, cron, tags, enabled, date_offset_days, tasks, updated_at "
                    "FROM flow_configs ORDER BY updated_at DESC"
                )
            flows = []
            for row in rows:
                row = dict(row)
                tasks_data = row.get("tasks") or []
                tags = row.get("tags") or []
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
    async def get_flow(name: str) -> Optional[FlowConfigInDB]:
        try:
            row = await DatabasePool.fetchrow(
                "SELECT * FROM flow_configs WHERE name = $1", name
            )
            if not row:
                return None
            return FlowService._parse_db_row(dict(row))
        except Exception as e:
            logger.error(f"Failed to get flow {name}: {e}", exc_info=True)
            raise

    @staticmethod
    async def create_flow(config: FlowConfigCreate) -> FlowConfigInDB:
        try:
            existing = await FlowService.get_flow(config.name)
            if existing:
                raise ValueError(f"Flow with name '{config.name}' already exists")

            now = _now()
            await DatabasePool.execute(
                """
                INSERT INTO flow_configs
                (name, description, cron, tags, tasks, enabled, date_offset_days,
                 version, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                config.name,
                config.description,
                config.cron,
                config.tags or [],
                [t.model_dump() for t in config.tasks] if config.tasks else [],
                config.enabled,
                config.date_offset_days,
                1,
                now,
                now,
            )
            logger.info(f"Created flow: {config.name}")
            return await FlowService.get_flow(config.name)
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Failed to create flow: {e}", exc_info=True)
            raise

    @staticmethod
    async def update_flow(name: str, config: FlowConfigUpdate) -> FlowConfigInDB:
        try:
            existing = await FlowService.get_flow(name)
            if not existing:
                raise ValueError(f"Flow '{name}' not found")

            update_data: Dict[str, Any] = {}
            if config.description is not None:
                update_data["description"] = config.description
            if config.cron is not None:
                update_data["cron"] = config.cron
            if config.tags is not None:
                update_data["tags"] = config.tags
            if config.enabled is not None:
                update_data["enabled"] = config.enabled
            if config.date_offset_days is not None:
                update_data["date_offset_days"] = config.date_offset_days
            if config.tasks is not None:
                update_data["tasks"] = [t.model_dump() for t in config.tasks]

            if not update_data:
                return existing

            update_data["updated_at"] = _now()
            update_data["version"] = existing.version + 1

            keys = list(update_data.keys())
            set_clause = ", ".join(f"{k} = ${i+1}" for i, k in enumerate(keys))
            values = [update_data[k] for k in keys] + [name]

            await DatabasePool.execute(
                f"UPDATE flow_configs SET {set_clause} WHERE name = ${len(keys)+1}",
                *values,
            )
            logger.info(f"Updated flow: {name}")
            return await FlowService.get_flow(name)
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Failed to update flow {name}: {e}", exc_info=True)
            raise

    @staticmethod
    async def delete_flow(name: str, soft_delete: bool = True) -> bool:
        try:
            existing = await FlowService.get_flow(name)
            if not existing:
                raise ValueError(f"Flow '{name}' not found")

            now = _now()
            if soft_delete:
                await DatabasePool.execute(
                    "UPDATE flow_configs SET enabled = false, updated_at = $1 WHERE name = $2",
                    now, name,
                )
                logger.info(f"Disabled (soft deleted) flow: {name}")
            else:
                await DatabasePool.execute(
                    "DELETE FROM flow_configs WHERE name = $1", name
                )
                logger.info(f"Hard deleted flow: {name}")
            return True
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Failed to delete flow {name}: {e}", exc_info=True)
            raise


# Singleton instance
flow_service = FlowService()
