"""
Seed Data Manager
Applies seed data from JSON configuration files to PostgreSQL
"""
import json
from datetime import datetime
from typing import Any

from app.core.logger import logger

from .loader import SeedDataLoader


class SeedDataManager:
    """数据初始化管理器（PostgreSQL 版本）"""

    def __init__(self, loader: SeedDataLoader | None = None) -> None:
        self.loader = loader or SeedDataLoader()

    async def seed_all(self) -> None:
        """执行所有 seed 操作"""
        logger.info("Starting seed data initialization...")
        await self.seed_sync_task_configs()
        await self.seed_etl_task_configs()
        await self.seed_factor_field_mappings()
        await self.seed_factor_configs()
        await self.seed_user_preferences()
        await self.seed_flow_configs()
        logger.info("Seed data initialization completed")

    async def seed_sync_task_configs(self) -> None:
        """如果 sync_task_configs 表为空，则写入默认同步任务定义"""
        from scheduler.db import DatabasePool

        try:
            row = await DatabasePool.fetchrow("SELECT COUNT(*) AS cnt FROM sync_task_configs")
            if row and row["cnt"] > 0:
                logger.info("sync_task_configs 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.debug(f"检查 sync_task_configs 失败（表可能刚创建）: {e}")

        tasks = self.loader.load_sync_tasks()
        if not tasks:
            logger.warning("No sync tasks found in config, skipping seed")
            return

        now = datetime.now()
        for t in tasks:
            await DatabasePool.execute("""
                INSERT INTO sync_task_configs
                  (task_id, api_name, description, sync_type, params_json,
                   date_field, primary_keys_json, table_name, schema_json,
                   enabled, api_limit, created_at, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                ON CONFLICT (task_id) DO NOTHING
            """,
                t["task_id"], t["api_name"], t["description"], t["sync_type"],
                json.dumps(t["params"], ensure_ascii=False),
                t.get("date_field", ""),
                json.dumps(t["primary_keys"]),
                t["table_name"],
                json.dumps(t["schema"], ensure_ascii=False),
                True, t.get("api_limit", 5000), now, now,
            )
        logger.info(f"已写入 {len(tasks)} 条默认同步任务配置")

    async def seed_etl_task_configs(self) -> None:
        """如果 etl_task_configs 表为空，则写入默认 ETL 任务定义"""
        from scheduler.db import DatabasePool

        try:
            row = await DatabasePool.fetchrow("SELECT COUNT(*) AS cnt FROM etl_task_configs")
            if row and row["cnt"] > 0:
                logger.info("etl_task_configs 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.warning(f"检查 etl_task_configs 失败（可能表不存在，将继续 seed）: {e}")

        tasks = self.loader.load_etl_tasks()
        if not tasks:
            logger.warning("No ETL tasks found in config, skipping seed")
            return

        now = datetime.now()
        for t in tasks:
            await DatabasePool.execute("""
                INSERT INTO etl_task_configs
                  (task_id, description, script, sync_type, date_field,
                   primary_keys_json, table_name, schema_json, source_tables, enabled,
                   created_at, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (task_id) DO NOTHING
            """,
                t["task_id"], t["description"], t["script"],
                t.get("sync_type", "full"), t.get("date_field", ""),
                json.dumps(t.get("primary_keys", [])),
                t["table_name"],
                json.dumps(t.get("schema", {}), ensure_ascii=False),
                json.dumps(t.get("source_tables", [])),
                True, now, now,
            )
        logger.info(f"已写入 {len(tasks)} 条默认 ETL 任务配置")

    async def seed_factor_field_mappings(self) -> None:
        """如果 factor_field_mappings 表为空，则写入默认字段映射"""
        from scheduler.db import DatabasePool

        try:
            row = await DatabasePool.fetchrow("SELECT COUNT(*) AS cnt FROM factor_field_mappings")
            if row and row["cnt"] > 0:
                logger.info("factor_field_mappings 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.debug(f"检查 factor_field_mappings 失败: {e}")

        mappings = self.loader.load_factor_data_config()
        if not mappings:
            logger.warning("No factor data config found in config, skipping seed")
            return

        now = datetime.now()
        for m in mappings:
            await DatabasePool.execute("""
                INSERT INTO factor_field_mappings
                  (field_key, description, table_name, column_name, extra_config, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6)
                ON CONFLICT (field_key) DO NOTHING
            """,
                m["field_key"], m["description"], m["table_name"],
                m["column_name"],
                json.dumps(m["extra_config"], ensure_ascii=False),
                now,
            )
        logger.info(f"已写入 {len(mappings)} 条默认因子字段映射配置")

    async def seed_factor_configs(self) -> None:
        """如果 factor_configs 表为空，则写入默认种子因子定义"""
        from scheduler.db import DatabasePool

        try:
            row = await DatabasePool.fetchrow("SELECT COUNT(*) AS cnt FROM factor_configs")
            if row and row["cnt"] > 0:
                logger.info("factor_configs 已有数据，跳过 seed")
                return
        except Exception:
            pass

        factors = self.loader.load_factor_metadata()
        if not factors:
            logger.warning("No factor metadata found in config, skipping seed")
            return

        now = datetime.now()
        for f in factors:
            await DatabasePool.execute("""
                INSERT INTO factor_configs
                  (factor_id, description, category, compute_mode, storage_target,
                   depends_on, params, code, created_at, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (factor_id) DO NOTHING
            """,
                f["factor_id"], f["description"], f["category"],
                f["compute_mode"], f["storage_target"],
                json.dumps(f["depends_on"]),
                json.dumps(f["params"]),
                f["code"], now, now,
            )
        logger.info(f"已写入 {len(factors)} 条默认因子定义")

    async def seed_user_preferences(self) -> None:
        """如果 user_preferences 表为空，则写入默认用户同步偏好配置"""
        from scheduler.db import DatabasePool

        try:
            row = await DatabasePool.fetchrow("SELECT COUNT(*) AS cnt FROM user_preferences")
            if row and row["cnt"] > 0:
                logger.info("user_preferences 已有数据，跳过 seed")
                return
        except Exception:
            pass

        now = datetime.now()
        await DatabasePool.execute("""
            INSERT INTO user_preferences (user_id, index_table, created_at, updated_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id) DO NOTHING
        """, "default", "sync_index_basic", now, now)
        logger.info("已写入默认用户同步偏好配置")

    async def seed_flow_configs(self) -> None:
        """如果 flow_configs 表为空，则写入默认 flow 配置"""
        from scheduler.db import DatabasePool
        from .seed_flow_config import DEFAULT_FLOWS

        try:
            row = await DatabasePool.fetchrow("SELECT COUNT(*) AS cnt FROM flow_configs")
            if row and row["cnt"] > 0:
                logger.info("flow_configs 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.debug(f"检查 flow_configs 失败: {e}")

        if not DEFAULT_FLOWS:
            logger.warning("No flows found in DEFAULT_FLOWS, skipping seed")
            return

        now = datetime.now()
        for f in DEFAULT_FLOWS:
            await DatabasePool.execute("""
                INSERT INTO flow_configs
                  (name, description, cron, tags, enabled, date_offset_days,
                   tasks, created_at, updated_at, version)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (name) DO NOTHING
            """,
                f["name"], f["description"], f["cron"],
                json.dumps(f["tags"], ensure_ascii=False),
                f["enabled"], f["date_offset_days"],
                json.dumps(f["tasks"], ensure_ascii=False),
                now, now, 1,
            )
        logger.info(f"已写入 {len(DEFAULT_FLOWS)} 条默认 flow 配置")
