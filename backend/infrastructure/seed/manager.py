"""
Seed Data Manager
Applies seed data from JSON configuration files to the database
"""
import json
from datetime import datetime
from typing import Any

import polars as pl

from app.core.logger import logger

from .loader import SeedDataLoader


class SeedDataManager:
    """数据初始化管理器（配置文件版本）"""

    def __init__(
        self,
        db_client: Any,
        loader: SeedDataLoader | None = None,
    ) -> None:
        """
        初始化数据种子管理器

        Args:
            db_client: DolphinDB 客户端
            loader: 种子数据加载器（可选，默认创建新实例）
        """
        self.db_client = db_client
        self.loader = loader or SeedDataLoader()

    def seed_all(self) -> None:
        """执行所有 seed 操作"""
        logger.info("Starting seed data initialization...")
        self.seed_sync_task_config()
        self.seed_etl_task_config()
        self.seed_factor_data_config()
        self.seed_factor_metadata()
        self.seed_user_sync_preference()
        self.seed_flow_config()
        logger.info("Seed data initialization completed")

    def seed_sync_task_config(self) -> None:
        """
        如果 sync_task_config 表为空，则写入默认同步任务定义
        仅在首次启动时生效，后续可通过 API 增删改
        """
        try:
            count = self.db_client.query("SELECT count(*) as cnt FROM sync_task_config")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("sync_task_config 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.debug(f"检查 sync_task_config 数据失败（表可能刚创建）: {e}")
            # 表可能刚创建，继续 seed

        now = datetime.now()
        tasks = self.loader.load_sync_tasks()

        if not tasks:
            logger.warning("No sync tasks found in config, skipping seed")
            return

        seed_df = pl.DataFrame({
            "task_id": [t["task_id"] for t in tasks],
            "api_name": [t["api_name"] for t in tasks],
            "description": [t["description"] for t in tasks],
            "sync_type": [t["sync_type"] for t in tasks],
            "params_json": [json.dumps(t["params"], ensure_ascii=False) for t in tasks],
            "date_field": [t.get("date_field", "") for t in tasks],
            "primary_keys_json": [json.dumps(t["primary_keys"]) for t in tasks],
            "table_name": [t["table_name"] for t in tasks],
            "schema_json": [json.dumps(t["schema"], ensure_ascii=False) for t in tasks],
            "enabled": [True] * len(tasks),
            "api_limit": [t.get("api_limit", 5000) for t in tasks],
            "created_at": [now] * len(tasks),
            "updated_at": [now] * len(tasks),
        })

        self.db_client.upsert("sync_task_config", seed_df, ["task_id"])
        logger.info(f"已写入 {len(tasks)} 条默认同步任务配置")

    def seed_etl_task_config(self) -> None:
        """
        如果 etl_task_config 表为空，则写入默认 ETL 任务定义
        仅在首次启动时生效，后续可通过 API 增删改
        """
        try:
            count = self.db_client.query("SELECT count(*) as cnt FROM etl_task_config")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("etl_task_config 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.warning(
                f"检查 etl_task_config 表时出错（可能表不存在，将继续 seed）: {e}"
            )

        now = datetime.now()
        tasks = self.loader.load_etl_tasks()

        if not tasks:
            logger.warning("No ETL tasks found in config, skipping seed")
            return

        # 替换脚本中的数据库路径模板
        db_meta = self.db_client._db_path
        db_ts = "dfs://quant_ts"

        for task in tasks:
            if "script" in task:
                task["script"] = task["script"].format(db_meta=db_meta, db_ts=db_ts)

        seed_df = pl.DataFrame({
            "task_id": [t["task_id"] for t in tasks],
            "description": [t["description"] for t in tasks],
            "script": [t["script"] for t in tasks],
            "sync_type": [t.get("sync_type", "full") for t in tasks],
            "date_field": [t.get("date_field", "") for t in tasks],
            "primary_keys_json": [json.dumps(t.get("primary_keys", [])) for t in tasks],
            "table_name": [t["table_name"] for t in tasks],
            "schema_json": [json.dumps(t.get("schema", {}), ensure_ascii=False) for t in tasks],
            "enabled": [True] * len(tasks),
            "created_at": [now] * len(tasks),
            "updated_at": [now] * len(tasks),
        })

        self.db_client.upsert("etl_task_config", seed_df, ["task_id"])
        logger.info(f"已写入 {len(tasks)} 条默认 ETL 任务配置")

    def seed_factor_data_config(self) -> None:
        """
        如果 factor_data_config 表为空，则写入默认字段映射
        仅在首次启动时生效，后续可通过 API 修改
        """
        try:
            count = self.db_client.query("SELECT count(*) as cnt FROM factor_data_config")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("factor_data_config 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.debug(f"检查 factor_data_config 数据失败（表可能刚创建）: {e}")

        now = datetime.now()
        mappings = self.loader.load_factor_data_config()

        if not mappings:
            logger.warning("No factor data config found in config, skipping seed")
            return

        seed_df = pl.DataFrame({
            "field_key": [m["field_key"] for m in mappings],
            "description": [m["description"] for m in mappings],
            "table_name": [m["table_name"] for m in mappings],
            "column_name": [m["column_name"] for m in mappings],
            "extra_config": [json.dumps(m["extra_config"], ensure_ascii=False) for m in mappings],
            "updated_at": [now] * len(mappings),
        })

        self.db_client.upsert("factor_data_config", seed_df, ["field_key"])
        logger.info(f"已写入 {len(mappings)} 条默认因子数据配置")

    def seed_factor_metadata(self) -> None:
        """
        如果 factor_metadata 表为空，则写入默认种子因子定义
        仅在首次启动时生效，后续可通过 API 增删改
        """
        try:
            count = self.db_client.query("SELECT count(*) as cnt FROM factor_metadata")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("factor_metadata 已有数据，跳过 seed")
                return
        except Exception:
            pass

        now = datetime.now()
        factors = self.loader.load_factor_metadata()

        if not factors:
            logger.warning("No factor metadata found in config, skipping seed")
            return

        seed_df = pl.DataFrame({
            "factor_id": [f["factor_id"] for f in factors],
            "description": [f["description"] for f in factors],
            "category": [f["category"] for f in factors],
            "compute_mode": [f["compute_mode"] for f in factors],
            "storage_target": [f["storage_target"] for f in factors],
            "depends_on": [json.dumps(f["depends_on"]) for f in factors],
            "params": [json.dumps(f["params"]) for f in factors],
            "code": [f["code"] for f in factors],
            "created_at": [now] * len(factors),
            "updated_at": [now] * len(factors),
        })

        self.db_client.upsert("factor_metadata", seed_df, ["factor_id"])
        logger.info(f"已写入 {len(factors)} 条默认因子定义")

    def seed_user_sync_preference(self) -> None:
        """
        如果 user_sync_preference 表为空，则写入默认用户同步偏好配置
        仅在首次启动时生效，后续可通过 API 修改
        """
        try:
            count = self.db_client.query("SELECT count(*) as cnt FROM user_sync_preference")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("user_sync_preference 已有数据，跳过 seed")
                return
        except Exception:
            pass

        now = datetime.now()
        default_preference = {
            "user_id": "default",
            "index_table": "sync_index_basic",
            "created_at": now,
            "updated_at": now,
        }

        seed_df = pl.DataFrame([default_preference])
        self.db_client.upsert("user_sync_preference", seed_df, ["user_id"])
        logger.info("已写入默认用户同步偏好配置")

    def seed_flow_config(self) -> None:
        """
        如果 flow_config 表为空，则写入默认 flow 配置
        仅在首次启动时生效，后续可通过 API 增删改
        """
        try:
            count = self.db_client.query("SELECT count(*) as cnt FROM flow_config")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("flow_config 已有数据，跳过 seed")
                return
        except Exception as e:
            logger.debug(f"检查 flow_config 数据失败（表可能刚创建）: {e}")
            # 表可能刚创建，继续 seed

        now = datetime.now()
        from .seed_flow_config import DEFAULT_FLOWS
        import json

        if not DEFAULT_FLOWS:
            logger.warning("No flows found in DEFAULT_FLOWS, skipping seed")
            return

        seed_df = pl.DataFrame({
            "name": [f["name"] for f in DEFAULT_FLOWS],
            "description": [f["description"] for f in DEFAULT_FLOWS],
            "cron": [f["cron"] for f in DEFAULT_FLOWS],
            "tags": [json.dumps(f["tags"], ensure_ascii=False) for f in DEFAULT_FLOWS],
            "enabled": [f["enabled"] for f in DEFAULT_FLOWS],
            "date_offset_days": [f["date_offset_days"] for f in DEFAULT_FLOWS],
            "tasks": [json.dumps(f["tasks"], ensure_ascii=False) for f in DEFAULT_FLOWS],
            "created_at": [now] * len(DEFAULT_FLOWS),
            "updated_at": [now] * len(DEFAULT_FLOWS),
            "version": [1] * len(DEFAULT_FLOWS),
        })

        self.db_client.upsert("flow_config", seed_df, ["name"])
        logger.info(f"已写入 {len(DEFAULT_FLOWS)} 条默认 flow 配置")
